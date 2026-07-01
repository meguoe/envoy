package main

// main.go —— 程序入口 + 服务器启动
//
// 启动顺序：
//   1. 初始化存储
//   2. 创建 xDS 引擎
//   3. 从数据库加载历史规则
//   4. 启动 gRPC（协程）
//   5. 启动 HTTP API（协程）
//   6. 推送初始快照
//   7. 启动规则轮询器
//   8. 等待信号，优雅关闭
//
// 数据库是唯一的规则来源。服务只通过 HTTP API 写入数据库，
// 关闭时不回写，避免覆盖人工直接修改的规则。

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"envoy-control-plane/internal/config"
	httpserver "envoy-control-plane/internal/server/http"
	xdsserver "envoy-control-plane/internal/server/xds"
	"envoy-control-plane/internal/store"

	grpc "google.golang.org/grpc"
)

var (
	cfg     config.Config
	engine  *xdsserver.Engine
	grpcSrv *grpc.Server
	httpSrv *http.Server
)

func main() {
	configPath := flag.String("config", "", "配置文件路径 (默认: config.yaml)")
	structuredLog := flag.Bool("json-log", false, "启用 JSON 结构化日志输出")
	flag.Parse()

	var err error
	cfg, err = config.Load(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	httpserver.SetLogLevel(cfg.LogLevel)
	if *structuredLog {
		httpserver.EnableStructuredLogging(true)
	}

	// 1. 初始化存储
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	dataStore, err := store.NewPgStore(startupCtx, store.BuildPgDSN(cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.DBName))
	cancelStartup()
	if err != nil {
		log.Fatalf("初始化存储失败: %v", err)
	}
	defer dataStore.Close()

	// 2. 创建 xDS 引擎
	engine = xdsserver.NewEngine(cfg.NodeID, cfg.ConnectTimeout, cfg.UDPIdleTimeout)

	// 2.1 注册 ACK/NACK 回调
	ackCb := xdsserver.NewAckCallbacks(dataStore, func(rev int64) {
		engine.SetDeployedRevision(rev)
	})
	engine.SetCallbacks(ackCb)

	// 3. 加载历史规则
	loadCtx, cancelLoad := context.WithTimeout(context.Background(), 5*time.Second)
	rules, err := dataStore.Load(loadCtx)
	cancelLoad()
	if err != nil {
		log.Fatalf("加载数据失败: %v", err)
	}
	if len(rules) > 0 {
		engine.SetRules(rules)
	}

	// 加载当前 revision
	revCtx, cancelRev := context.WithTimeout(context.Background(), 5*time.Second)
	currentRev, err := dataStore.LoadRevision(revCtx)
	cancelRev()
	if err != nil {
		log.Printf("加载 revision 失败: %v", err)
	}

	// 4. 启动 gRPC 服务器
	var grpcOpts []grpc.ServerOption
	if creds, err := cfg.TLSConfig().ServerCredentials(); err != nil {
		log.Fatalf("加载 TLS 凭证失败: %v", err)
	} else if creds != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	grpcSrv = engine.NewGRPCServer(grpcOpts...)
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- engine.StartGRPC(cfg.GRPCAddr, grpcSrv)
	}()

	// 5. 启动 HTTP API 服务器
	var authCfg *httpserver.AuthConfig
	if cfg.HasAuth() {
		authCfg = &httpserver.AuthConfig{
			APIKey:         cfg.Auth.APIKey,
			AllowedIPs:     cfg.Auth.AllowedIPs,
			TrustedProxies: cfg.Auth.TrustedProxies,
		}
	}
	httpSrv = &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           httpserver.NewHandler(engine, dataStore, cfg.MaxBodyBytes, authCfg, cfg.RateLimit.RPS, cfg.RateLimit.Burst),
		ReadHeaderTimeout: cfg.HttpTimeout.ReadHeaderTimeout,
		ReadTimeout:       cfg.HttpTimeout.ReadTimeout,
		WriteTimeout:      cfg.HttpTimeout.WriteTimeout,
		IdleTimeout:       cfg.HttpTimeout.IdleTimeout,
	}
	if cfg.HasHTTPS() {
		httpSrv.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	httpErr := make(chan error, 1)
	go func() {
		if cfg.HasHTTPS() {
			httpErr <- httpSrv.ListenAndServeTLS(cfg.HTTPS.CertFile, cfg.HTTPS.KeyFile)
		} else {
			httpErr <- httpSrv.ListenAndServe()
		}
	}()

	// 等待服务器启动或立即失败
	select {
	case err := <-grpcErr:
		log.Fatalf("gRPC 服务启动失败: %v", err)
	case err := <-httpErr:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务启动失败: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
	}

	// 6. 推送初始快照
	if currentRev > 0 {
		if err := dataStore.LogPushPending(context.Background(), currentRev); err != nil {
			log.Printf("记录初始 push pending 失败: %v", err)
		}
	}
	if err := engine.ReplaceRulesAndPushWithVersion(rules, currentRev); err != nil {
		log.Printf("初始快照推送失败: %v", err)
	}
	stopPoll := startRulePoller(dataStore, engine, 5*time.Second)
	defer stopPoll()

	log.Printf("xDS 控制面就绪  gRPC=%s  HTTP=%s", cfg.GRPCAddr, cfg.APIAddr)

	// 7. 等待信号或服务器错误
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("收到信号 %v，正在关闭...", sig)
	case err := <-grpcErr:
		log.Printf("gRPC 服务异常退出: %v", err)
	case err := <-httpErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP 服务异常退出: %v", err)
		}
	}

	// 给进行中的请求 5 秒完成时间
	shutdownTimeout := 5 * time.Second
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// HTTP 优雅关闭
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 关闭失败: %v", err)
	} else {
		log.Printf("HTTP 已关闭")
	}

	// ADS 是长连接；Envoy 不退出时 GracefulStop 会等待，控制面关闭应直接断开。
	grpcSrv.Stop()
	log.Printf("gRPC 已关闭")

	log.Printf("服务已停止")
	engine.Close()
}

func startRulePoller(dataStore *store.PgStore, engine *xdsserver.Engine, interval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				loadCtx, cancelLoad := context.WithTimeout(ctx, 5*time.Second)
				dbRev, err := dataStore.LoadRevision(loadCtx)
				cancelLoad()
				if err != nil {
					log.Printf("规则轮询检查失败: %v", err)
					continue
				}
				engineRev := engine.KnownRevision()
				if dbRev == engineRev {
					// 同一 revision 已推送过，检查是否 failed 允许重试
					statusCtx, cancelStatus := context.WithTimeout(ctx, 5*time.Second)
					status, err := dataStore.PushStatus(statusCtx, dbRev)
					cancelStatus()
					if err != nil || status != "failed" {
						continue
					}
					log.Printf("规则轮询重试已失败的 revision %d", dbRev)
				}

				rulesCtx, cancelRules := context.WithTimeout(ctx, 5*time.Second)
				rules, err := dataStore.Load(rulesCtx)
				cancelRules()
				if err != nil {
					log.Printf("规则轮询加载失败: %v", err)
					continue
				}
				if err := dataStore.LogPushPending(ctx, dbRev); err != nil {
					log.Printf("记录 push pending 失败: %v", err)
				}
				if err := engine.ReplaceRulesAndPushWithVersion(rules, dbRev); err != nil {
					log.Printf("规则轮询推送失败: %v", err)
					_ = dataStore.MarkPushFailed(ctx, dbRev, err.Error())
					continue
				}
				log.Printf("规则轮询发现变更并推送: rules=%d rev=%d", len(rules), dbRev)
			}
		}
	}()
	return cancel
}
