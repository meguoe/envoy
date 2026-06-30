package main

// main.go —— 程序入口 + 服务器启动
//
// 启动顺序：
//   1. 创建 xDS 引擎
//   2. 从文件加载历史规则
//   3. 启动 gRPC（协程）
//   4. 启动 HTTP API（协程）
//   5. 推送初始快照
//   6. 等待信号，优雅关闭

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

	// 1. 创建 xDS 引擎
	engine = xdsserver.NewEngine(cfg.NodeID, cfg.ConnectTimeout, cfg.UDPIdleTimeout)
	engine.SetOnRulesChanged(func(rules []*xdsserver.ProxyRule) error {
		return store.Save(cfg.StorePath, rules)
	})

	// 2. 从文件加载历史规则
	rules, err := store.Load(cfg.StorePath)
	if err != nil {
		log.Fatalf("加载数据失败: %v", err)
	}
	if len(rules) > 0 {
		engine.SetRules(rules)
	}

	// 3. 启动 gRPC 服务器
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

	// 4. 启动 HTTP API 服务器
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
		Handler:           httpserver.NewHandler(engine, cfg.MaxBodyBytes, authCfg, cfg.RateLimit.RPS, cfg.RateLimit.Burst),
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
		log.Printf("HTTP API 启动 %s", cfg.APIAddr)
		if cfg.HasHTTPS() {
			log.Printf("HTTPS 已启用")
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

	// 5. 推送初始快照
	if err := engine.PushSnapshot(); err != nil {
		log.Fatalf("初始快照推送失败: %v", err)
	}

	log.Printf("xDS 控制面就绪")
	log.Printf("  gRPC (ADS)  %s", cfg.GRPCAddr)
	log.Printf("  HTTP (API)  %s", cfg.APIAddr)
	log.Printf("  数据文件     %s", cfg.StorePath)
	log.Printf("")
	log.Printf("  GET    /nodes          获取代理节点")
	log.Printf("  GET    /health         服务健康检查")
	log.Printf("  GET    /metrics        服务运行指标")
	log.Printf("  GET    /rules          获取规则列表")
	log.Printf("  POST   /rules          创建代理规则")
	log.Printf("  PUT    /rules/:id      更新代理规则")
	log.Printf("  DELETE /rules/:id      删除代理规则")

	// 6. 等待信号或服务器错误
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

	// 最终持久化一次
	if err := store.Save(cfg.StorePath, engine.ListRules()); err != nil {
		log.Printf("关闭前持久化失败: %v", err)
	}

	// 给进行中的请求 5 秒完成时间
	shutdownTimeout := 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// HTTP 优雅关闭
	if err := httpSrv.Shutdown(ctx); err != nil {
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
