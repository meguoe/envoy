package main

// main.go —— 程序入口 + 服务器启动
//
// 启动顺序：
//   1. 初始化存储
//   2. 创建 xDS 引擎
//   3. 从数据库加载历史规则
//   4. 启动 gRPC（协程）
//   5. 启动推送 worker（事件触发 + 30s 兜底 ticker）
//   6. 启动 HTTP API（协程）
//   7. 推送初始快照
//   8. 等待信号，优雅关闭
//
// 数据库是唯一的规则来源。服务只通过 HTTP API 写入数据库，
// 关闭时不回写，避免覆盖人工直接修改的规则。

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"envoy-control-plane/source/config"
	httpserver "envoy-control-plane/source/server/http"
	xdsserver "envoy-control-plane/source/server/xds"
	"envoy-control-plane/source/store"

	grpc "google.golang.org/grpc"
)

const (
	pidFile = "xds-control-plane.pid"
	logDir  = "logs"
	logFile = "logs/xds-control-plane.log"
)

var (
	cfg     config.Config
	engine  *xdsserver.Engine
	grpcSrv *grpc.Server
	httpSrv *http.Server
)

// getEnv 从环境变量中读取指定 key 的值，若未设置则返回 fallback 默认值。
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// readPIDFile 从 PID 文件中读取进程 ID 并返回。
func readPIDFile() (int, error) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}
	var pid int
	_, err = fmt.Sscanf(string(data), "%d", &pid)
	return pid, err
}

// isProcessRunning 通过向指定 PID 发送零号信号判断进程是否存活。
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// startDaemon 以守护进程方式启动服务，将标准输出和错误输出重定向到日志文件。
func startDaemon(configPath string, structuredLog bool) {
	pid, err := readPIDFile()
	if err == nil && isProcessRunning(pid) {
		fmt.Printf("服务已在运行 (PID %d)\n", pid)
		return
	}

	args := []string{"-config", configPath}
	if structuredLog {
		args = append(args, "-json-log")
	}

	selfPath, err := os.Executable()
	if err != nil {
		selfPath = os.Args[0]
	}
	cmd := exec.Command(selfPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Printf("创建日志目录失败: %v\n", err)
		return
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("打开日志文件失败: %v", err)
	}
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Start(); err != nil {
		log.Fatalf("启动后台进程失败: %v", err)
	}
	f.Close()

	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644); err != nil {
		log.Printf("写入 PID 文件失败: %v", err)
	}

	fmt.Printf("服务已启动 (PID %d)\n", cmd.Process.Pid)
	fmt.Printf("日志文件: %s\n", logFile)
}

// stopDaemon 向运行中的服务发送 SIGTERM 信号，等待优雅退出，超时后强制终止。
func stopDaemon() {
	pid, err := readPIDFile()
	if err != nil {
		fmt.Println("服务未运行")
		return
	}
	if !isProcessRunning(pid) {
		os.Remove(pidFile)
		fmt.Println("服务未运行")
		return
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		fmt.Printf("查找进程失败: %v\n", err)
		return
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		fmt.Printf("停止服务失败: %v\n", err)
		return
	}

	for i := 0; i < 50; i++ {
		if !isProcessRunning(pid) {
			os.Remove(pidFile)
			fmt.Printf("服务已停止 (PID %d)\n", pid)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	process.Signal(syscall.SIGKILL)
	os.Remove(pidFile)
	fmt.Printf("服务已强制停止 (PID %d)\n", pid)
}

// statusDaemon 检查 PID 文件并判断服务是否正在运行，输出当前状态。
func statusDaemon() {
	pid, err := readPIDFile()
	if err != nil {
		fmt.Println("服务未运行")
		return
	}
	if isProcessRunning(pid) {
		fmt.Printf("服务运行中 (PID %d)\n", pid)
	} else {
		os.Remove(pidFile)
		fmt.Println("服务未运行")
	}
}

// restartDaemon 先停止已运行的服务，再以守护进程方式重新启动。
func restartDaemon(configPath string, structuredLog bool) {
	pid, err := readPIDFile()
	if err == nil && isProcessRunning(pid) {
		fmt.Printf("正在停止服务 (PID %d)...\n", pid)
		process, _ := os.FindProcess(pid)
		process.Signal(syscall.SIGTERM)
		for i := 0; i < 50; i++ {
			if !isProcessRunning(pid) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		os.Remove(pidFile)
	}
	startDaemon(configPath, structuredLog)
}

// reloadDaemon 向运行中的服务发送 SIGHUP 信号触发配置热重载。
func reloadDaemon() {
	pid, err := readPIDFile()
	if err != nil {
		fmt.Println("服务未运行")
		return
	}
	if !isProcessRunning(pid) {
		os.Remove(pidFile)
		fmt.Println("服务未运行")
		return
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		fmt.Printf("查找进程失败: %v\n", err)
		return
	}
	if err := process.Signal(syscall.SIGHUP); err != nil {
		fmt.Printf("发送重载信号失败: %v\n", err)
		return
	}
	fmt.Printf("已发送重载信号 (PID %d)\n", pid)
}

// initDB 连接 PostgreSQL 并初始化数据库表结构，force 为 true 时会先删除并重建数据库。
func initDB(force bool) {
	dbName := getEnv("DB_NAME", "envoy_cp")
	dbUser := getEnv("DB_USER", "postgres")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")

	if force {
		fmt.Printf("即将删除并重建数据库 %s，此操作不可恢复！\n", dbName)
		fmt.Print("确认执行？(y/N): ")
		var confirm string
		fmt.Scanln(&confirm)
		if confirm != "y" && confirm != "Y" {
			fmt.Println("已取消")
			return
		}

		adminCtx, cancelAdmin := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelAdmin()
		adminDSN := store.BuildPgDSN(dbHost, dbPort, dbUser, os.Getenv("DB_PASSWORD"), "postgres")
		adminDS, err := store.NewPgStore(adminCtx, adminDSN)
		if err != nil {
			log.Fatalf("连接 postgres 数据库失败: %v", err)
		}
		if err := adminDS.DropDB(adminCtx, dbName); err != nil {
			log.Fatalf("删除数据库失败: %v", err)
		}
		adminDS.Close()
		fmt.Printf("数据库 %s 已删除\n", dbName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ds, err := store.NewPgStore(ctx, store.BuildPgDSN(dbHost, dbPort, dbUser, os.Getenv("DB_PASSWORD"), dbName))
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer ds.Close()

	if err := ds.InitDB(ctx); err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}
	log.Println("数据库表结构初始化完成")
}

// printUsage 输出命令行帮助信息，列出所有可用命令和选项。
func printUsage() {
	fmt.Println(`xds-control-plane - Envoy xDS 控制面

用法:
  xds-control-plane <命令> [选项]

命令:
  setup               配置向导（数据库、API 密钥）
  initdb              初始化数据库表结构
  initdb --force      强制重建数据库（需确认）
  config --init       生成默认配置文件
  config --validate   校验配置文件
  cert                生成证书（默认生成 mTLS + HTTPS）
  cert --mtls         仅生成 mTLS 证书
  cert --https        仅生成 HTTPS 证书
  start               后台启动服务
  restart             重启服务
  stop                停止服务
  reload              重新加载配置
  status              查看运行状态

选项:
  -config <path>      配置文件路径 (默认: config.yaml)
  -json-log           启用 JSON 结构化日志
  --help, -h          显示此帮助信息

示例:
  xds-control-plane setup
  xds-control-plane initdb
  xds-control-plane cert --mtls
  xds-control-plane cert --https
  xds-control-plane start`)
}

// main 程序入口，解析命令行参数并启动 xDS 控制面服务。
func main() {
	configPath := flag.String("config", "", "配置文件路径 (默认: config.yaml)")
	structuredLog := flag.Bool("json-log", false, "启用 JSON 结构化日志输出")
	help := flag.Bool("help", false, "显示帮助信息")
	flag.Parse()

	args := flag.Args()

	if len(args) > 0 {
		switch args[0] {
		case "setup":
			if err := config.RunSetupWizard(*configPath); err != nil {
				log.Fatalf("配置向导失败: %v", err)
			}
			return
		case "initdb":
			force := false
			for _, arg := range args[1:] {
				if arg == "--force" || arg == "-f" {
					force = true
				}
			}
			initDB(force)
			return
		case "start":
			startDaemon(*configPath, *structuredLog)
			return
		case "restart":
			restartDaemon(*configPath, *structuredLog)
			return
		case "stop":
			stopDaemon()
			return
		case "reload":
			reloadDaemon()
			return
		case "status":
			statusDaemon()
			return
		case "config":
			if len(args) > 1 && args[1] == "--init" {
				if err := config.GenerateDefaultConfigWithCerts(*configPath); err != nil {
					fmt.Printf("初始化失败: %v\n", err)
					return
				}
				fmt.Printf("已生成配置文件: %s\n", *configPath)
				fmt.Println("已生成证书: certs/mtls/, certs/https/")
				return
			}
			if len(args) > 1 && args[1] == "--validate" {
				if err := config.ValidateConfig(*configPath); err != nil {
					fmt.Printf("配置校验失败: %v\n", err)
					return
				}
				fmt.Println("配置校验通过")
				return
			}
			fmt.Println("用法: config --init | config --validate")
			return
		case "cert":
			cfg, err := config.Load(*configPath)
			if err != nil {
				fmt.Printf("加载配置失败: %v\n", err)
				return
			}
			certTypes := []string{"mtls", "https"}
			for _, arg := range args[1:] {
				switch arg {
				case "--mtls":
					certTypes = []string{"mtls"}
				case "--https":
					certTypes = []string{"https"}
				}
			}

			for _, certType := range certTypes {
				certDir := "certs/" + certType
				var clientURI string
				if certType == "mtls" {
					if cfg.XDS.TLS.Enabled && cfg.XDS.TLS.CACert != "" {
						certDir = filepath.Dir(cfg.XDS.TLS.CACert)
					}
					clientURI = cfg.XDS.TLS.ClientURI
				} else {
					if cfg.API.TLS.Enabled && cfg.API.TLS.CertFile != "" {
						certDir = filepath.Dir(cfg.API.TLS.CertFile)
					}
				}
				if err := config.GenerateCerts(config.CertConfig{
					Dir:       certDir,
					Type:      certType,
					ClientURI: clientURI,
					ServerDNS: "xds-server",
				}); err != nil {
					if _, ok := err.(*config.CertExistsError); ok {
						fmt.Printf("%s\n", err)
						fmt.Print("确认重新生成？(y/N): ")
						var confirm string
						fmt.Scanln(&confirm)
						if confirm != "y" && confirm != "Y" {
							fmt.Printf("跳过 %s 证书\n", certType)
							continue
						}
						os.RemoveAll(certDir)
						if err := config.GenerateCerts(config.CertConfig{
							Dir:       certDir,
							Type:      certType,
							ClientURI: clientURI,
							ServerDNS: "xds-server",
						}); err != nil {
							fmt.Printf("生成 %s 证书失败: %v\n", certType, err)
							return
						}
					} else {
						fmt.Printf("生成 %s 证书失败: %v\n", certType, err)
						return
					}
				}
			}
			return
		default:
			if args[0] == "--help" || args[0] == "-h" {
				printUsage()
				return
			}
			fmt.Printf("未知命令: %s\n\n运行 xds-control-plane --help 查看可用命令\n", args[0])
			return
		}
	}

	if *help {
		printUsage()
		return
	}

	var err error
	cfg, err = config.Load(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	httpserver.SetLogLevel(cfg.Server.LogLevel)
	if *structuredLog {
		httpserver.EnableStructuredLogging(true)
	}

	// 1. 初始化存储
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	dataStore, err := store.NewPgStore(startupCtx, store.BuildPgDSN(
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_USER", "postgres"),
		os.Getenv("DB_PASSWORD"),
		getEnv("DB_NAME", "envoy_cp"),
	))
	cancelStartup()
	if err != nil {
		log.Fatalf("初始化存储失败: %v", err)
	}
	defer dataStore.Close()

	// 2. 创建 xDS 引擎
	engine = xdsserver.NewEngine(cfg.Server.NodeID, cfg.XDS.ConnectTimeout, cfg.XDS.UDPIdleTimeout)

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
		log.Printf("加载 revision 失败: %v，跳过初始推送", err)
	}

	// 4. 启动 gRPC 服务器
	var grpcOpts []grpc.ServerOption
	if cfg.XDS.TLS.Enabled && cfg.XDS.TLS.ServerCert != "" && cfg.XDS.TLS.ServerKey != "" {
		if creds, err := cfg.TLSConfig().ServerCredentials(); err != nil {
			log.Fatalf("加载 TLS 凭证失败: %v", err)
		} else if creds != nil {
			grpcOpts = append(grpcOpts, grpc.Creds(creds))
		}
	}
	grpcSrv = engine.NewGRPCServer(grpcOpts...)
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- engine.StartGRPC(cfg.Server.GRPCAddr, grpcSrv)
	}()

	// 5. 启动推送 worker
	worker := xdsserver.NewRulePushWorker(dataStore, engine, 30*time.Second)
	worker.Start()

	// 6. 启动 HTTP API 服务器
	var authCfg *httpserver.AuthConfig
	if cfg.API.Auth.Enabled {
		apiKey := os.Getenv("API_KEY")
		if apiKey == "" {
			log.Fatal("api.auth.enabled 为 true 但 API_KEY 环境变量未设置，请在 .env 中配置 API_KEY")
		}
		authCfg = &httpserver.AuthConfig{APIKey: apiKey}
	}
	httpSrv = &http.Server{
		Addr:              cfg.Server.APIAddr,
		Handler:           httpserver.NewHandler(engine, dataStore, worker, cfg.API.MaxBodyBytes, authCfg, cfg.API.RateLimit.RPS, cfg.API.RateLimit.Burst),
		ReadHeaderTimeout: cfg.API.Timeout.ReadHeaderTimeout,
		ReadTimeout:       cfg.API.Timeout.ReadTimeout,
		WriteTimeout:      cfg.API.Timeout.WriteTimeout,
		IdleTimeout:       cfg.API.Timeout.IdleTimeout,
	}
	if cfg.HasHTTPS() {
		httpSrv.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
				tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
			},
		}
	}
	httpErr := make(chan error, 1)
	go func() {
		if cfg.HasHTTPS() {
			httpErr <- httpSrv.ListenAndServeTLS(cfg.API.TLS.CertFile, cfg.API.TLS.KeyFile)
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

	log.Printf("xDS 控制面就绪  gRPC=%s  HTTP=%s", cfg.Server.GRPCAddr, cfg.Server.APIAddr)

	// 7. 等待信号或服务器错误
	quit := make(chan os.Signal, 1)
	reload := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(reload, syscall.SIGHUP)

	go func() {
		for range reload {
			log.Println("收到 SIGHUP，重新加载配置...")
			newCfg, err := config.Load(*configPath)
			if err != nil {
				log.Printf("重新加载配置失败: %v", err)
				continue
			}
			cfg = newCfg
			httpserver.SetLogLevel(cfg.Server.LogLevel)
			// 重新加载 API 认证配置
			if cfg.API.Auth.Enabled {
				apiKey := os.Getenv("API_KEY")
				if apiKey == "" {
					log.Printf("警告: api.auth.enabled 为 true 但 API_KEY 未设置，保留旧认证配置")
				} else {
					httpserver.SetAuthConfig(&httpserver.AuthConfig{APIKey: apiKey})
				}
			} else {
				httpserver.SetAuthConfig(nil)
			}
			log.Printf("配置已重新加载")
		}
	}()

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

	// 停止 HTTP handler 后台 goroutine（限流器清理等）
	httpserver.StopHandler()

	// 先停推送 worker，等待 goroutine 退出
	worker.Stop()

	// HTTP 优雅关闭
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP 关闭失败: %v", err)
	} else {
		log.Printf("HTTP 已关闭")
	}

	// 优雅停止 gRPC，给 Envoy drain 时间；超时后强制关闭。
	grpcDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()
	select {
	case <-grpcDone:
	case <-time.After(2 * time.Second):
		grpcSrv.Stop()
	}
	log.Printf("gRPC 已关闭")

	log.Printf("服务已停止")
	engine.Close()
}
