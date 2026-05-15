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
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	xdsServer "envoy-control-plane/xds_server"

	grpc "google.golang.org/grpc"
)

var (
	engine  *xdsServer.Engine
	grpcSrv *grpc.Server
	httpSrv *http.Server
)

func main() {
	// 1. 创建 xDS 引擎
	engine = xdsServer.NewEngine("envoy-local")
	engine.SetOnRulesChanged(func() {
		if err := saveRules(engine.ListRules()); err != nil {
			log.Printf("⚠️  自动持久化失败: %v", err)
		}
	})

	// 2. 从文件加载历史规则
	rules, err := loadRules()
	if err != nil {
		log.Fatalf("加载数据失败: %v", err)
	}
	if len(rules) > 0 {
		engine.SetRules(rules)
	}

	// 3. 启动 gRPC 服务器
	grpcSrv = engine.NewGRPCServer()
	go func() {
		if err := engine.StartGRPC(grpcAddr, grpcSrv); err != nil {
			log.Fatalf("gRPC serve: %v", err)
		}
	}()

	// 4. 启动 HTTP API 服务器
	httpSrv = &http.Server{Addr: apiAddr, Handler: buildHTTPMux()}
	go func() {
		log.Printf("HTTP API on %s", apiAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP serve: %v", err)
		}
	}()

	// 5. 推送初始快照
	if err := engine.PushSnapshot(); err != nil {
		log.Fatalf("initial snapshot: %v", err)
	}

	log.Printf("✅ xDS control-plane ready")
	log.Printf("   gRPC (ADS)   %s", grpcAddr)
	log.Printf("   HTTP (API)   %s", apiAddr)
	log.Printf("   数据文件      %s", storePath)
	log.Printf("")
	log.Printf("   GET    /nodes          获取节点信息")
	log.Printf("   GET    /health         服务健康检查")
	log.Printf("   GET    /rules          获取规则列表")
	log.Printf("   POST   /rules          创建代理规则")
	log.Printf("   PUT    /rules/:name    更新代理规则")
	log.Printf("   DELETE /rules/:name    删除代理规则")

	// 6. 等待信号，优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("🛑 收到信号 %v，正在关闭...", sig)

	// 最终持久化一次
	if err := saveRules(engine.ListRules()); err != nil {
		log.Printf("⚠️  关闭前持久化失败: %v", err)
	}

	// 给进行中的请求 5 秒完成时间
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// HTTP 优雅关闭
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("⚠️  HTTP 关闭失败: %v", err)
	} else {
		log.Printf("✅ HTTP 已关闭")
	}

	// gRPC 优雅关闭（ADS 长连接可能无法自然结束，超时后强制关闭）
	done := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		log.Printf("✅ gRPC 已关闭")
	case <-time.After(3 * time.Second):
		grpcSrv.Stop()
	}

	log.Printf("👋 Bye!")
}
