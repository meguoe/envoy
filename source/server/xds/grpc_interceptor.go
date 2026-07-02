package xdsserver

// grpc_interceptor.go —— gRPC 拦截器与指标
//
// 提供统一的日志和指标拦截器，用于 gRPC 请求监控。
// 指标通过 /metrics 端点暴露。

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCMetrics gRPC 运行指标
type GRPCMetrics struct {
	ConnectionsTotal  atomic.Uint64
	ActiveConnections atomic.Int64
	RequestsTotal     atomic.Uint64
	RequestsFailed    atomic.Uint64
	RequestsByMethod  map[string]*atomic.Uint64
	mu                sync.Mutex
}

var globalGRPCMetrics = &GRPCMetrics{
	RequestsByMethod: make(map[string]*atomic.Uint64),
}

// incMethod 原子递增指定 gRPC 方法的请求计数。
func (m *GRPCMetrics) incMethod(method string) {
	m.mu.Lock()
	counter, ok := m.RequestsByMethod[method]
	if !ok {
		counter = &atomic.Uint64{}
		m.RequestsByMethod[method] = counter
	}
	m.mu.Unlock()
	counter.Add(1)
}

// getMethodCount 返回指定 gRPC 方法的累计请求次数。
func (m *GRPCMetrics) getMethodCount(method string) uint64 {
	m.mu.Lock()
	counter, ok := m.RequestsByMethod[method]
	m.mu.Unlock()
	if !ok {
		return 0
	}
	return counter.Load()
}

// GRPCMetricsSnapshotData gRPC 指标快照数据。
type GRPCMetricsSnapshotData struct {
	ConnectionsTotal  uint64            `json:"connections_total"`
	ActiveConnections int64             `json:"active_connections"`
	RequestsTotal     uint64            `json:"requests_total"`
	RequestsFailed    uint64            `json:"requests_failed"`
	RequestsByMethod  map[string]uint64 `json:"requests_by_method"`
}

// GRPCMetricsSnapshot 返回 gRPC 指标快照。
func GRPCMetricsSnapshot() GRPCMetricsSnapshotData {
	m := globalGRPCMetrics
	m.mu.Lock()
	methods := make([]string, 0, len(m.RequestsByMethod))
	for method := range m.RequestsByMethod {
		methods = append(methods, method)
	}
	m.mu.Unlock()
	sort.Strings(methods)
	byMethod := make(map[string]uint64, len(methods))
	for _, method := range methods {
		byMethod[method] = m.getMethodCount(method)
	}
	return GRPCMetricsSnapshotData{
		ConnectionsTotal:  m.ConnectionsTotal.Load(),
		ActiveConnections: m.ActiveConnections.Load(),
		RequestsTotal:     m.RequestsTotal.Load(),
		RequestsFailed:    m.RequestsFailed.Load(),
		RequestsByMethod:  byMethod,
	}
}

// UnaryServerInterceptor 日志 + 指标拦截器
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		globalGRPCMetrics.RequestsTotal.Add(1)
		globalGRPCMetrics.incMethod(info.FullMethod)

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
			globalGRPCMetrics.RequestsFailed.Add(1)
		}

		slog.Info("gRPC request", "method", info.FullMethod, "code", code.String(), "duration", duration.Round(time.Millisecond).String())
		return resp, err
	}
}

// StreamServerInterceptor 流式日志 + 指标拦截器
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		globalGRPCMetrics.ConnectionsTotal.Add(1)
		globalGRPCMetrics.ActiveConnections.Add(1)
		defer globalGRPCMetrics.ActiveConnections.Add(-1)
		globalGRPCMetrics.RequestsTotal.Add(1)
		globalGRPCMetrics.incMethod(info.FullMethod)

		err := handler(srv, ss)

		duration := time.Since(start)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
			globalGRPCMetrics.RequestsFailed.Add(1)
		}

		slog.Info("gRPC stream", "method", info.FullMethod, "code", code.String(), "duration", duration.Round(time.Millisecond).String())
		return err
	}
}
