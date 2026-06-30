package xdsserver

import (
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

func TestGRPCMetricsIncMethod(t *testing.T) {
	m := &GRPCMetrics{RequestsByMethod: make(map[string]*atomic.Uint64)}
	m.incMethod("/envoy.service-discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources")
	m.incMethod("/envoy.service-discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources")
	m.incMethod("/envoy.service-discovery.v3.ClusterDiscoveryService/FetchClusters")

	count1 := m.getMethodCount("/envoy.service-discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources")
	count2 := m.getMethodCount("/envoy.service-discovery.v3.ClusterDiscoveryService/FetchClusters")
	missing := m.getMethodCount("/nonexistent")

	if count1 != 2 {
		t.Errorf("method 1 count = %d, want 2", count1)
	}
	if count2 != 1 {
		t.Errorf("method 2 count = %d, want 1", count2)
	}
	if missing != 0 {
		t.Errorf("missing method count = %d, want 0", missing)
	}
}

func TestGRPCMetricsSnapshot(t *testing.T) {
	m := globalGRPCMetrics
	before := m.RequestsTotal.Load()
	beforeFailed := m.RequestsFailed.Load()
	m.RequestsTotal.Add(5)
	m.RequestsFailed.Add(1)
	m.incMethod("/test.Method")

	got := GRPCMetricsSnapshot()
	if got.RequestsTotal < 5 {
		t.Errorf("requests_total = %d, want at least 5", got.RequestsTotal)
	}
	if got.RequestsFailed < 1 {
		t.Errorf("requests_failed = %d, want at least 1", got.RequestsFailed)
	}
	if got.RequestsByMethod["/test.Method"] == 0 {
		t.Error("requests_by_method missing /test.Method")
	}

	m.RequestsTotal.Store(before)
	m.RequestsFailed.Store(beforeFailed)
}

func TestGRPCMetricsConcurrent(t *testing.T) {
	m := &GRPCMetrics{RequestsByMethod: make(map[string]*atomic.Uint64)}
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			m.incMethod("/test.Method")
			m.RequestsTotal.Add(1)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
	if m.RequestsTotal.Load() != 100 {
		t.Errorf("RequestsTotal = %d, want 100", m.RequestsTotal.Load())
	}
	if m.getMethodCount("/test.Method") != 100 {
		t.Errorf("method count = %d, want 100", m.getMethodCount("/test.Method"))
	}
}

func TestStreamInterceptorTracksActiveConnections(t *testing.T) {
	silenceLogs(t)
	m := globalGRPCMetrics
	beforeTotal := m.ConnectionsTotal.Load()
	beforeActive := m.ActiveConnections.Load()
	beforeReq := m.RequestsTotal.Load()

	interceptor := StreamServerInterceptor()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error)
	go func() {
		done <- interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/test.Stream"}, func(any, grpc.ServerStream) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	if got := m.ConnectionsTotal.Load(); got != beforeTotal+1 {
		t.Errorf("connections_total = %d, want %d", got, beforeTotal+1)
	}
	if got := m.ActiveConnections.Load(); got != beforeActive+1 {
		t.Errorf("active_connections = %d, want %d", got, beforeActive+1)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got := m.ConnectionsTotal.Load(); got != beforeTotal+1 {
		t.Errorf("connections_total after close = %d, want %d", got, beforeTotal+1)
	}
	if got := m.ActiveConnections.Load(); got != beforeActive {
		t.Errorf("active_connections after close = %d, want %d", got, beforeActive)
	}
	if got := m.RequestsTotal.Load(); got != beforeReq+1 {
		t.Errorf("requests_total = %d, want %d", got, beforeReq+1)
	}
}
