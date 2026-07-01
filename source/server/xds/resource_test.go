package xdsserver

import (
	"testing"
	"time"

	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
)

// TestBuildHTTPRule 测试多后端 HTTP 规则生成完整的 endpoint/cluster/route/listener 资源。
func TestBuildHTTPRule(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends: []BackendNode{
			{Address: "127.0.0.1", Port: 8080, Weight: 1},
			{Address: "127.0.0.1", Port: 8081, Weight: 2},
		},
		LBPolicy: "ROUND_ROBIN",
	}

	res, err := buildHTTPRule(rule, time.Second)
	if err != nil {
		t.Fatalf("buildHTTPRule: %v", err)
	}

	if res.endpoint == nil {
		t.Fatal("endpoint should not be nil for HTTP rule")
	}
	if res.cluster == nil {
		t.Fatal("cluster should not be nil for HTTP rule")
	}
	if res.route == nil {
		t.Fatal("route should not be nil for HTTP rule")
	}
	if res.listener == nil {
		t.Fatal("listener should not be nil for HTTP rule")
	}

	// 验证 listener 名称
	li := res.listener.(*listener.Listener)
	if li.Name != "listener_web" {
		t.Errorf("listener name = %q, want listener_web", li.Name)
	}
}

// TestBuildHTTPRuleSingleBackend 测试单后端 HTTP 规则生成完整资源。
func TestBuildHTTPRuleSingleBackend(t *testing.T) {
	rule := &ProxyRule{
		Name:       "api",
		Protocol:   "http",
		ListenAddr: "127.0.0.1",
		ListenPort: 8080,
		Backends:   []BackendNode{{Address: "10.0.0.1", Port: 3000, Weight: 1}},
		LBPolicy:   "LEAST_REQUEST",
	}

	res, err := buildHTTPRule(rule, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("buildHTTPRule: %v", err)
	}

	if res.endpoint == nil || res.cluster == nil || res.route == nil || res.listener == nil {
		t.Error("all resources should be non-nil for HTTP rule")
	}
}

// TestBuildUDPRule 测试 UDP 规则生成 cluster 和 listener，不生成 endpoint 和 route。
func TestBuildUDPRule(t *testing.T) {
	rule := &ProxyRule{
		Name:       "dns",
		Protocol:   "udp",
		ListenAddr: "0.0.0.0",
		ListenPort: 5353,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 53, Weight: 1}},
		LBPolicy:   "RANDOM",
	}

	res, err := buildUDPRule(rule, time.Second, 60*time.Second)
	if err != nil {
		t.Fatalf("buildUDPRule: %v", err)
	}

	// UDP 规则不生成 endpoint 和 route
	if res.endpoint != nil {
		t.Error("endpoint should be nil for UDP rule")
	}
	if res.route != nil {
		t.Error("route should be nil for UDP rule")
	}
	if res.cluster == nil {
		t.Fatal("cluster should not be nil for UDP rule")
	}
	if res.listener == nil {
		t.Fatal("listener should not be nil for UDP rule")
	}

	li := res.listener.(*listener.Listener)
	if li.Name != "listener_dns" {
		t.Errorf("listener name = %q, want listener_dns", li.Name)
	}
}

// TestBuildUDPRuleMultipleBackends 测试多后端 UDP 规则生成 cluster 和 listener。
func TestBuildUDPRuleMultipleBackends(t *testing.T) {
	rule := &ProxyRule{
		Name:       "game",
		Protocol:   "udp",
		ListenAddr: "0.0.0.0",
		ListenPort: 7777,
		Backends: []BackendNode{
			{Address: "10.0.0.1", Port: 7778, Weight: 1},
			{Address: "10.0.0.2", Port: 7778, Weight: 3},
		},
		LBPolicy: "RING_HASH",
	}

	res, err := buildUDPRule(rule, time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("buildUDPRule: %v", err)
	}
	if res.cluster == nil || res.listener == nil {
		t.Error("cluster and listener should be non-nil")
	}
}

// TestParseLBPolicy 测试负载均衡策略字符串解析为 Envoy 枚举值。
func TestParseLBPolicy(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"ROUND_ROBIN", "ROUND_ROBIN"},
		{"LEAST_REQUEST", "LEAST_REQUEST"},
		{"RANDOM", "RANDOM"},
		{"RING_HASH", "RING_HASH"},
		{"", "ROUND_ROBIN"},
		{"unknown", "ROUND_ROBIN"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseLBPolicy(tt.input)
			want := parseLBPolicy(tt.want)
			if got != want {
				t.Errorf("parseLBPolicy(%q) = %v, want %v", tt.input, got, want)
			}
		})
	}
}

// TestBuildOneRuleDispatch 测试 buildOneRule 根据协议类型分发到正确的构建函数。
func TestBuildOneRuleDispatch(t *testing.T) {
	httpRule := &ProxyRule{
		Name: "http-test", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9001,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
	}
	res, err := buildOneRule(httpRule, time.Second, 60*time.Second)
	if err != nil {
		t.Fatalf("buildOneRule http: %v", err)
	}
	if res.endpoint == nil {
		t.Error("http rule should have endpoint")
	}

	udpRule := &ProxyRule{
		Name: "udp-test", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 9002,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 53}},
	}
	res, err = buildOneRule(udpRule, time.Second, 60*time.Second)
	if err != nil {
		t.Fatalf("buildOneRule udp: %v", err)
	}
	if res.endpoint != nil {
		t.Error("udp rule should not have endpoint")
	}
}
