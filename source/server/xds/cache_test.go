package xdsserver

import (
	"testing"
	"time"
)

// TestSyncResCacheAddNewRules 测试同步资源缓存添加新规则。
func TestSyncResCacheAddNewRules(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.mu.Lock()
	e.rulesGen = 1
	e.mu.Unlock()

	rules := map[string]*ProxyRule{
		"web": {
			Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN",
		},
		"api": {
			Name: "api", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 3000}}, LBPolicy: "LEAST_REQUEST",
		},
	}

	failed := e.syncResCache(rules, time.Second, 60*time.Second)
	if len(failed) > 0 {
		t.Fatalf("unexpected failures: %v", failed)
	}
	if len(e.resCache) != 2 {
		t.Errorf("resCache len = %d, want 2", len(e.resCache))
	}
}

// TestSyncResCacheDeleteRemovedRules 测试同步资源缓存删除已移除的规则。
func TestSyncResCacheDeleteRemovedRules(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.mu.Lock()
	e.rulesGen = 1
	e.mu.Unlock()

	// 先添加两条规则
	rules1 := map[string]*ProxyRule{
		"web": {Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		"api": {Name: "api", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 3000}}, LBPolicy: "ROUND_ROBIN"},
	}
	e.syncResCache(rules1, time.Second, 60*time.Second)

	// 只保留 web
	e.mu.Lock()
	e.rulesGen = 2
	e.mu.Unlock()
	rules2 := map[string]*ProxyRule{
		"web": {Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
	}
	e.syncResCache(rules2, time.Second, 60*time.Second)

	if len(e.resCache) != 1 {
		t.Errorf("resCache len = %d, want 1", len(e.resCache))
	}
	if _, ok := e.resCache["api"]; ok {
		t.Error("api should have been removed from resCache")
	}
}

// TestSyncResCacheSkipUnchangedRules 测试相同 generation 下不重复构建资源。
func TestSyncResCacheSkipUnchangedRules(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.mu.Lock()
	e.rulesGen = 1
	e.mu.Unlock()

	rules := map[string]*ProxyRule{
		"web": {Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
	}
	e.syncResCache(rules, time.Second, 60*time.Second)

	// 同一 generation 不重建
	failed := e.syncResCache(rules, time.Second, 60*time.Second)
	if len(failed) > 0 {
		t.Fatalf("unexpected failures: %v", failed)
	}
	// resCache 应保持原样
	if _, ok := e.resCache["web"]; !ok {
		t.Error("web should still be in resCache")
	}
}

// TestCollectResources 测试按规则名称收集 Envoy 资源（endpoint/cluster/route/listener）。
func TestCollectResources(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.mu.Lock()
	e.rulesGen = 1
	e.mu.Unlock()

	rules := map[string]*ProxyRule{
		"http_rule": {Name: "http_rule", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		"udp_rule": {Name: "udp_rule", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 53}}, LBPolicy: "ROUND_ROBIN"},
	}
	e.syncResCache(rules, time.Second, 60*time.Second)

	names := []string{"http_rule", "udp_rule"}
	eps, cls, rts, lis := e.collectResources(names)

	// HTTP 规则有 endpoint 和 route，UDP 没有
	if len(eps) != 1 {
		t.Errorf("eps count = %d, want 1", len(eps))
	}
	if len(cls) != 2 {
		t.Errorf("cls count = %d, want 2", len(cls))
	}
	if len(rts) != 1 {
		t.Errorf("rts count = %d, want 1", len(rts))
	}
	if len(lis) != 2 {
		t.Errorf("lis count = %d, want 2", len(lis))
	}
}

// TestCollectResourcesEmpty 测试空名称列表返回空资源集合。
func TestCollectResourcesEmpty(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	eps, cls, rts, lis := e.collectResources(nil)
	if len(eps) != 0 || len(cls) != 0 || len(rts) != 0 || len(lis) != 0 {
		t.Error("all resource slices should be empty for nil names")
	}
}
