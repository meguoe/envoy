package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xdsserver "envoy-control-plane/source/server/xds"
)

// mockStore 是用于单元测试的内存存储实现。
type mockStore struct {
	rules      []*xdsserver.ProxyRule
	revision   int64
	mutateErr  error
	loadErr    error
	loadOneErr error
}

// MutateRulesAndBumpRevision 在内存中执行规则变更并递增 revision。
func (m *mockStore) MutateRulesAndBumpRevision(_ context.Context, mutate func([]*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error)) ([]*xdsserver.ProxyRule, int64, error) {
	if m.mutateErr != nil {
		return nil, 0, m.mutateErr
	}
	rules, err := mutate(m.rules)
	if err != nil {
		return nil, 0, err
	}
	m.rules = rules
	m.revision++
	return m.rules, m.revision, nil
}

// Load 返回内存中所有规则。
func (m *mockStore) Load(_ context.Context) ([]*xdsserver.ProxyRule, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return m.rules, nil
}

// LoadOne 根据 ID 查找并返回单条规则。
func (m *mockStore) LoadOne(_ context.Context, id string) (*xdsserver.ProxyRule, error) {
	if m.loadOneErr != nil {
		return nil, m.loadOneErr
	}
	for _, r := range m.rules {
		if r.ID == id {
			return r, nil
		}
	}
	return nil, nil
}

// newTestHandler 创建用于测试的引擎和 HTTP 处理器。
func newTestHandler(t *testing.T) (*xdsserver.Engine, http.Handler) {
	t.Helper()
	engine, _, handler := newTestHandlerWithStore(t)
	return engine, handler
}

// newTestHandlerWithStore 创建用于测试的引擎、mock 存储和 HTTP 处理器。
func newTestHandlerWithStore(t *testing.T) (*xdsserver.Engine, *mockStore, http.Handler) {
	t.Helper()
	engine := xdsserver.NewEngine("test-node", time.Second, 60*time.Second)
	store := &mockStore{}
	return engine, store, NewHandler(engine, store, nil, 1<<20, nil, 0, 0)
}

// TestCreateRule201 测试创建规则接口返回 201 状态码。
func TestCreateRule201(t *testing.T) {
	_, handler := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{
		"name":        "test",
		"listen_port": 9981,
		"listen_addr": "0.0.0.0",
		"backends":    []map[string]any{{"address": "127.0.0.1", "port": 8080, "weight": 1}},
		"lb_policy":   "ROUND_ROBIN",
	})
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 201 {
		t.Errorf("POST /rules status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	var resp apiResp
	json.Unmarshal(w.Body.Bytes(), &resp)
	if !resp.Success {
		t.Errorf("POST /rules success=false, message=%s", resp.Message)
	}
}

// TestCreateRulePreservesStoreRulesWhenEngineCacheIsStale 测试引擎缓存未同步时创建规则不会覆盖存储中已有的规则。
func TestCreateRulePreservesStoreRulesWhenEngineCacheIsStale(t *testing.T) {
	engine, store, handler := newTestHandlerWithStore(t)
	store.rules = []*xdsserver.ProxyRule{{
		ID: "external", Name: "external", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9980, Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}}

	body, _ := json.Marshal(map[string]any{
		"name":        "created",
		"listen_port": 9981,
		"listen_addr": "0.0.0.0",
		"backends":    []map[string]any{{"address": "127.0.0.1", "port": 8081, "weight": 1}},
		"lb_policy":   "ROUND_ROBIN",
	})
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 201 {
		t.Fatalf("POST /rules status = %d, want 201; body: %s", w.Code, w.Body.String())
	}
	if len(store.rules) != 2 {
		t.Fatalf("store rules = %d, want 2", len(store.rules))
	}
	if store.rules[0].ID != "external" && store.rules[1].ID != "external" {
		t.Fatalf("external rule was overwritten: %+v", store.rules)
	}
	if got := len(engine.ListRules()); got != 0 {
		t.Fatalf("engine rules = %d, want 0 before poller push", got)
	}
}

// TestCreateRule400EmptyName 测试创建规则时名称为空返回 400 状态码。
func TestCreateRule400EmptyName(t *testing.T) {
	_, handler := newTestHandler(t)
	body := []byte(`{"name":"","listen_port":9981,"listen_addr":"0.0.0.0","backends":[{"address":"127.0.0.1","port":8080}]}`)
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("POST /rules empty name: status = %d, want 400", w.Code)
	}
}

// TestCreateRule400InvalidJSON 测试请求体为无效 JSON 时返回 400 状态码。
func TestCreateRule400InvalidJSON(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/rules", bytes.NewReader([]byte(`{bad`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("POST /rules bad JSON: status = %d, want 400", w.Code)
	}
}

// TestGetRule200 测试根据 ID 获取已存在规则返回 200 状态码。
func TestGetRule200(t *testing.T) {
	_, store, handler := newTestHandlerWithStore(t)
	store.rules = []*xdsserver.ProxyRule{{
		ID: "r1", Name: "test", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}}
	req := httptest.NewRequest(http.MethodGet, "/rules/r1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("GET /rules/{id} status = %d, want 200", w.Code)
	}
}

// TestGetRule404 测试获取不存在的规则返回 404 状态码。
func TestGetRule404(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("GET /rules/{id} missing: status = %d, want 404", w.Code)
	}
}

// TestUpdateRule200 测试更新已存在规则返回 200 状态码。
func TestUpdateRule200(t *testing.T) {
	_, store, handler := newTestHandlerWithStore(t)
	rules := []*xdsserver.ProxyRule{{
		ID: "r1", Name: "test", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}}
	store.rules = rules
	body, _ := json.Marshal(map[string]any{
		"listen_port": 9982,
		"backends":    []map[string]any{{"address": "127.0.0.1", "port": 9090, "weight": 1}},
	})
	req := httptest.NewRequest(http.MethodPut, "/rules/r1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("PUT /rules/{id} status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
}

// TestUpdateRule404 测试更新不存在的规则返回 404 状态码。
func TestUpdateRule404(t *testing.T) {
	_, handler := newTestHandler(t)
	body := []byte(`{"listen_port":9982}`)
	req := httptest.NewRequest(http.MethodPut, "/rules/nonexistent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("PUT /rules/{id} missing: status = %d, want 404", w.Code)
	}
}

// TestDeleteRule200 测试删除已存在规则返回 200 状态码。
func TestDeleteRule200(t *testing.T) {
	_, store, handler := newTestHandlerWithStore(t)
	rules := []*xdsserver.ProxyRule{{
		ID: "r1", Name: "test", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}}
	store.rules = rules
	req := httptest.NewRequest(http.MethodDelete, "/rules/r1", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("DELETE /rules/{id} status = %d, want 200", w.Code)
	}
}

// TestDeleteRule404 测试删除不存在的规则返回 404 状态码。
func TestDeleteRule404(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/rules/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("DELETE /rules/{id} missing: status = %d, want 404", w.Code)
	}
}

// TestListRules200 测试获取规则列表返回 200 状态码。
func TestListRules200(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("GET /rules status = %d, want 200", w.Code)
	}
}

// TestInternalErrorsAreRedacted 测试 500 响应不会暴露底层错误细节。
func TestInternalErrorsAreRedacted(t *testing.T) {
	engine := xdsserver.NewEngine("test-node", time.Second, 60*time.Second)
	store := &mockStore{loadErr: errors.New("postgres password leaked")}
	handler := NewHandler(engine, store, nil, 1<<20, nil, 0, 0)

	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 500 {
		t.Fatalf("GET /rules status = %d, want 500", w.Code)
	}
	if strings.Contains(w.Body.String(), "postgres password leaked") {
		t.Fatalf("response leaked internal error: %s", w.Body.String())
	}
	var resp apiResp
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response JSON: %v", err)
	}
	if resp.Message != "读取规则失败" {
		t.Fatalf("message = %q, want %q", resp.Message, "读取规则失败")
	}
}

// TestHealthCheck200 测试健康检查接口返回 200 状态码。
func TestHealthCheck200(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("GET /health status = %d, want 200", w.Code)
	}
}

// TestMethodNotAllowed 测试不支持的 HTTP 方法返回 405 状态码。
func TestMethodNotAllowed(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 405 {
		t.Errorf("DELETE /health: status = %d, want 405", w.Code)
	}
}

// TestAuthRejectNoKey 测试未携带 API 密钥时返回 401 状态码。
func TestAuthRejectNoKey(t *testing.T) {
	handler := NewHandler(nil, nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("no API key: status = %d, want 401", w.Code)
	}
}

// TestAuthRejectWrongKey 测试携带错误 API 密钥时返回 401 状态码。
func TestAuthRejectWrongKey(t *testing.T) {
	handler := NewHandler(nil, nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-API-KEY", "wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("wrong API key: status = %d, want 401", w.Code)
	}
}

// TestAuthRejectQueryAPIKey 测试通过查询参数传递 API 密钥时返回 401 状态码。
func TestAuthRejectQueryAPIKey(t *testing.T) {
	handler := NewHandler(nil, nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health?api_key=secret", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("query API key: status = %d, want 401", w.Code)
	}
}

// TestRulesEndpointMissingID 测试访问 /rules/ 路径但缺少规则 ID 时返回 400 状态码。
func TestRulesEndpointMissingID(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("GET /rules/ (empty id): status = %d, want 400", w.Code)
	}
}

// TestCreateRuleTrailingContent 测试请求体包含多余内容时返回 400 状态码。
func TestCreateRuleTrailingContent(t *testing.T) {
	_, handler := newTestHandler(t)
	body := `{"name":"trail","listen_port":9981,"listen_addr":"0.0.0.0","backends":[{"address":"127.0.0.1","port":80}]}garbage`
	req := httptest.NewRequest(http.MethodPost, "/rules", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("trailing content: status = %d, want 400", w.Code)
	}
}

// TestUpdateRuleTrailingContent 测试更新规则时请求体包含多余内容返回 400 状态码。
func TestUpdateRuleTrailingContent(t *testing.T) {
	engine, handler := newTestHandler(t)
	engine.SetRules([]*xdsserver.ProxyRule{{
		ID: "r1", Name: "trail", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 80}},
		LBPolicy: "ROUND_ROBIN",
	}})

	body := `{"listen_port":9982} trailing`
	req := httptest.NewRequest(http.MethodPut, "/rules/r1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("update trailing content: status = %d, want 400", w.Code)
	}
}

// TestRateLimiter 测试令牌桶限流器的基本突发和限流行为。
func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(2, 2)
	key := "10.0.0.1"

	if !rl.Allow(key) {
		t.Fatal("first request should be allowed")
	}
	if !rl.Allow(key) {
		t.Fatal("second request should be allowed (burst)")
	}
	if rl.Allow(key) {
		t.Fatal("third request should be rate limited")
	}
}

// TestRateLimiterDifferentKeys 测试不同来源 IP 的限流计数相互独立。
func TestRateLimiterDifferentKeys(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	if !rl.Allow("10.0.0.1") {
		t.Fatal("first IP should be allowed")
	}
	if !rl.Allow("10.0.0.2") {
		t.Fatal("different IP should be allowed")
	}
}

// TestMetrics200 测试指标接口返回 200 状态码及正确的 JSON 结构。
func TestMetrics200(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			HTTP metricsSnapshot                   `json:"http"`
			GRPC xdsserver.GRPCMetricsSnapshotData `json:"grpc"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("metrics JSON: %v", err)
	}
	if !resp.Success {
		t.Fatal("metrics success=false")
	}
	if resp.Data.HTTP.RequestsTotal == 0 {
		t.Error("metrics http.requests_total should be present")
	}
}

// TestMetricsMethodNotAllowed 测试使用 POST 方法访问指标接口返回 405 状态码。
func TestMetricsMethodNotAllowed(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 405 {
		t.Errorf("POST /metrics: status = %d, want 405", w.Code)
	}
}

// TestMetricsIncrementsOnRequest 测试请求处理后指标计数器正确递增。
func TestMetricsIncrementsOnRequest(t *testing.T) {
	_, handler := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	req2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	var resp struct {
		Data struct {
			HTTP metricsSnapshot `json:"http"`
		} `json:"data"`
	}
	json.Unmarshal(w2.Body.Bytes(), &resp)

	if resp.Data.HTTP.RequestsTotal == 0 {
		t.Error("metrics should contain http.requests_total")
	}
}

// TestMetricsIncrementsOnError 测试错误请求后错误计数器正确递增。
func TestMetricsIncrementsOnError(t *testing.T) {
	_, handler := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/rules/nonexistent-id-abc123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("GET /rules/bad: status = %d, want 404", w.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	var resp struct {
		Data struct {
			HTTP metricsSnapshot `json:"http"`
		} `json:"data"`
	}
	json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp.Data.HTTP.ErrorsTotal == 0 {
		t.Error("metrics should contain http.errors_total")
	}
}

// TestMetricsRateLimited 测试被限流的请求返回 429 状态码。
func TestMetricsRateLimited(t *testing.T) {
	engine, handler := newTestHandler(t)
	_ = engine

	rl := NewRateLimiter(1, 1)
	remoteAddr := "10.0.0.1:1234"
	rl.Allow("10.0.0.1")
	rl.Allow("10.0.0.1")

	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	req.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()

	rlHandler := rateLimitMiddleware(handler, rl)
	rlHandler.ServeHTTP(w, req)
	if w.Code != 429 {
		t.Errorf("rate limited: status = %d, want 429", w.Code)
	}
}

// TestRateLimiterIgnoresUntrustedXFF 测试限流器忽略不可信的 X-Forwarded-For 头，使用 RemoteAddr 作为限流键。
func TestRateLimiterIgnoresUntrustedXFF(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	handler := rateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}), rl)

	for i, xff := range []string{"10.0.0.1", "10.0.0.2"} {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.RemoteAddr = "1.2.3.4:12345"
		req.Header.Set("X-Forwarded-For", xff)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if i == 0 && w.Code != 204 {
			t.Fatalf("first request status = %d, want 204", w.Code)
		}
		if i == 1 && w.Code != 429 {
			t.Fatalf("second request status = %d, want 429", w.Code)
		}
	}
}

// TestMetricsSnapshot 测试指标快照正确反映各计数器的当前值。
func TestMetricsSnapshot(t *testing.T) {
	me := &metrics{startTime: time.Now()}
	me.requestsTotal.Add(10)
	me.errorsTotal.Add(2)
	me.rateLimited.Add(1)
	got := me.snapshot()
	if got.RequestsTotal != 10 {
		t.Errorf("requests_total = %d, want 10", got.RequestsTotal)
	}
	if got.ErrorsTotal != 2 {
		t.Errorf("errors_total = %d, want 2", got.ErrorsTotal)
	}
	if got.RateLimited != 1 {
		t.Errorf("rate_limited_total = %d, want 1", got.RateLimited)
	}
}

// ─── RateLimiter 单元测试 ──────────────────────────────────────────────

// TestRateLimiterBurstExhaustion 测试令牌桶突发容量耗尽后请求被限流。
func TestRateLimiterBurstExhaustion(t *testing.T) {
	rl := NewRateLimiter(1, 3)
	key := "test-ip"
	for i := 0; i < 3; i++ {
		if !rl.Allow(key) {
			t.Fatalf("request %d should be allowed (burst=3)", i+1)
		}
	}
	if rl.Allow(key) {
		t.Fatal("4th request should be rate limited")
	}
}

// TestRateLimiterTokenRefill 测试令牌桶在间隔一段时间后正确补充令牌。
func TestRateLimiterTokenRefill(t *testing.T) {
	rl := NewRateLimiter(10, 1)
	key := "refill-ip"
	if !rl.Allow(key) {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow(key) {
		t.Fatal("second request should be rate limited (capacity=1)")
	}
	// 等待 200ms，应该补充 2 个 token (10 rps * 0.2s = 2)
	time.Sleep(200 * time.Millisecond)
	if !rl.Allow(key) {
		t.Fatal("request after refill should be allowed")
	}
}

// TestRateLimiterMaxBuckets 测试达到桶数量上限后新来源被拒绝，已有来源不受影响。
func TestRateLimiterMaxBuckets(t *testing.T) {
	rl := NewRateLimiter(100, 100)
	for i := 0; i < maxBuckets; i++ {
		rl.Allow(fmt.Sprintf("ip-%d", i))
	}
	// 达到上限后，新 key 应被拒绝
	if rl.Allow("new-ip") {
		t.Fatal("should reject when max buckets reached")
	}
	// 已存在的 key 仍然允许
	if !rl.Allow("ip-0") {
		t.Fatal("existing key should still be allowed")
	}
}

// TestRateLimiterStop 测试限流器停止后 Allow 方法不会 panic。
func TestRateLimiterStop(t *testing.T) {
	rl := NewRateLimiter(10, 10)
	rl.Stop()
	// Stop 后 Allow 仍可调用，不会 panic
	rl.Allow("after-stop")
}

// TestRateLimitMiddlewareBypass 测试限流器为 nil 时中间件放行所有请求。
func TestRateLimitMiddlewareBypass(t *testing.T) {
	// rl=nil 时应该放行所有请求
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(200)
	})
	handler := rateLimitMiddleware(inner, nil)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if !called {
		t.Fatal("inner handler should be called when rl is nil")
	}
	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// TestRateLimitKeyExtraction 测试从 RemoteAddr 中正确提取 IP 作为限流键。
func TestRateLimitKeyExtraction(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected string
	}{
		{"normal", "10.0.0.1:1234", "10.0.0.1"},
		{"no-port", "10.0.0.1", "10.0.0.1"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.addr
			got := rateLimitKey(req)
			if got != tt.expected {
				t.Errorf("rateLimitKey() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// ─── Auth 中间件单元测试 ──────────────────────────────────────────────

// TestAuthMiddlewareEmptyKey 测试配置为空 APIKey 时认证关闭，所有请求放行。
func TestAuthMiddlewareEmptyKey(t *testing.T) {
	// 配置了空 APIKey → 认证关闭，所有请求放行
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(200)
	})
	handler := authMiddleware(inner, &AuthConfig{APIKey: ""})
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if !called {
		t.Fatal("inner handler should be called when APIKey is empty")
	}
}

// TestAuthMiddlewareCorrectKey 测试携带正确 API 密钥时请求被放行。
func TestAuthMiddlewareCorrectKey(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	handler := authMiddleware(inner, &AuthConfig{APIKey: "my-secret-key"})
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-KEY", "my-secret-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("correct key: status = %d, want 200", w.Code)
	}
}

// TestAuthMiddlewareNoKeyHeader 测试未设置 X-API-KEY 头时返回 401 状态码。
func TestAuthMiddlewareNoKeyHeader(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	handler := authMiddleware(inner, &AuthConfig{APIKey: "my-secret-key"})
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// 不设置 X-API-KEY header
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("no key header: status = %d, want 401", w.Code)
	}
}

// TestAuthMiddlewareCaseSensitive 测试 API 密钥比较区分大小写。
func TestAuthMiddlewareCaseSensitive(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	handler := authMiddleware(inner, &AuthConfig{APIKey: "my-secret-key"})

	// 不同大小写应被拒绝（ConstantTimeCompare 是字节精确比较）
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-KEY", "MY-SECRET-KEY")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("wrong case key: status = %d, want 401", w.Code)
	}
}

// TestAuthMiddlewareHandlersAreIsolated 测试不同 handler 的认证配置互不污染。
func TestAuthMiddlewareHandlersAreIsolated(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	alphaHandler := authMiddleware(inner, &AuthConfig{APIKey: "alpha"})
	betaHandler := authMiddleware(inner, &AuthConfig{APIKey: "beta"})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-API-KEY", "alpha")
	w := httptest.NewRecorder()
	alphaHandler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("alpha key: status = %d, want 200", w.Code)
	}

	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req2.Header.Set("X-API-KEY", "alpha")
	betaHandler.ServeHTTP(w2, req2)
	if w2.Code != 401 {
		t.Errorf("alpha key on beta handler: status = %d, want 401", w2.Code)
	}

	w3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req3.Header.Set("X-API-KEY", "beta")
	betaHandler.ServeHTTP(w3, req3)
	if w3.Code != 200 {
		t.Errorf("beta key: status = %d, want 200", w3.Code)
	}
}
