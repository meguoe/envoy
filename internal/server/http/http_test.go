package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xdsserver "envoy-control-plane/internal/server/xds"
)

type mockStore struct {
	rules    []*xdsserver.ProxyRule
	revision int64
}

func (m *mockStore) MutateRulesAndBumpRevision(_ context.Context, mutate func([]*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error)) ([]*xdsserver.ProxyRule, int64, error) {
	rules, err := mutate(m.rules)
	if err != nil {
		return nil, 0, err
	}
	m.rules = rules
	m.revision++
	return m.rules, m.revision, nil
}

func (m *mockStore) Load(_ context.Context) ([]*xdsserver.ProxyRule, error) {
	return m.rules, nil
}

func newTestHandler(t *testing.T) (*xdsserver.Engine, http.Handler) {
	t.Helper()
	engine, _, handler := newTestHandlerWithStore(t)
	return engine, handler
}

func newTestHandlerWithStore(t *testing.T) (*xdsserver.Engine, *mockStore, http.Handler) {
	t.Helper()
	engine := xdsserver.NewEngine("test-node", time.Second, 60*time.Second)
	store := &mockStore{}
	return engine, store, NewHandler(engine, store, 1<<20, nil, 0, 0)
}

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

func TestGetRule404(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("GET /rules/{id} missing: status = %d, want 404", w.Code)
	}
}

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

func TestDeleteRule404(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/rules/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("DELETE /rules/{id} missing: status = %d, want 404", w.Code)
	}
}

func TestListRules200(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("GET /rules status = %d, want 200", w.Code)
	}
}

func TestHealthCheck200(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("GET /health status = %d, want 200", w.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodDelete, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 405 {
		t.Errorf("DELETE /health: status = %d, want 405", w.Code)
	}
}

func TestAuthRejectNoKey(t *testing.T) {
	handler := NewHandler(nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("no API key: status = %d, want 401", w.Code)
	}
}

func TestAuthRejectWrongKey(t *testing.T) {
	handler := NewHandler(nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-API-KEY", "wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("wrong API key: status = %d, want 401", w.Code)
	}
}

func TestAuthRejectQueryAPIKey(t *testing.T) {
	handler := NewHandler(nil, nil, 1<<20, &AuthConfig{APIKey: "secret"}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health?api_key=secret", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 {
		t.Errorf("query API key: status = %d, want 401", w.Code)
	}
}

func TestAuthRejectIPNotInAllowlist(t *testing.T) {
	handler := NewHandler(nil, nil, 1<<20, &AuthConfig{AllowedIPs: []string{"10.0.0.1"}}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Errorf("IP not in allowlist: status = %d, want 403", w.Code)
	}
}

func TestRulesEndpointMissingID(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/rules/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 400 {
		t.Errorf("GET /rules/ (empty id): status = %d, want 400", w.Code)
	}
}

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

func TestAuthTrustedProxy(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.1.100"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.100")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("trusted proxy XFF: status = %d, want 200", w.Code)
	}
}

func TestAuthTrustedProxySpoofed(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.1.100"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.999:12345"
	req.Header.Set("X-Forwarded-For", "10.0.0.1")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Errorf("untrusted proxy spoofing: status = %d, want 403", w.Code)
	}
}

func TestAuthXFFChainWalksRightToLeft(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.1.100"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	req.Header.Set("X-Forwarded-For", "spoofed, 10.0.0.1, 192.168.1.100")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("spoofed prefix ignored: status = %d, want 200", w.Code)
	}
}

func TestAuthXFFChainSkipsMultipleProxies(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.1.100", "192.168.1.20"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.20, 192.168.1.100")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("multi-proxy chain with real client: status = %d, want 200", w.Code)
	}
}

func TestAuthXFFSpoofedAllowedIPRejected(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.1.100"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.1.100:12345"
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.99, 192.168.1.100")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Errorf("spoofed XFF with real untrusted client: status = %d, want 403", w.Code)
	}
}

func TestAuthAllowedCIDR(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs: []string{"10.0.0.0/24"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "10.0.0.42:54321"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("CIDR match: status = %d, want 200", w.Code)
	}
}

func TestAuthAllowedCIDRNoMatch(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs: []string{"10.0.0.0/24"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "10.0.1.1:54321"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Errorf("CIDR no match: status = %d, want 403", w.Code)
	}
}

func TestAuthTrustedProxyCIDR(t *testing.T) {
	engine, _ := newTestHandler(t)
	handler := NewHandler(engine, nil, 1<<20, &AuthConfig{
		AllowedIPs:     []string{"10.0.0.1"},
		TrustedProxies: []string{"192.168.0.0/16"},
	}, 0, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "192.168.5.100:12345"
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.5.100")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("trusted proxy CIDR: status = %d, want 200", w.Code)
	}
}

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

func TestRateLimiterDifferentKeys(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	if !rl.Allow("10.0.0.1") {
		t.Fatal("first IP should be allowed")
	}
	if !rl.Allow("10.0.0.2") {
		t.Fatal("different IP should be allowed")
	}
}

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

func TestMetricsMethodNotAllowed(t *testing.T) {
	_, handler := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 405 {
		t.Errorf("POST /metrics: status = %d, want 405", w.Code)
	}
}

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

	rlHandler := rateLimitMiddleware(handler, rl, nil)
	rlHandler.ServeHTTP(w, req)
	if w.Code != 429 {
		t.Errorf("rate limited: status = %d, want 429", w.Code)
	}
}

func TestRateLimiterIgnoresUntrustedXFF(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	handler := rateLimitMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	}), rl, nil)

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
