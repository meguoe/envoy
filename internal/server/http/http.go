package httpserver

// http.go —— HTTP API
//
// 职责：
//   - 统一响应格式
//   - CRUD 请求处理（基于规则 ID）
//   - 成功后触发持久化

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	xdsserver "envoy-control-plane/internal/server/xds"
)

const (
	DefaultMaxBodyBytes = 1 << 20 // 1MB 请求体上限
)

var reqSeq uint64

type requestIDProvider interface {
	requestID() string
}

type Store interface {
	MutateRulesAndBumpRevision(ctx context.Context, mutate func([]*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error)) ([]*xdsserver.ProxyRule, int64, error)
	Load(ctx context.Context) ([]*xdsserver.ProxyRule, error)
}

type Server struct {
	engine       *xdsserver.Engine
	store        Store
	maxBodyBytes int64
}

func NewHandler(engine *xdsserver.Engine, store Store, maxBodyBytes int64, authCfg *AuthConfig, rps, burst float64) http.Handler {
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	if rps <= 0 {
		rps = 20
	}
	if burst <= 0 {
		burst = 40
	}
	s := &Server{engine: engine, store: store, maxBodyBytes: maxBodyBytes}
	rl := NewRateLimiter(rps, burst)
	return logHTTP(rateLimitMiddleware(authMiddleware(s.buildMux(), authCfg), rl, authCfg))
}

// ─── 响应结构体 ────────────────────────────────────────────────────────

type apiResp struct {
	Code    int    `json:"code"`
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func respOK(w http.ResponseWriter, data any) {
	writeJSON(w, 200, apiResp{Code: 200, Success: true, Message: "ok", Data: data})
}

func respCreated(w http.ResponseWriter, data any) {
	writeJSON(w, 201, apiResp{Code: 201, Success: true, Message: "ok", Data: data})
}

func respErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiResp{Code: status, Success: false, Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		w.Write([]byte(`{"code":500,"success":false,"message":"internal error"}`))
		return
	}
	reqID := nextReqID()
	if p, ok := w.(requestIDProvider); ok && p.requestID() != "" {
		reqID = p.requestID()
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("X-Request-Id", reqID)
	w.WriteHeader(status)
	w.Write(body)
}

func nextReqID() string {
	return fmt.Sprintf("%08x", atomic.AddUint64(&reqSeq, 1))
}

// ─── HTTP 路由 ─────────────────────────────────────────────────────────

func (s *Server) buildMux() http.Handler {
	mux := http.NewServeMux()

	// 健康检查
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respErr(w, 405, "仅支持 GET")
			return
		}
		envoyConnected := s.engine.IsEnvoyConnected()
		knownRev := s.engine.KnownRevision()
		deployedRev := s.engine.LastDeployedRevision()
		writeJSON(w, 200, map[string]any{
			"code":    200,
			"success": true,
			"message": "ok",
			"data": map[string]any{
				"status":            "up",
				"rules":             len(s.engine.ListRules()),
				"envoy_connected":   envoyConnected,
				"known_revision":    knownRev,
				"deployed_revision": deployedRev,
				"uptime_seconds":    int64(time.Since(m.startTime).Seconds()),
				"requests_total":    m.requestsTotal.Load(),
				"errors_total":      m.errorsTotal.Load(),
			},
		})
	})

	// 服务运行指标 (HTTP + gRPC)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respErr(w, 405, "仅支持 GET")
			return
		}
		respOK(w, map[string]any{
			"http": m.snapshot(),
			"grpc": xdsserver.GRPCMetricsSnapshot(),
		})
	})

	// Envoy 节点信息
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respErr(w, 405, "仅支持 GET")
			return
		}
		respOK(w, s.engine.GetEnvoyNodes())
	})

	mux.HandleFunc("/rules", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.handleCreate(w, r)
		case http.MethodGet:
			s.handleList(w)
		default:
			respErr(w, 405, "仅支持 POST 或 GET")
		}
	})

	mux.HandleFunc("/rules/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/rules/")
		if id == "" {
			respErr(w, 400, "缺少规则 ID")
			return
		}
		switch r.Method {
		case http.MethodGet:
			s.handleGetOne(w, id)
		case http.MethodPut:
			s.handleUpdate(w, r, id)
		case http.MethodDelete:
			s.handleDelete(w, r, id)
		default:
			respErr(w, 405, "仅支持 GET、PUT 或 DELETE")
		}
	})

	return mux
}

type accessLogResponseWriter struct {
	http.ResponseWriter
	status int
	reqID  string
}

func (w *accessLogResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *accessLogResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *accessLogResponseWriter) requestID() string { return w.reqID }

func logHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := sanitizeRequestID(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = nextReqID()
		}
		lw := &accessLogResponseWriter{ResponseWriter: w, reqID: reqID}
		next.ServeHTTP(lw, r)
		if lw.status == 0 {
			lw.status = http.StatusOK
		}
		logAccess(r, reqID, lw.status, time.Since(start))
	})
}

func sanitizeRequestID(id string) string {
	b := make([]byte, 0, len(id))
	for _, c := range id {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' {
			b = append(b, byte(c))
		}
	}
	if len(b) > 128 {
		b = b[:128]
	}
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

func logAccess(r *http.Request, reqID string, status int, duration time.Duration) {
	m.incRequests()
	if status >= 400 {
		m.incErrors()
	}
	if IsStructuredLogging() {
		attrs := map[string]any{
			"request_id": reqID,
			"method":     r.Method,
			"path":       r.URL.Path,
			"status":     status,
			"duration":   duration.Round(time.Millisecond).String(),
			"remote":     r.RemoteAddr,
		}
		switch {
		case status >= 500:
			slogError("request", attrs)
		case status >= 400:
			slogWarn("request", attrs)
		default:
			slogInfo("request", attrs)
		}
		return
	}
	format := "request_id=%s method=%s path=%s status=%d duration=%s remote=%s"
	args := []any{reqID, r.Method, r.URL.Path, status, duration.Round(time.Millisecond), r.RemoteAddr}
	switch {
	case status >= 500:
		logError(format, args...)
	case status >= 400:
		logWarn(format, args...)
	default:
		logInfo(format, args...)
	}
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var rule xdsserver.ProxyRule
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&rule); err != nil {
		respErr(w, 400, err.Error())
		return
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		respErr(w, 400, "JSON 后有多余内容")
		return
	}
	rule.ID = ""

	rule.ID = xdsserver.GenerateID()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, _, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		nextRules := make([]*xdsserver.ProxyRule, 0, len(currentRules)+1)
		nextRules = append(nextRules, currentRules...)
		nextRules = append(nextRules, &rule)
		if err := xdsserver.CheckRulesConflicts(nextRules); err != nil {
			return nil, err
		}
		return nextRules, nil
	})
	if err != nil {
		var ve *xdsserver.ValidationError
		if errors.As(err, &ve) {
			respErr(w, 400, err.Error())
			return
		}
		respErr(w, 500, fmt.Sprintf("保存规则失败: %v", err))
		return
	}

	respCreated(w, &rule)
}

func (s *Server) handleGetOne(w http.ResponseWriter, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rules, err := s.store.Load(ctx)
	if err != nil {
		respErr(w, 500, fmt.Sprintf("读取规则失败: %v", err))
		return
	}
	for _, rule := range rules {
		if rule.ID == id {
			respOK(w, rule)
			return
		}
	}
	respErr(w, 404, "规则不存在")
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request, id string) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var rule xdsserver.ProxyRule
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&rule); err != nil {
		respErr(w, 400, err.Error())
		return
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		respErr(w, 400, "JSON 后有多余内容")
		return
	}
	rule.ID = ""

	if rule.ListenPort != 0 && (rule.ListenPort < 10 || rule.ListenPort > 65535) {
		respErr(w, 400, "listen_port 超出范围 (10-65535)")
		return
	}

	rule.ID = id
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var updatedRule *xdsserver.ProxyRule
	_, _, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		nextRules := make([]*xdsserver.ProxyRule, 0, len(currentRules))
		found := false
		for _, r := range currentRules {
			if r.ID == id {
				old := r
				rule.Name = old.Name
				if rule.ListenPort == 0 {
					rule.ListenPort = old.ListenPort
				}
				if rule.Protocol == "" {
					rule.Protocol = old.Protocol
				}
				if rule.ListenAddr == "" {
					rule.ListenAddr = old.ListenAddr
				}
				if rule.LBPolicy == "" {
					rule.LBPolicy = old.LBPolicy
				}
				if len(rule.Backends) == 0 {
					rule.Backends = old.Backends
				}
				cp := rule
				updatedRule = &cp
				nextRules = append(nextRules, &cp)
				found = true
			} else {
				nextRules = append(nextRules, r)
			}
		}
		if !found {
			return nil, xdsserver.ErrRuleNotFound
		}
		if err := xdsserver.CheckRulesConflicts(nextRules); err != nil {
			return nil, err
		}
		return nextRules, nil
	})
	if err != nil {
		if err == xdsserver.ErrRuleNotFound {
			respErr(w, 404, "规则不存在")
			return
		}
		var ve *xdsserver.ValidationError
		if errors.As(err, &ve) {
			respErr(w, 400, err.Error())
			return
		}
		respErr(w, 500, fmt.Sprintf("保存规则失败: %v", err))
		return
	}

	respOK(w, updatedRule)
}

func (s *Server) handleList(w http.ResponseWriter) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rules, err := s.store.Load(ctx)
	if err != nil {
		respErr(w, 500, fmt.Sprintf("读取规则失败: %v", err))
		return
	}
	respOK(w, rules)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, _, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		found := false
		nextRules := make([]*xdsserver.ProxyRule, 0, len(currentRules))
		for _, r := range currentRules {
			if r.ID == id {
				found = true
			} else {
				nextRules = append(nextRules, r)
			}
		}
		if !found {
			return nil, xdsserver.ErrRuleNotFound
		}
		return nextRules, nil
	})
	if err != nil {
		if err == xdsserver.ErrRuleNotFound {
			respErr(w, 404, "规则不存在")
			return
		}
		respErr(w, 500, fmt.Sprintf("删除规则失败: %v", err))
		return
	}

	respOK(w, map[string]string{"id": id})
}
