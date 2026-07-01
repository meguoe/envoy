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
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	xdsserver "envoy-control-plane/source/server/xds"
)

const (
	DefaultMaxBodyBytes = 5 << 20 // 5MB 请求体上限
)

var reqSeq uint64

type Store interface {
	MutateRulesAndBumpRevision(ctx context.Context, mutate func([]*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error)) ([]*xdsserver.ProxyRule, int64, error)
	Load(ctx context.Context) ([]*xdsserver.ProxyRule, error)
	LoadOne(ctx context.Context, id string) (*xdsserver.ProxyRule, error)
}

type Server struct {
	engine       *xdsserver.Engine
	store        Store
	notifier     xdsserver.RuleChangeNotifier
	maxBodyBytes int64
	rateLimiter  *RateLimiter
	authCfg      *AuthConfig
}

// NewServer 创建 HTTP API Server。
func NewServer(engine *xdsserver.Engine, store Store, notifier xdsserver.RuleChangeNotifier, maxBodyBytes int64, authCfg *AuthConfig, rps, burst float64) *Server {
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	if rps <= 0 {
		rps = 20
	}
	if burst <= 0 {
		burst = 40
	}
	return &Server{
		engine:       engine,
		store:        store,
		notifier:     notifier,
		maxBodyBytes: maxBodyBytes,
		rateLimiter:  NewRateLimiter(rps, burst),
		authCfg:      authCfg,
	}
}

// NewHandler 创建 HTTP API 处理器，配置路由、认证、限流和访问日志中间件。
func NewHandler(engine *xdsserver.Engine, store Store, notifier xdsserver.RuleChangeNotifier, maxBodyBytes int64, authCfg *AuthConfig, rps, burst float64) http.Handler {
	return NewServer(engine, store, notifier, maxBodyBytes, authCfg, rps, burst).Handler()
}

// Handler 返回带认证、限流和访问日志中间件的 HTTP handler。
func (s *Server) Handler() http.Handler {
	return logHTTP(rateLimitMiddleware(authMiddleware(s.buildMux(), s.authCfg), s.rateLimiter))
}

// Stop 停止 Server 内部的后台 goroutine（如限流器清理）
func (s *Server) Stop() {
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
}

// ─── 响应结构体 ────────────────────────────────────────────────────────

type apiResp struct {
	Code    int    `json:"code"`
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// respOK 写入 200 状态码的成功响应。
func respOK(w http.ResponseWriter, data any) {
	writeJSON(w, 200, apiResp{Code: 200, Success: true, Message: "ok", Data: data})
}

// respCreated 写入 201 状态码的成功响应，用于资源创建场景。
func respCreated(w http.ResponseWriter, data any) {
	writeJSON(w, 201, apiResp{Code: 201, Success: true, Message: "ok", Data: data})
}

// respErr 写入指定状态码的错误响应。
func respErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiResp{Code: status, Success: false, Message: msg})
}

// respInternal 记录内部错误，但不把底层错误细节返回给客户端。
func respInternal(w http.ResponseWriter, msg string, err error) {
	if err != nil {
		slog.Error(msg, "error", err)
	}
	respErr(w, 500, msg)
}

// writeJSON 将结构体序列化为 JSON 并写入 HTTP 响应，自动设置 Content-Type 和 X-Request-Id 头。
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"code":500,"success":false,"message":"internal error"}`))
		return
	}
	reqID := nextReqID()
	if lw, ok := w.(*accessLogResponseWriter); ok && lw.reqID != "" {
		reqID = lw.reqID
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("X-Request-Id", reqID)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// nextReqID 返回自增的十六进制请求序列号。
func nextReqID() string {
	return fmt.Sprintf("%08x", atomic.AddUint64(&reqSeq, 1))
}

// ─── HTTP 路由 ─────────────────────────────────────────────────────────

// buildMux 构建 HTTP 路由，注册健康检查、指标、节点信息和规则 CRUD 端点。
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
			s.handleList(w, r)
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
			s.handleGetOne(w, r, id)
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

// WriteHeader 拦截状态码写入，确保只记录第一次设置的状态码。
func (w *accessLogResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Write 写入响应体数据，若未设置状态码则默认为 200。
func (w *accessLogResponseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// logHTTP 中间件记录每个 HTTP 请求的方法、路径、状态码和耗时。
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

// sanitizeRequestID 清理请求 ID，仅保留字母、数字和短横线，最长 128 字符。
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

// logAccess 输出访问日志，根据状态码级别分别记录，同时更新 metrics 计数器。
func logAccess(r *http.Request, reqID string, status int, duration time.Duration) {
	m.incRequests()
	if status >= 400 {
		m.incErrors()
	}
	attrs := []any{
		"request_id", reqID,
		"method", r.Method,
		"path", r.URL.Path,
		"status", status,
		"duration", duration.Round(time.Millisecond).String(),
		"remote", r.RemoteAddr,
	}
	switch {
	case status >= 500:
		logAttrs(slog.LevelError, "request", attrs...)
	case status >= 400:
		logAttrs(slog.LevelWarn, "request", attrs...)
	default:
		logAttrs(slog.LevelInfo, "request", attrs...)
	}
}

// handleCreate 处理 POST /rules 请求，解析 JSON 请求体并创建新规则。
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var rule xdsserver.ProxyRule
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&rule); err != nil {
		respErr(w, 400, "JSON 解析失败")
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

	id, err := xdsserver.GenerateID()
	if err != nil {
		respInternal(w, "生成规则 ID 失败", err)
		return
	}
	rule.ID = id
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, newRev, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
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
		respInternal(w, "保存规则失败", err)
		return
	}

	if s.notifier != nil {
		s.notifier.NotifyRevision(newRev)
	}
	respCreated(w, &rule)
}

// handleGetOne 处理 GET /rules/{id} 请求，返回指定 ID 的规则详情。
func (s *Server) handleGetOne(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rule, err := s.store.LoadOne(ctx, id)
	if err != nil {
		respInternal(w, "读取规则失败", err)
		return
	}
	if rule == nil {
		respErr(w, 404, "规则不存在")
		return
	}
	respOK(w, rule)
}

// handleUpdate 处理 PUT /rules/{id} 请求，更新指定规则的配置字段。
func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request, id string) {
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	var rule xdsserver.ProxyRule
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&rule); err != nil {
		respErr(w, 400, "JSON 解析失败")
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
	_, newRev, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
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
		respInternal(w, "保存规则失败", err)
		return
	}

	if s.notifier != nil {
		s.notifier.NotifyRevision(newRev)
	}
	respOK(w, updatedRule)
}

// handleList 处理 GET /rules 请求，返回所有规则的列表。
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rules, err := s.store.Load(ctx)
	if err != nil {
		respInternal(w, "读取规则失败", err)
		return
	}
	respOK(w, rules)
}

// handleDelete 处理 DELETE /rules/{id} 请求，删除指定 ID 的规则。
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, newRev, err := s.store.MutateRulesAndBumpRevision(ctx, func(currentRules []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
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
		respInternal(w, "删除规则失败", err)
		return
	}

	if s.notifier != nil {
		s.notifier.NotifyRevision(newRev)
	}
	respOK(w, map[string]string{"id": id})
}
