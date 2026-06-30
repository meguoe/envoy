package httpserver

// http.go —— HTTP API
//
// 职责：
//   - 统一响应格式
//   - CRUD 请求处理（基于规则 ID）
//   - 成功后触发持久化

import (
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

type Server struct {
	engine       *xdsserver.Engine
	maxBodyBytes int64
}

func NewHandler(engine *xdsserver.Engine, maxBodyBytes int64, authCfg *AuthConfig, rps, burst float64) http.Handler {
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	if rps <= 0 {
		rps = 20
	}
	if burst <= 0 {
		burst = 40
	}
	s := &Server{engine: engine, maxBodyBytes: maxBodyBytes}
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
		writeJSON(w, 200, map[string]any{
			"code":    200,
			"success": true,
			"message": "ok",
			"data": map[string]any{
				"status":           "up",
				"rules":            len(s.engine.ListRules()),
				"envoy_connected":  envoyConnected,
				"persist_failures": s.engine.PersistFailures(),
				"uptime_seconds":   int64(time.Since(m.startTime).Seconds()),
				"requests_total":   m.requestsTotal.Load(),
				"errors_total":     m.errorsTotal.Load(),
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
			s.handleDelete(w, id)
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
	// 客户端不应传 ID，忽略
	rule.ID = ""

	created, err := s.engine.CreateRule(&rule)
	if err != nil {
		status := 500
		var ve *xdsserver.ValidationError
		if errors.As(err, &ve) {
			status = 400
		}
		respErr(w, status, err.Error())
		return
	}
	respCreated(w, created)
}

func (s *Server) handleGetOne(w http.ResponseWriter, id string) {
	rule, ok := s.engine.GetRule(id)
	if !ok {
		respErr(w, 404, "规则不存在")
		return
	}
	respOK(w, rule)
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
	// body 中的 ID 忽略，以 URL 为准
	rule.ID = ""

	// 获取已有规则
	old, ok := s.engine.GetRule(id)
	if !ok {
		respErr(w, 404, "规则不存在")
		return
	}

	// Name: 默认继承，不允许变更
	rule.Name = old.Name

	// ListenPort: 0 表示未传，继承旧值；非 0 则严格校验范围
	if rule.ListenPort == 0 {
		rule.ListenPort = old.ListenPort
	} else if rule.ListenPort < 10 || rule.ListenPort > 65535 {
		respErr(w, 400, "listen_port 超出范围 (10-65535)")
		return
	}

	// 未传字段继承旧值
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

	updated, err := s.engine.UpdateRule(id, &rule)
	if err != nil {
		status := 500
		if errors.Is(err, xdsserver.ErrRuleNotFound) {
			status = 404
		} else {
			var ve *xdsserver.ValidationError
			if errors.As(err, &ve) {
				status = 400
			}
		}
		respErr(w, status, err.Error())
		return
	}
	respOK(w, updated)
}

func (s *Server) handleList(w http.ResponseWriter) {
	respOK(w, s.engine.ListRules())
}

func (s *Server) handleDelete(w http.ResponseWriter, id string) {
	if err := s.engine.DeleteRule(id); err != nil {
		status := 500
		if errors.Is(err, xdsserver.ErrRuleNotFound) {
			status = 404
		}
		respErr(w, status, err.Error())
		return
	}
	respOK(w, map[string]string{"id": id})
}
