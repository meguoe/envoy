package main

// api.go —— HTTP API
//
// 职责：
//   - 统一响应格式
//   - CRUD 请求处理（基于规则 ID）
//   - 成功后触发持久化

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	xdsServer "envoy-control-plane/xds_server"
)

var reqSeq uint64

const (
	grpcAddr     = ":18000"
	apiAddr      = ":18001"
	maxBodyBytes = 1 << 20 // 1MB 请求体上限
)

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
	reqID := fmt.Sprintf("%08x", atomic.AddUint64(&reqSeq, 1))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	w.Header().Set("X-Request-Id", reqID)
	w.WriteHeader(status)
	w.Write(body)
}

// ─── HTTP 路由 ─────────────────────────────────────────────────────────

func buildHTTPMux() http.Handler {
	mux := http.NewServeMux()

	// 健康检查
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respErr(w, 405, "GET")
			return
		}
		envoyConnected := engine.IsEnvoyConnected()
		writeJSON(w, 200, map[string]any{
			"code":    200,
			"success": true,
			"message": "ok",
			"data": map[string]any{
				"status":          "up",
				"rules":           len(engine.ListRules()),
				"envoy_connected": envoyConnected,
			},
		})
	})

	// Envoy 节点信息
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respErr(w, 405, "GET")
			return
		}
		respOK(w, engine.GetEnvoyNodes())
	})

	mux.HandleFunc("/rules", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreate(w, r)
		case http.MethodGet:
			handleList(w)
		default:
			respErr(w, 405, "POST or GET")
		}
	})

	mux.HandleFunc("/rules/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/rules/")
		if id == "" {
			respErr(w, 400, "missing rule id")
			return
		}
		switch r.Method {
		case http.MethodGet:
			handleGetOne(w, id)
		case http.MethodPut:
			handleUpdate(w, r, id)
		case http.MethodDelete:
			handleDelete(w, id)
		default:
			respErr(w, 405, "GET, PUT or DELETE")
		}
	})

	return mux
}

func handleCreate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var rule xdsServer.ProxyRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		respErr(w, 400, err.Error())
		return
	}
	// 客户端不应传 ID，忽略
	rule.ID = ""

	created, err := engine.CreateRule(&rule)
	if err != nil {
		status := 500
		var ve *xdsServer.ValidationError
		if errors.As(err, &ve) {
			status = 400
		}
		respErr(w, status, err.Error())
		return
	}
	respCreated(w, created)
}

func handleGetOne(w http.ResponseWriter, id string) {
	rule, ok := engine.GetRule(id)
	if !ok {
		respErr(w, 404, "rule not found")
		return
	}
	respOK(w, rule)
}

func handleUpdate(w http.ResponseWriter, r *http.Request, id string) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var rule xdsServer.ProxyRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		respErr(w, 400, err.Error())
		return
	}
	// body 中的 ID 忽略，以 URL 为准
	rule.ID = ""

	// 获取已有规则
	old, ok := engine.GetRule(id)
	if !ok {
		respErr(w, 404, "rule not found")
		return
	}

	// Name: 默认继承，不允许变更
	rule.Name = old.Name

	// ListenPort: 0 表示未传，继承旧值；非 0 则严格校验范围
	if rule.ListenPort == 0 {
		rule.ListenPort = old.ListenPort
	} else if rule.ListenPort < 10 || rule.ListenPort > 65534 {
		respErr(w, 400, "listen_port 超出范围 (10-65534)")
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

	updated, err := engine.UpdateRule(id, &rule)
	if err != nil {
		status := 500
		if errors.Is(err, xdsServer.ErrRuleNotFound) {
			status = 404
		} else {
			var ve *xdsServer.ValidationError
			if errors.As(err, &ve) {
				status = 400
			}
		}
		respErr(w, status, err.Error())
		return
	}
	respOK(w, updated)
}

func handleList(w http.ResponseWriter) {
	respOK(w, engine.ListRules())
}

func handleDelete(w http.ResponseWriter, id string) {
	if err := engine.DeleteRule(id); err != nil {
		status := 500
		if errors.Is(err, xdsServer.ErrRuleNotFound) {
			status = 404
		}
		respErr(w, status, err.Error())
		return
	}
	respOK(w, map[string]string{"id": id})
}
