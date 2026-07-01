package httpserver

// auth.go —— HTTP API 认证中间件
//
// 通过 X-API-KEY 头进行身份验证。

import (
	"crypto/subtle"
	"net/http"
)

type AuthConfig struct {
	APIKey string
}

// authMiddleware 基于 X-API-KEY 头进行请求认证，API_KEY 为空时跳过验证。
func authMiddleware(next http.Handler, cfg *AuthConfig) http.Handler {
	apiKey := ""
	if cfg != nil {
		apiKey = cfg.APIKey
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if apiKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-KEY")), []byte(apiKey)) != 1 {
			respErr(w, 401, "无效的 API Key")
			return
		}
		next.ServeHTTP(w, r)
	})
}
