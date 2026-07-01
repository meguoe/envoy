package httpserver

// auth.go —— HTTP API 认证中间件
//
// 通过 X-API-KEY 头进行身份验证。
// 支持运行时热更新 API Key（SIGHUP reload）。

import (
	"crypto/subtle"
	"net/http"
	"sync/atomic"
)

type AuthConfig struct {
	APIKey string
}

// currentAuthConfig 存储当前认证配置，支持原子替换
var currentAuthConfig atomic.Pointer[AuthConfig]

// init 初始化认证配置为空实例。
func init() {
	currentAuthConfig.Store(&AuthConfig{})
}

// SetAuthConfig 热更新认证配置
func SetAuthConfig(cfg *AuthConfig) {
	if cfg == nil {
		currentAuthConfig.Store(&AuthConfig{})
	} else {
		currentAuthConfig.Store(cfg)
	}
}

// authMiddleware 基于 X-API-KEY 头进行请求认证，API_KEY 为空时跳过验证。
func authMiddleware(next http.Handler, _ *AuthConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := currentAuthConfig.Load()
		if cfg.APIKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-KEY")), []byte(cfg.APIKey)) != 1 {
			respErr(w, 401, "无效的 API Key")
			return
		}
		next.ServeHTTP(w, r)
	})
}
