package httpserver

// auth.go —— HTTP API 认证中间件
//
// 支持两层防护：
//   1. X-API-KEY 认证（需配置 api_key）
//   2. IP 白名单（需配置 allowed_ips），支持精确 IP 和 CIDR
//
// 支持反向代理：
//   如果 trusted_proxies 配置了代理 IP/CIDR，当请求来源 IP 匹配时，
//   从 X-Forwarded-For 提取真实客户端 IP 进行校验。

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strings"
)

type AuthConfig struct {
	APIKey         string
	AllowedIPs     []string
	TrustedProxies []string
}

type ipSet struct {
	ips   map[string]net.IP
	cidrs []*net.IPNet
}

func newIPSet(raw []string) ipSet {
	s := ipSet{ips: make(map[string]net.IP)}
	for _, addr := range raw {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if strings.Contains(addr, "/") {
			_, cidr, err := net.ParseCIDR(addr)
			if err == nil {
				s.cidrs = append(s.cidrs, cidr)
			}
		} else {
			if ip := net.ParseIP(addr); ip != nil {
				s.ips[ip.String()] = ip
			}
		}
	}
	return s
}

func (s ipSet) contains(ip net.IP) bool {
	if _, ok := s.ips[ip.String()]; ok {
		return true
	}
	for _, cidr := range s.cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func (s ipSet) len() int {
	return len(s.ips) + len(s.cidrs)
}

// resolveXFF walks the X-Forwarded-For chain from right to left,
// skipping trusted proxy IPs, and returns the first non-proxy IP.
// fallback is used when the entire chain consists of trusted proxies.
func resolveXFF(xff string, proxies ipSet, fallback string) string {
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		ip := net.ParseIP(strings.TrimSpace(parts[i]))
		if ip == nil {
			continue
		}
		if !proxies.contains(ip) {
			return ip.String()
		}
	}
	if fallback != "" {
		return fallback
	}
	return strings.TrimSpace(parts[0])
}

func authMiddleware(next http.Handler, cfg *AuthConfig) http.Handler {
	if cfg == nil || (cfg.APIKey == "" && len(cfg.AllowedIPs) == 0) {
		return next
	}
	allowed := newIPSet(cfg.AllowedIPs)
	proxies := newIPSet(cfg.TrustedProxies)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowed.len() > 0 {
			clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				clientIP = r.RemoteAddr
			}
			if clientIP == "::1" {
				clientIP = "127.0.0.1"
			}
			if proxies.len() > 0 {
				if remoteIP := net.ParseIP(clientIP); remoteIP != nil && proxies.contains(remoteIP) {
					if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
						clientIP = resolveXFF(xff, proxies, clientIP)
					}
				}
			}
			parsed := net.ParseIP(clientIP)
			if parsed == nil || !allowed.contains(parsed) {
				respErr(w, 403, "来源 IP 不允许")
				return
			}
		}
		if cfg.APIKey != "" {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-KEY")), []byte(cfg.APIKey)) != 1 {
				respErr(w, 401, "无效的 API Key")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
