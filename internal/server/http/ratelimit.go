package httpserver

// ratelimit.go —— 简易令牌桶速率限制
//
// 基于 IP 的令牌桶算法，保护 API 免受暴力攻击。
// 默认: 每个 IP 每秒 20 个请求，突发容量 40。

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type tokenBucket struct {
	tokens    float64
	lastTime  time.Time
	rate      float64
	maxTokens float64
}

type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     float64
	capacity float64
}

func NewRateLimiter(rps float64, burst float64) *RateLimiter {
	rl := &RateLimiter{
		buckets:  make(map[string]*tokenBucket),
		rate:     rps,
		capacity: burst,
	}
	go rl.cleanup()
	return rl
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &tokenBucket{
			tokens:    rl.capacity - 1,
			lastTime:  now,
			rate:      rl.rate,
			maxTokens: rl.capacity,
		}
		rl.buckets[key] = b
		return true
	}

	elapsed := now.Sub(b.lastTime).Seconds()
	b.tokens += elapsed * b.rate
	if b.tokens > b.maxTokens {
		b.tokens = b.maxTokens
	}
	b.lastTime = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for k, b := range rl.buckets {
			if now.Sub(b.lastTime) > 10*time.Minute {
				delete(rl.buckets, k)
			}
		}
		rl.mu.Unlock()
	}
}

func rateLimitMiddleware(next http.Handler, rl *RateLimiter, authCfg *AuthConfig) http.Handler {
	if rl == nil {
		return next
	}
	proxies := ipSet{}
	if authCfg != nil {
		proxies = newIPSet(authCfg.TrustedProxies)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := rateLimitKey(r, proxies)
		if !rl.Allow(key) {
			m.incRateLimited()
			respErr(w, 429, "请求过于频繁，请稍后重试")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func rateLimitKey(r *http.Request, proxies ipSet) string {
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		clientIP = r.RemoteAddr
	}
	if proxies.len() > 0 {
		if remoteIP := net.ParseIP(clientIP); remoteIP != nil && proxies.contains(remoteIP) {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				return resolveXFF(xff, proxies, clientIP)
			}
		}
	}
	if clientIP != "" {
		return clientIP
	}
	return strings.TrimSpace(r.RemoteAddr)
}
