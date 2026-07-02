package httpserver

// ratelimit.go —— 简易令牌桶速率限制
//
// 基于 IP 的令牌桶算法，保护 API 免受暴力攻击。
// 默认: 每个 IP 每秒 20 个请求，突发容量 40。

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// tokenBucket 表示单个 IP 的令牌桶状态。
type tokenBucket struct {
	tokens    float64
	lastTime  time.Time
	rate      float64
	maxTokens float64
}

const maxBuckets = 10000

// RateLimiter 基于 IP 的令牌桶限流器。
type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*tokenBucket
	rate     float64
	capacity float64
	cancel   context.CancelFunc
}

// NewRateLimiter 创建令牌桶限流器，rps 为每秒补充速率，burst 为突发容量。
func NewRateLimiter(rps float64, burst float64) *RateLimiter {
	ctx, cancel := context.WithCancel(context.Background())
	rl := &RateLimiter{
		buckets:  make(map[string]*tokenBucket),
		rate:     rps,
		capacity: burst,
		cancel:   cancel,
	}
	go rl.cleanup(ctx)
	return rl
}

// Stop 停止限流器的后台清理协程。
func (rl *RateLimiter) Stop() {
	if rl.cancel != nil {
		rl.cancel()
	}
}

// Allow 判断指定 key 的请求是否被允许，超过桶容量时返回 false。
func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		if len(rl.buckets) >= maxBuckets {
			return false
		}
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

// cleanup 定期清理超过 10 分钟未活动的令牌桶。
func (rl *RateLimiter) cleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
}

// rateLimitMiddleware 基于客户端 IP 进行限流，超出限制时返回 429。
func rateLimitMiddleware(next http.Handler, rl *RateLimiter) http.Handler {
	if rl == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := rateLimitKey(r)
		if !rl.Allow(key) {
			m.incRateLimited()
			respErr(w, 429, "请求过于频繁，请稍后重试")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitKey 从请求中提取客户端 IP 作为限流的 key。
func rateLimitKey(r *http.Request) string {
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return clientIP
}
