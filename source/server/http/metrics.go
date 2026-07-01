package httpserver

// metrics.go —— 简易 metrics 计数器
//
// 在内存中维护请求计数、错误计数等指标，
// 通过 /metrics 端点暴露为 JSON。

import (
	"sync/atomic"
	"time"
)

// metrics 维护 HTTP 请求计数指标。
type metrics struct {
	requestsTotal atomic.Uint64
	errorsTotal   atomic.Uint64
	rateLimited   atomic.Uint64
	startTime     time.Time
}

var m = &metrics{startTime: time.Now()}

// incRequests 原子递增请求总数计数器。
func (me *metrics) incRequests()    { me.requestsTotal.Add(1) }
// incErrors 原子递增错误计数器。
func (me *metrics) incErrors()      { me.errorsTotal.Add(1) }
// incRateLimited 原子递增被限流的请求计数器。
func (me *metrics) incRateLimited() { me.rateLimited.Add(1) }

// metricsSnapshot 返回当前指标的快照，用于 /metrics 端点响应。
type metricsSnapshot struct {
	UptimeSeconds int64  `json:"uptime_seconds"`
	RequestsTotal uint64 `json:"requests_total"`
	ErrorsTotal   uint64 `json:"errors_total"`
	RateLimited   uint64 `json:"rate_limited_total"`
}

// snapshot 返回当前所有指标值的快照。
func (me *metrics) snapshot() metricsSnapshot {
	return metricsSnapshot{
		UptimeSeconds: int64(time.Since(me.startTime).Seconds()),
		RequestsTotal: me.requestsTotal.Load(),
		ErrorsTotal:   me.errorsTotal.Load(),
		RateLimited:   me.rateLimited.Load(),
	}
}
