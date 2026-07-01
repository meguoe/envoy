package xdsserver

// cache.go —— 增量缓存同步
//
// 通过指针比较检测变更，只重建新增/变更的规则资源
//
// 注意: resCache 的读写由 pushMu 保护，仅在 pushSnapshotLocked 内调用，
// 不可并发访问。

import (
	"log/slog"
	"time"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
)

// syncResCache 增量同步资源缓存
// 返回构建失败的规则名列表
func (e *Engine) syncResCache(current map[string]*ProxyRule, connectTimeout, udpIdleTimeout time.Duration) []string {
	gen := e.rulesGen
	// 清理已删除
	for name := range e.resCache {
		if _, ok := current[name]; !ok {
			delete(e.resCache, name)
		}
	}
	// 重建新增或变更（generation 不匹配说明规则列表已变更）
	var failed []string
	for name, rule := range current {
		cached, ok := e.resCache[name]
		if !ok || cached.generation != gen {
			res, err := buildOneRule(rule, connectTimeout, udpIdleTimeout)
			if err != nil {
				slog.Error("构建资源失败", "name", name, "error", err)
				failed = append(failed, name)
				delete(e.resCache, name)
				continue
			}
			res.generation = gen
			e.resCache[name] = res
		}
	}
	return failed
}

// collectResources 从缓存中按类型收集资源列表
func (e *Engine) collectResources(names []string) (eps, cls, rts, lis []types.Resource) {
	var epCount, rtCount int
	for _, name := range names {
		cr := e.resCache[name]
		if cr.endpoint != nil {
			epCount++
		}
		if cr.route != nil {
			rtCount++
		}
	}
	eps = make([]types.Resource, 0, epCount)
	cls = make([]types.Resource, 0, len(names))
	rts = make([]types.Resource, 0, rtCount)
	lis = make([]types.Resource, 0, len(names))
	for _, name := range names {
		cr := e.resCache[name]
		if cr.endpoint != nil {
			eps = append(eps, cr.endpoint)
		}
		cls = append(cls, cr.cluster)
		if cr.route != nil {
			rts = append(rts, cr.route)
		}
		lis = append(lis, cr.listener)
	}
	return
}
