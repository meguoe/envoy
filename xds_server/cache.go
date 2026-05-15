package xdsServer

// cache.go —— 增量缓存同步
//
// 通过指针比较检测变更，只重建新增/变更的规则资源

import (
	"log"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
)

// syncResCache 增量同步资源缓存
// 返回构建失败的规则名列表
func (e *Engine) syncResCache(current map[string]*ProxyRule) []string {
	// 清理已删除
	for name := range e.resCache {
		if _, ok := current[name]; !ok {
			delete(e.resCache, name)
		}
	}
	// 重建新增或变更
	var failed []string
	for name, rule := range current {
		cached, ok := e.resCache[name]
		if !ok || cached.owner != rule {
			res, err := buildOneRule(rule)
			if err != nil {
				log.Printf("⚠️  构建资源失败 name=%s: %v", name, err)
				failed = append(failed, name)
				delete(e.resCache, name)
				continue
			}
			e.resCache[name] = res
		}
	}
	return failed
}

// collectResources 从缓存中按类型收集资源列表
func (e *Engine) collectResources(names []string) (eps, cls, rts, lis []types.Resource) {
	eps = make([]types.Resource, 0, len(names))
	cls = make([]types.Resource, 0, len(names))
	rts = make([]types.Resource, 0, len(names))
	lis = make([]types.Resource, 0, len(names))
	for _, name := range names {
		cr := e.resCache[name]
		if cr.endpoint != nil {
			eps = append(eps, cr.endpoint.(types.Resource))
		}
		cls = append(cls, cr.cluster.(types.Resource))
		if cr.route != nil {
			rts = append(rts, cr.route.(types.Resource))
		}
		lis = append(lis, cr.listener.(types.Resource))
	}
	return
}
