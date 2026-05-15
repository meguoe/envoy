package xdsServer

// snapshot.go —— 快照组装与推送到 Envoy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"log"
	"sort"
	"sync/atomic"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	proto "google.golang.org/protobuf/proto"
)

// pushSnapshotLocked 构建快照并推送到 Envoy
// 调用方必须持有 e.pushMu，保证规则修改与推送的原子性
func (e *Engine) pushSnapshotLocked() error {
	// 在 pushMu 保护下取快照，此时不会有其他 CRUD 操作修改 rules
	e.mu.RLock()
	snapshot := make(map[string]*ProxyRule, len(e.rules))
	for name, r := range e.rules {
		snapshot[name] = r
	}
	e.mu.RUnlock()

	// 增量同步（pushMu 串行化，resCache 无并发读写）
	failedRules := e.syncResCache(snapshot)

	// 构建失败的规则从内存中移除
	if len(failedRules) > 0 {
		e.mu.Lock()
		for _, name := range failedRules {
			delete(e.rules, name)
			log.Printf("🗑️  构建失败，移除规则: %s", name)
		}
		e.mu.Unlock()
		// 通知外部持久化（非阻塞）
		go func() {
			if e.onRulesChanged != nil {
				e.onRulesChanged()
			}
		}()
	}

	// 排序组装
	names := make([]string, 0, len(e.resCache))
	for name := range e.resCache {
		names = append(names, name)
	}
	sort.Strings(names)

	eps, cls, rts, lis := e.collectResources(names)

	// SHA-256 去重
	h := sha256.New()
	for _, res := range eps {
		if err := writeHash(h, res); err != nil {
			return fmt.Errorf("hash endpoint: %w", err)
		}
	}
	for _, res := range cls {
		if err := writeHash(h, res); err != nil {
			return fmt.Errorf("hash cluster: %w", err)
		}
	}
	for _, res := range rts {
		if err := writeHash(h, res); err != nil {
			return fmt.Errorf("hash route: %w", err)
		}
	}
	for _, res := range lis {
		if err := writeHash(h, res); err != nil {
			return fmt.Errorf("hash listener: %w", err)
		}
	}
	newHash := fmt.Sprintf("%x", h.Sum(nil))

	if newHash == e.lastContentHash {
		log.Printf("📦 Snapshot unchanged, skip push  rules=%d", len(names))
		return nil
	}

	// 推送
	resources := map[resourcev3.Type][]types.Resource{}
	if len(eps) > 0 {
		resources[resourcev3.EndpointType] = eps
	}
	if len(cls) > 0 {
		resources[resourcev3.ClusterType] = cls
	}
	if len(rts) > 0 {
		resources[resourcev3.RouteType] = rts
	}
	if len(lis) > 0 {
		resources[resourcev3.ListenerType] = lis
	}

	version := fmt.Sprintf("%d", atomic.AddUint64(&e.versionSeq, 1))

	snap, err := cache.NewSnapshot(version, resources)
	if err != nil {
		return fmt.Errorf("new snapshot: %w", err)
	}

	if err := e.snapCache.SetSnapshot(context.Background(), e.nodeID, snap); err != nil {
		return fmt.Errorf("set snapshot: %w", err)
	}

	e.lastContentHash = newHash
	log.Printf("📦 Snapshot pushed  ver=%s  rules=%d", version, len(names))
	return nil
}

func writeHash(h hash.Hash, r types.Resource) error {
	if msg, ok := r.(proto.Message); ok {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
		if err != nil {
			return fmt.Errorf("marshal resource: %w", err)
		}
		h.Write(data)
	}
	return nil
}
