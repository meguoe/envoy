package xdsserver

// snapshot.go —— 快照组装与推送到 Envoy
//
// Delta xDS 模式下，快照缓存通过 ConstructVersionMap() 构建
// 每个资源的哈希版本映射，自动比对并只推送变更的资源，
// 控制面无需再维护全局内容哈希做去重。
//
// 快照推送由后台轮询器触发：DB revision 变化 → 加载全量规则 → 构建快照 → 写入 SnapshotCache。
// Envoy ACK 全部 typeURL 后该 revision 标记为 deployed。

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

// pushSnapshotLocked 构建快照并推送到 Envoy
// 调用方必须持有 e.pushMu，保证规则修改与推送的原子性
// 本函数不修改 e.rules，不触发持久化；构建失败的规则跳过但保留在内存中
func (e *Engine) pushSnapshotLocked() error {
	version := fmt.Sprintf("%d", atomic.AddUint64(&e.versionSeq, 1))
	return e.pushSnapshotLockedWithVersion(version)
}

// pushSnapshotLockedWithVersion 构建快照并推送到 Envoy，使用指定版本号
func (e *Engine) pushSnapshotLockedWithVersion(version string) error {
	// 在 pushMu 保护下取快照，此时不会有其他操作修改 rules
	e.mu.RLock()
	snapshot := make(map[string]*ProxyRule, len(e.rules))
	for name, r := range e.rules {
		snapshot[name] = r
	}
	e.mu.RUnlock()

	// 增量同步（pushMu 串行化，resCache 无并发读写）
	failedRules := e.syncResCache(snapshot, e.connectTimeout, e.udpIdleTimeout)
	if len(failedRules) > 0 {
		return fmt.Errorf("资源构建失败，跳过规则: %s", strings.Join(failedRules, ", "))
	}

	// 排序组装（仅使用 resCache 中构建成功的规则）
	names := make([]string, 0, len(e.resCache))
	for name := range e.resCache {
		names = append(names, name)
	}
	sort.Strings(names)

	eps, cls, rts, lis := e.collectResources(names)

	// 构建快照
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

	expectedURLs := expectedTypeURLs(resources)

	// 非空 snapshot：先 TrackExpected 再 SetSnapshot，避免 ACK 先到但 expected 为空的竞态
	if len(expectedURLs) > 0 {
		if tracker, ok := e.callbacks.(interface {
			TrackExpected(int64, []string)
		}); ok {
			var revision int64
			if _, err := fmt.Sscanf(version, "%d", &revision); err == nil {
				tracker.TrackExpected(revision, expectedURLs)
			}
		}
	}

	snap, err := cache.NewSnapshot(version, resources)
	if err != nil {
		return fmt.Errorf("创建快照失败: %w", err)
	}
	// 构建资源版本映射（Delta xDS 需要，用于比对每个资源的版本）
	if err := snap.ConstructVersionMap(); err != nil {
		return fmt.Errorf("构建版本映射失败: %w", err)
	}
	if err := e.snapCache.SetSnapshot(context.Background(), e.nodeID, snap); err != nil {
		return fmt.Errorf("设置快照失败: %w", err)
	}

	// 空 snapshot（如删除所有规则后）没有资源需要 ACK，SetSnapshot 成功后直接标记 deployed
	if len(expectedURLs) == 0 {
		var revision int64
		if _, err := fmt.Sscanf(version, "%d", &revision); err == nil {
			log.Printf("空快照推送  ver=%s  直接标记 deployed", version)
			if tracker, ok := e.callbacks.(interface {
				MarkRevisionDeployed(int64)
			}); ok {
				tracker.MarkRevisionDeployed(revision)
			}
		}
	}

	if len(names) > 0 {
		log.Printf("快照推送  ver=%s  rules=%d", version, len(names))
	}
	return nil
}

func expectedTypeURLs(resources map[resourcev3.Type][]types.Resource) []string {
	typeURLs := make([]string, 0, len(resources))
	for typeURL := range resources {
		typeURLs = append(typeURLs, string(typeURL))
	}
	return typeURLs
}
