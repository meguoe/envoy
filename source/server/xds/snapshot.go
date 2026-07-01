package xdsserver

// snapshot.go —— 快照组装与推送到 Envoy
//
// Delta xDS 模式下，快照缓存通过 ConstructVersionMap() 构建
// 每个资源的哈希版本映射，自动比对并只推送变更的资源，
// 控制面无需再维护全局内容哈希做去重。
//
// 快照推送由后台轮询器触发：DB revision 变化 → 加载全量规则 → 构建快照 → 写入 SnapshotCache。
// 推送后按实际资源动态确定期望 ACK 集合，超时未齐标记 timeout。

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"

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

	snap, err := cache.NewSnapshot(version, resources)
	if err != nil {
		return fmt.Errorf("创建快照失败: %w", err)
	}
	if err := snap.ConstructVersionMap(); err != nil {
		return fmt.Errorf("构建版本映射失败: %w", err)
	}
	if err := e.snapCache.SetSnapshot(context.Background(), e.nodeID, snap); err != nil {
		return fmt.Errorf("设置快照失败: %w", err)
	}

	// 与上一次快照 diff，计算本次变更的资源名集合
	currSnapshot := snapshotResourceNames(resources)
	changedResources := diffSnapshots(e.prevSnapshot, currSnapshot)
	e.prevSnapshot = currSnapshot

	// 从变更资源中提取期望 ACK 的 typeURL 集合
	expectedURLs := expectedTypeURLsFromDiff(changedResources, resources)

	if len(names) == 0 {
		slog.Info("空快照推送，直接标记 deployed", "version", version)
		if tracker, ok := e.callbacks.(interface {
			MarkRevisionDeployed(int64)
		}); ok {
			if revision, err := parseVersion(version); err != nil {
				slog.Warn("版本号解析失败", "version", version, "error", err)
			} else {
				tracker.MarkRevisionDeployed(revision)
			}
		}
	} else {
		if tracker, ok := e.callbacks.(interface {
			TrackExpected(int64, []string)
		}); ok {
			if revision, err := parseVersion(version); err != nil {
				slog.Warn("版本号解析失败", "version", version, "error", err)
			} else {
				tracker.TrackExpected(revision, expectedURLs)
			}
		}
		slog.Info("快照推送", "version", version, "rules", len(names),
			"LDS", len(lis), "CDS", len(cls), "EDS", len(eps), "RDS", len(rts),
			"changed", changedResourceSummary(changedResources),
			"expected_ack", expectedURLs)
	}
	return nil
}

// snapshotResourceNames 提取快照中每个 typeURL 的资源名集合。
func snapshotResourceNames(resources map[resourcev3.Type][]types.Resource) map[resourcev3.Type]map[string]bool {
	names := make(map[resourcev3.Type]map[string]bool, len(resources))
	for typeURL, list := range resources {
		set := make(map[string]bool, len(list))
		for _, r := range list {
			var name string
			switch v := r.(type) {
			case *listener.Listener:
				name = v.GetName()
			case *cluster.Cluster:
				name = v.GetName()
			default:
				continue
			}
			set[name] = true
		}
		names[typeURL] = set
	}
	return names
}

// diffSnapshots 对比新旧快照，返回变更的资源名集合（新增 + 删除）。
func diffSnapshots(prev, curr map[resourcev3.Type]map[string]bool) map[resourcev3.Type]map[string]bool {
	changed := make(map[resourcev3.Type]map[string]bool)

	// 合并所有 typeURL
	allTypes := make(map[resourcev3.Type]bool)
	for t := range prev {
		allTypes[t] = true
	}
	for t := range curr {
		allTypes[t] = true
	}

	for typeURL := range allTypes {
		oldSet := prev[typeURL]
		newSet := curr[typeURL]
		if oldSet == nil {
			oldSet = make(map[string]bool)
		}
		if newSet == nil {
			newSet = make(map[string]bool)
		}
		diff := make(map[string]bool)
		// 新增的资源
		for name := range newSet {
			if !oldSet[name] {
				diff[name] = true
			}
		}
		// 删除的资源
		for name := range oldSet {
			if !newSet[name] {
				diff[name] = true
			}
		}
		if len(diff) > 0 {
			changed[typeURL] = diff
		}
	}
	return changed
}

// expectedTypeURLsFromDiff 从变更资源集合中提取期望 ACK 的 typeURL 集合，
// 只包含有实际变更的 typeURL，再根据资源内容判断 EDS/RDS 是否需要。
func expectedTypeURLsFromDiff(changed map[resourcev3.Type]map[string]bool, resources map[resourcev3.Type][]types.Resource) []string {
	var typeURLs []string

	// LDS：有 Listener 变更时等待
	if changed[resourcev3.ListenerType] != nil {
		typeURLs = append(typeURLs, string(resourcev3.ListenerType))
	}

	// CDS + EDS：有 Cluster 变更时等待，检查变更的 Cluster 是否需要 EDS
	if changed[resourcev3.ClusterType] != nil {
		typeURLs = append(typeURLs, string(resourcev3.ClusterType))
		needEDS := false
		for _, r := range resources[resourcev3.ClusterType] {
			cl, ok := r.(*cluster.Cluster)
			if !ok || !changed[resourcev3.ClusterType][cl.GetName()] {
				continue
			}
			if _, isEDS := cl.ClusterDiscoveryType.(*cluster.Cluster_Type); isEDS && cl.GetType() == cluster.Cluster_EDS {
				needEDS = true
				break
			}
		}
		if needEDS {
			typeURLs = append(typeURLs, string(resourcev3.EndpointType))
		}
	}

	// RDS：有 Listener 变更时，检查变更的 Listener 是否使用 HCM + RDS
	if changed[resourcev3.ListenerType] != nil {
		needRDS := false
		for _, r := range resources[resourcev3.ListenerType] {
			l, ok := r.(*listener.Listener)
			if !ok || !changed[resourcev3.ListenerType][l.GetName()] {
				continue
			}
			for _, fc := range l.GetFilterChains() {
				for _, f := range fc.GetFilters() {
					if f.GetName() != "envoy.filters.network.http_connection_manager" {
						continue
					}
					hcmCfg := &hcm.HttpConnectionManager{}
					if err := f.GetTypedConfig().UnmarshalTo(hcmCfg); err != nil {
						continue
					}
					if _, ok := hcmCfg.GetRouteSpecifier().(*hcm.HttpConnectionManager_Rds); ok {
						needRDS = true
						break
					}
				}
				if needRDS {
					break
				}
			}
			if needRDS {
				break
			}
		}
		if needRDS {
			typeURLs = append(typeURLs, string(resourcev3.RouteType))
		}
	}

	return typeURLs
}

// changedResourceSummary 返回变更资源的摘要字符串。
func changedResourceSummary(changed map[resourcev3.Type]map[string]bool) string {
	var parts []string
	for typeURL, names := range changed {
		short := strings.TrimPrefix(string(typeURL), "type.googleapis.com/envoy.config.")
		short = strings.TrimSuffix(short, "/v3")
		parts = append(parts, fmt.Sprintf("%s=%d", short, len(names)))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// parseVersion 将版本字符串解析为 int64 类型的 revision。
func parseVersion(version string) (int64, error) {
	var revision int64
	_, err := fmt.Sscanf(version, "%d", &revision)
	return revision, err
}
