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
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
)

var orderedSnapshotTypes = []resourcev3.Type{
	resourcev3.ListenerType,
	resourcev3.ClusterType,
	resourcev3.EndpointType,
	resourcev3.RouteType,
}

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
	if err := e.snapCache.SetSnapshot(e.ctx, e.nodeID, snap); err != nil {
		return fmt.Errorf("设置快照失败: %w", err)
	}

	// 与上一次快照 diff，计算本次变更的资源集合。
	currSnapshot := snapshotResourceVersions(snap)
	changedResources := diffSnapshots(e.prevSnapshot, currSnapshot)
	e.prevSnapshot = currSnapshot

	// 从变更资源中提取期望 ACK 的 typeURL 集合
	expectedURLs := expectedTypeURLsFromDiff(changedResources)

	if len(expectedURLs) == 0 {
		slog.Info("快照无资源变更，直接标记 deployed", "version", version)
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

// snapshotResourceVersions 提取 Delta xDS 实际使用的资源版本图。
func snapshotResourceVersions(snap *cache.Snapshot) map[resourcev3.Type]map[string]string {
	versions := make(map[resourcev3.Type]map[string]string)
	for _, typeURL := range orderedSnapshotTypes {
		versionMap := snap.GetVersionMap(string(typeURL))
		if len(versionMap) == 0 {
			continue
		}
		cp := make(map[string]string, len(versionMap))
		for name, version := range versionMap {
			cp[name] = version
		}
		versions[typeURL] = cp
	}
	return versions
}

// diffSnapshots 对比新旧快照，返回变更的资源名集合（新增 + 更新 + 删除）。
func diffSnapshots(prev, curr map[resourcev3.Type]map[string]string) map[resourcev3.Type]map[string]bool {
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
			oldSet = make(map[string]string)
		}
		if newSet == nil {
			newSet = make(map[string]string)
		}
		diff := make(map[string]bool)
		// 新增或内容更新的资源
		for name, version := range newSet {
			if oldSet[name] != version {
				diff[name] = true
			}
		}
		// 删除的资源
		for name := range oldSet {
			if _, ok := newSet[name]; !ok {
				diff[name] = true
			}
		}
		if len(diff) > 0 {
			changed[typeURL] = diff
		}
	}
	return changed
}

// expectedTypeURLsFromDiff 从变更资源集合中提取期望 ACK 的 typeURL 集合。
func expectedTypeURLsFromDiff(changed map[resourcev3.Type]map[string]bool) []string {
	var typeURLs []string
	for _, typeURL := range orderedSnapshotTypes {
		if changed[typeURL] != nil {
			typeURLs = append(typeURLs, string(typeURL))
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
