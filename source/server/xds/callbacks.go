package xdsserver

// callbacks.go —— ACK/NACK 追踪
//
// 推送状态按本次规则变更实际影响的 xDS 资源集合判断：
//   - NACK → failed
//   - 全部 expected 资源 ACK → deployed
//   - 超时未齐 → timeout，等下一轮 reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

var trackedTypeURLs = map[string]bool{
	"type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment": true,
	"type.googleapis.com/envoy.config.cluster.v3.Cluster":                true,
	"type.googleapis.com/envoy.config.route.v3.RouteConfiguration":       true,
	"type.googleapis.com/envoy.config.listener.v3.Listener":              true,
}

const deployTimeout = 5 * time.Second

type pendingNonce struct {
	revision int64
	typeURL  string
}

type revisionState struct {
	expected map[string]bool // 本次推送期望被 ACK 的 typeURL 雅合
	acked    map[string]bool // 已 ACK 的 typeURL
}

type PushStatusStore interface {
	MarkPushDeployed(ctx context.Context, revision int64) error
	MarkPushFailed(ctx context.Context, revision int64, errMsg string) error
	MarkPushTimeout(ctx context.Context, revision int64) error
}

type AckCallbacks struct {
	mu            sync.Mutex
	pendingNonces map[int64]map[string]pendingNonce // streamID → nonce+typeURL → pendingNonce
	revisions     map[int64]*revisionState          // revision → 追踪状态
	timers        map[int64]*time.Timer             // revision → 部署超时定时器
	store         PushStatusStore
	onDeployed    func(revision int64)
}

// NewAckCallbacks 创建 ACK/NACK 追踪回调。
func NewAckCallbacks(store PushStatusStore, onDeployed func(int64)) *AckCallbacks {
	return &AckCallbacks{
		pendingNonces: make(map[int64]map[string]pendingNonce),
		revisions:     make(map[int64]*revisionState),
		timers:        make(map[int64]*time.Timer),
		store:         store,
		onDeployed:    onDeployed,
	}
}

// TrackExpected 记录本次推送期望被 ACK 的 typeURL 集合，启动超时定时器。
func (c *AckCallbacks) TrackExpected(revision int64, typeURLs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 取消旧定时器（同一 revision 重复推送时）
	if old, ok := c.timers[revision]; ok {
		old.Stop()
	}

	expected := make(map[string]bool, len(typeURLs))
	for _, u := range typeURLs {
		if trackedTypeURLs[u] {
			expected[u] = true
		}
	}
	c.revisions[revision] = &revisionState{
		expected: expected,
		acked:    make(map[string]bool),
	}

	c.timers[revision] = time.AfterFunc(deployTimeout, func() {
		c.mu.Lock()
		state, ok := c.revisions[revision]
		if !ok {
			c.mu.Unlock()
			return
		}
		missing := c.missingTypesLocked(state)
		c.cleanupRevisionLocked(revision)
		c.mu.Unlock()

		if len(missing) > 0 {
			slog.Warn("ACK 超时未齐，标记 timeout", "revision", revision, "missing", missing)
			if c.store != nil {
				if err := c.store.MarkPushTimeout(context.Background(), revision); err != nil {
					slog.Error("标记 timeout 失败", "revision", revision, "error", err)
				}
			}
		}
	})
}

// missingTypes 返回尚未 ACK 的 typeURL 列表。
func (c *AckCallbacks) missingTypesLocked(state *revisionState) []string {
	var missing []string
	for typeURL := range state.expected {
		if !state.acked[typeURL] {
			missing = append(missing, typeURL)
		}
	}
	return missing
}

// markTypeAcked 记录 ACK，全部 expected ACK 后标记 deployed。
func (c *AckCallbacks) markTypeAcked(revision int64, typeURL string) {
	c.mu.Lock()
	state, ok := c.revisions[revision]
	if !ok {
		c.mu.Unlock()
		return
	}
	state.acked[typeURL] = true

	// 检查是否全部 expected 都已 ACK
	for u := range state.expected {
		if !state.acked[u] {
			c.mu.Unlock()
			return
		}
	}

	// 全部 ACK，清理并标记 deployed
	c.cleanupRevisionLocked(revision)
	c.mu.Unlock()

	slog.Info("全部 typeURL 已 ACK，标记 deployed", "revision", revision)
	if c.store != nil {
		if err := c.store.MarkPushDeployed(context.Background(), revision); err != nil {
			slog.Error("标记 deployed 失败", "revision", revision, "error", err)
		}
	}
	if c.onDeployed != nil {
		c.onDeployed(revision)
	}
}

// markFailed 收到 NACK 标记 failed。
func (c *AckCallbacks) markFailed(revision int64, errMsg string) {
	c.mu.Lock()
	c.cleanupRevisionLocked(revision)
	c.mu.Unlock()

	if c.store != nil {
		if err := c.store.MarkPushFailed(context.Background(), revision, errMsg); err != nil {
			slog.Error("标记 failed 失败", "revision", revision, "error", err)
		}
	}
}

// cleanupRevisionLocked 清理指定 revision 的所有追踪记录（定时器、状态、pendingNonces）。
func (c *AckCallbacks) cleanupRevisionLocked(revision int64) {
	if timer, ok := c.timers[revision]; ok {
		timer.Stop()
		delete(c.timers, revision)
	}
	delete(c.revisions, revision)
	for streamID, inner := range c.pendingNonces {
		for nonce, pending := range inner {
			if pending.revision == revision {
				delete(inner, nonce)
			}
		}
		if len(inner) == 0 {
			delete(c.pendingNonces, streamID)
		}
	}
}

// TrackNonce 记录流中待确认的 nonce 与 revision、typeURL 的映射关系。
// key 为 nonce:typeURL:revision，防止旧 snapshot 的 nonce 误匹配新 revision。
func (c *AckCallbacks) TrackNonce(streamID int64, nonce string, revision int64, typeURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		inner = make(map[string]pendingNonce)
		c.pendingNonces[streamID] = inner
	}
	key := nonce + ":" + typeURL + ":" + fmt.Sprintf("%d", revision)
	inner[key] = pendingNonce{revision: revision, typeURL: typeURL}
}

// ─── server.Callbacks 实现 ────────────────────────────────────────────────

func (c *AckCallbacks) OnStreamOpen(_ context.Context, _ int64, _ string) error {
	return nil
}

func (c *AckCallbacks) OnStreamClosed(streamID int64, _ *core.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pendingNonces, streamID)
}

func (c *AckCallbacks) OnStreamRequest(streamID int64, req *discovery.DiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	typeURL := req.GetTypeUrl()
	if nonce == "" || typeURL == "" {
		return nil
	}

	c.mu.Lock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	prefix := nonce + ":" + typeURL + ":"
	// 收集所有匹配 nonce+typeURL 的条目（可能跨多个 revision）
	type match struct {
		key      string
		revision int64
	}
	var matches []match
	for key, pending := range inner {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			matches = append(matches, match{key: key, revision: pending.revision})
		}
	}
	for _, m := range matches {
		delete(inner, m.key)
	}
	if len(inner) == 0 {
		delete(c.pendingNonces, streamID)
	}
	c.mu.Unlock()

	if !trackedTypeURLs[typeURL] {
		return nil
	}

	isNACK := req.GetErrorDetail() != nil
	for _, m := range matches {
		if isNACK {
			slog.Warn("NACK", "revision", m.revision, "type_url", typeURL, "nonce", nonce)
			c.markFailed(m.revision, req.GetErrorDetail().String())
		} else {
			slog.Info("ACK", "revision", m.revision, "type_url", typeURL, "nonce", nonce)
			c.markTypeAcked(m.revision, typeURL)
		}
	}
	return nil
}

func (c *AckCallbacks) OnStreamResponse(_ context.Context, streamID int64, _ *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
	nonce := resp.GetNonce()
	versionInfo := resp.GetVersionInfo()
	typeURL := resp.GetTypeUrl()
	if nonce == "" || versionInfo == "" {
		return
	}
	var revision int64
	if _, err := fmt.Sscanf(versionInfo, "%d", &revision); err != nil {
		return
	}
	c.TrackNonce(streamID, nonce, revision, typeURL)
}

func (c *AckCallbacks) OnDeltaStreamOpen(_ context.Context, _ int64, _ string) error {
	return nil
}

func (c *AckCallbacks) OnDeltaStreamClosed(streamID int64, _ *core.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pendingNonces, streamID)
}

func (c *AckCallbacks) OnStreamDeltaRequest(streamID int64, req *discovery.DeltaDiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	typeURL := req.GetTypeUrl()
	if nonce == "" || typeURL == "" {
		return nil
	}

	c.mu.Lock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	prefix := nonce + ":" + typeURL + ":"
	type match struct {
		key      string
		revision int64
	}
	var matches []match
	for key, pending := range inner {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			matches = append(matches, match{key: key, revision: pending.revision})
		}
	}
	for _, m := range matches {
		delete(inner, m.key)
	}
	if len(inner) == 0 {
		delete(c.pendingNonces, streamID)
	}
	c.mu.Unlock()

	if !trackedTypeURLs[typeURL] {
		return nil
	}

	isNACK := req.GetErrorDetail() != nil
	for _, m := range matches {
		if isNACK {
			slog.Warn("DELTA NACK", "revision", m.revision, "type_url", typeURL, "nonce", nonce)
			c.markFailed(m.revision, req.GetErrorDetail().String())
		} else {
			slog.Debug("DELTA ACK", "revision", m.revision, "type_url", typeURL, "nonce", nonce)
			c.markTypeAcked(m.revision, typeURL)
		}
	}
	return nil
}

func (c *AckCallbacks) OnStreamDeltaResponse(streamID int64, _ *discovery.DeltaDiscoveryRequest, resp *discovery.DeltaDiscoveryResponse) {
	nonce := resp.GetNonce()
	versionInfo := resp.GetSystemVersionInfo()
	typeURL := resp.GetTypeUrl()
	if nonce == "" || versionInfo == "" {
		return
	}
	var revision int64
	if _, err := fmt.Sscanf(versionInfo, "%d", &revision); err != nil {
		return
	}
	c.TrackNonce(streamID, nonce, revision, typeURL)
}

func (c *AckCallbacks) OnFetchRequest(_ context.Context, _ *discovery.DiscoveryRequest) error {
	return nil
}

func (c *AckCallbacks) OnFetchResponse(_ *discovery.DiscoveryRequest, _ *discovery.DiscoveryResponse) {
}

var _ server.Callbacks = (*AckCallbacks)(nil)

// MarkRevisionDeployed 直接标记 revision 为 deployed（用于空快照等无需 ACK 的场景）。
func (c *AckCallbacks) MarkRevisionDeployed(revision int64) {
	c.mu.Lock()
	c.cleanupRevisionLocked(revision)
	c.mu.Unlock()

	if c.store != nil {
		if err := c.store.MarkPushDeployed(context.Background(), revision); err != nil {
			slog.Error("标记 deployed 失败", "revision", revision, "error", err)
		}
	}
	if c.onDeployed != nil {
		c.onDeployed(revision)
	}
}
