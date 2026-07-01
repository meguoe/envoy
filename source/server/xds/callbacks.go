package xdsserver

// callbacks.go —— ACK/NACK 追踪
//
// pendingNonces 使用嵌套 map (streamID → nonce → pendingNonce)，
// 避免 OnStreamClosed 中的全量前缀遍历。

import (
	"context"
	"fmt"
	"log"
	"sync"

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

type pendingNonce struct {
	revision int64
	typeURL  string
}

type PushStatusStore interface {
	MarkPushDeployed(ctx context.Context, revision int64) error
	MarkPushFailed(ctx context.Context, revision int64, errMsg string) error
}

type AckCallbacks struct {
	mu            sync.Mutex
	pendingNonces map[int64]map[string]pendingNonce // streamID → nonce → pendingNonce
	expectedByRev map[int64]map[string]bool
	ackedByRev    map[int64]map[string]bool
	store         PushStatusStore
	onDeployed    func(revision int64)
}

// NewAckCallbacks 创建 ACK/NACK 追踪回调，store 用于记录推送状态，onDeployed 在全部 ACK 后触发。
func NewAckCallbacks(store PushStatusStore, onDeployed func(int64)) *AckCallbacks {
	return &AckCallbacks{
		pendingNonces: make(map[int64]map[string]pendingNonce),
		expectedByRev: make(map[int64]map[string]bool),
		ackedByRev:    make(map[int64]map[string]bool),
		store:         store,
		onDeployed:    onDeployed,
	}
}

// TrackExpected 记录指定 revision 期望被 ACK 的 typeURL 列表。
func (c *AckCallbacks) TrackExpected(revision int64, typeURLs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expected := make(map[string]bool, len(typeURLs))
	for _, typeURL := range typeURLs {
		if trackedTypeURLs[typeURL] {
			expected[typeURL] = true
		}
	}
	c.expectedByRev[revision] = expected
	c.ackedByRev[revision] = make(map[string]bool, len(expected))
}

// TrackNonce 记录流中待确认的 nonce 与 revision、typeURL 的映射关系。
func (c *AckCallbacks) TrackNonce(streamID int64, nonce string, revision int64, typeURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		inner = make(map[string]pendingNonce)
		c.pendingNonces[streamID] = inner
	}
	inner[nonce] = pendingNonce{revision: revision, typeURL: typeURL}
}

// OnStreamOpen 处理 gRPC 流打开事件，当前无特殊处理。
func (c *AckCallbacks) OnStreamOpen(_ context.Context, _ int64, _ string) error {
	return nil
}

// OnStreamClosed 清理已关闭流的 pendingNonce 记录。
func (c *AckCallbacks) OnStreamClosed(streamID int64, _ *core.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pendingNonces, streamID)
}

// OnStreamRequest 处理 Envoy 的 DiscoveryRequest，根据 nonce 匹配并记录 ACK 或 NACK。
func (c *AckCallbacks) OnStreamRequest(streamID int64, req *discovery.DiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	if nonce == "" {
		return nil
	}

	c.mu.Lock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	pending, ok := inner[nonce]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(inner, nonce)
	if len(inner) == 0 {
		delete(c.pendingNonces, streamID)
	}
	revision := pending.revision
	typeURL := pending.typeURL
	c.mu.Unlock()

	if !trackedTypeURLs[typeURL] {
		return nil
	}

	if req.GetErrorDetail() == nil {
		log.Printf("ACK  rev=%d  type=%s", revision, typeURL)
		c.markTypeAcked(revision, typeURL)
	} else {
		errMsg := req.GetErrorDetail().String()
		log.Printf("NACK rev=%d  type=%s  error=%s", revision, typeURL, errMsg)
		c.markFailed(revision, errMsg)
	}

	return nil
}

// OnStreamResponse 在发送 DiscoveryResponse 时记录 nonce 与 revision 的映射。
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

// OnDeltaStreamOpen 处理 Delta xDS 流打开事件，当前无特殊处理。
func (c *AckCallbacks) OnDeltaStreamOpen(_ context.Context, _ int64, _ string) error {
	return nil
}

// OnDeltaStreamClosed 清理已关闭 Delta 流的 pendingNonce 记录。
func (c *AckCallbacks) OnDeltaStreamClosed(streamID int64, _ *core.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pendingNonces, streamID)
}

// OnStreamDeltaRequest 处理 Delta 模式的 DiscoveryRequest，记录 ACK 或 NACK。
func (c *AckCallbacks) OnStreamDeltaRequest(streamID int64, req *discovery.DeltaDiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	if nonce == "" {
		return nil
	}

	c.mu.Lock()
	inner, ok := c.pendingNonces[streamID]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	pending, ok := inner[nonce]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(inner, nonce)
	if len(inner) == 0 {
		delete(c.pendingNonces, streamID)
	}
	revision := pending.revision
	typeURL := pending.typeURL
	c.mu.Unlock()

	if !trackedTypeURLs[typeURL] {
		return nil
	}

	if req.GetErrorDetail() == nil {
		log.Printf("DELTA ACK  rev=%d  type=%s", revision, typeURL)
		c.markTypeAcked(revision, typeURL)
	} else {
		errMsg := req.GetErrorDetail().String()
		log.Printf("DELTA NACK rev=%d  type=%s  error=%s", revision, typeURL, errMsg)
		c.markFailed(revision, errMsg)
	}

	return nil
}

// OnStreamDeltaResponse 在发送 Delta DiscoveryResponse 时记录 nonce 映射。
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

// OnFetchRequest 处理主动拉取请求，当前无特殊处理。
func (c *AckCallbacks) OnFetchRequest(_ context.Context, _ *discovery.DiscoveryRequest) error {
	return nil
}

// OnFetchResponse 处理主动拉取响应，当前无特殊处理。
func (c *AckCallbacks) OnFetchResponse(_ *discovery.DiscoveryRequest, _ *discovery.DiscoveryResponse) {
}

var _ server.Callbacks = (*AckCallbacks)(nil)

// markTypeAcked 记录指定 revision 的 typeURL 已被 ACK，全部 ACK 后标记 deployed。
func (c *AckCallbacks) markTypeAcked(revision int64, typeURL string) {
	c.mu.Lock()
	if _, ok := c.ackedByRev[revision]; !ok {
		c.ackedByRev[revision] = make(map[string]bool)
	}
	c.ackedByRev[revision][typeURL] = true
	allAcked := c.allExpectedAckedLocked(revision)
	if allAcked {
		c.finishRevisionLocked(revision)
	}
	c.mu.Unlock()

	if !allAcked {
		return
	}
	log.Printf("revision %d 全部 typeURL 已 ACK，标记 deployed", revision)
	if c.store != nil {
		if err := c.store.MarkPushDeployed(context.Background(), revision); err != nil {
			log.Printf("标记 deployed 失败: %v", err)
		}
	}
	if c.onDeployed != nil {
		c.onDeployed(revision)
	}
}

// allExpectedAckedLocked 检查指定 revision 的所有期望 typeURL 是否已全部 ACK。
func (c *AckCallbacks) allExpectedAckedLocked(revision int64) bool {
	expected := c.expectedByRev[revision]
	if len(expected) == 0 {
		return false
	}
	acked := c.ackedByRev[revision]
	for typeURL := range expected {
		if !acked[typeURL] {
			return false
		}
	}
	return true
}

// markFailed 标记指定 revision 推送失败，清理相关追踪记录。
func (c *AckCallbacks) markFailed(revision int64, errMsg string) {
	c.mu.Lock()
	c.finishRevisionLocked(revision)
	c.mu.Unlock()

	if c.store != nil {
		if err := c.store.MarkPushFailed(context.Background(), revision, errMsg); err != nil {
			log.Printf("标记 failed 失败: %v", err)
		}
	}
}

// finishRevisionLocked 清理指定 revision 的所有追踪记录（expected、acked、pending）。
func (c *AckCallbacks) finishRevisionLocked(revision int64) {
	delete(c.expectedByRev, revision)
	delete(c.ackedByRev, revision)
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

// MarkRevisionDeployed 直接标记 revision 为 deployed（用于空快照等无需 ACK 的场景）
func (c *AckCallbacks) MarkRevisionDeployed(revision int64) {
	c.mu.Lock()
	c.finishRevisionLocked(revision)
	c.mu.Unlock()

	if c.store != nil {
		if err := c.store.MarkPushDeployed(context.Background(), revision); err != nil {
			log.Printf("标记 deployed 失败: %v", err)
		}
	}
	if c.onDeployed != nil {
		c.onDeployed(revision)
	}
}
