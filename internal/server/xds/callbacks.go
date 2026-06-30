package xdsserver

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
	pendingNonces map[string]pendingNonce
	expectedByRev map[int64]map[string]bool
	ackedByRev    map[int64]map[string]bool
	store         PushStatusStore
	onDeployed    func(revision int64)
}

func NewAckCallbacks(store PushStatusStore, onDeployed func(int64)) *AckCallbacks {
	return &AckCallbacks{
		pendingNonces: make(map[string]pendingNonce),
		expectedByRev: make(map[int64]map[string]bool),
		ackedByRev:    make(map[int64]map[string]bool),
		store:         store,
		onDeployed:    onDeployed,
	}
}

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

func (c *AckCallbacks) TrackNonce(streamID int64, nonce string, revision int64, typeURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingNonces[nonceKey(streamID, nonce)] = pendingNonce{revision: revision, typeURL: typeURL}
}

func (c *AckCallbacks) OnStreamOpen(_ context.Context, _ int64, _ string) error {
	return nil
}

func (c *AckCallbacks) OnStreamClosed(_ int64, _ *core.Node) {}

func (c *AckCallbacks) OnStreamRequest(streamID int64, req *discovery.DiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	if nonce == "" {
		return nil
	}

	c.mu.Lock()
	pending, ok := c.pendingNonces[nonceKey(streamID, nonce)]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.pendingNonces, nonceKey(streamID, nonce))
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

func (c *AckCallbacks) OnDeltaStreamClosed(_ int64, _ *core.Node) {}

func (c *AckCallbacks) OnStreamDeltaRequest(streamID int64, req *discovery.DeltaDiscoveryRequest) error {
	nonce := req.GetResponseNonce()
	if nonce == "" {
		return nil
	}

	c.mu.Lock()
	pending, ok := c.pendingNonces[nonceKey(streamID, nonce)]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	delete(c.pendingNonces, nonceKey(streamID, nonce))
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

func nonceKey(streamID int64, nonce string) string {
	return fmt.Sprintf("%d:%s", streamID, nonce)
}

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

func (c *AckCallbacks) finishRevisionLocked(revision int64) {
	delete(c.expectedByRev, revision)
	delete(c.ackedByRev, revision)
	for key, pending := range c.pendingNonces {
		if pending.revision == revision {
			delete(c.pendingNonces, key)
		}
	}
}
