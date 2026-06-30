package xdsserver

import (
	"context"
	"testing"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

type ackStore struct {
	deployed []int64
	failed   []int64
}

func (s *ackStore) MarkPushDeployed(_ context.Context, revision int64) error {
	s.deployed = append(s.deployed, revision)
	return nil
}

func (s *ackStore) MarkPushFailed(_ context.Context, revision int64, _ string) error {
	s.failed = append(s.failed, revision)
	return nil
}

func TestAckCallbacksKeyNonceByStream(t *testing.T) {
	const typeURL = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	store := &ackStore{}
	cb := NewAckCallbacks(store, nil)
	cb.TrackExpected(1, []string{typeURL})
	cb.TrackExpected(2, []string{typeURL})

	cb.OnStreamResponse(context.Background(), 10, nil, &discovery.DiscoveryResponse{
		VersionInfo: "1",
		TypeUrl:     typeURL,
		Nonce:       "1",
	})
	cb.OnStreamResponse(context.Background(), 20, nil, &discovery.DiscoveryResponse{
		VersionInfo: "2",
		TypeUrl:     typeURL,
		Nonce:       "1",
	})

	if err := cb.OnStreamRequest(10, &discovery.DiscoveryRequest{ResponseNonce: "1"}); err != nil {
		t.Fatalf("stream 10 ACK: %v", err)
	}
	if err := cb.OnStreamRequest(20, &discovery.DiscoveryRequest{ResponseNonce: "1"}); err != nil {
		t.Fatalf("stream 20 ACK: %v", err)
	}

	if len(store.deployed) != 2 || store.deployed[0] != 1 || store.deployed[1] != 2 {
		t.Fatalf("deployed revisions = %v, want [1 2]", store.deployed)
	}
}
