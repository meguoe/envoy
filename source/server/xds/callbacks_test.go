package xdsserver

import (
	"context"
	"testing"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	spb "google.golang.org/genproto/googleapis/rpc/status"
)

type ackStore struct {
	deployed []int64
	failed   []int64
	timeout  []int64
}

func (s *ackStore) MarkPushDeployed(_ context.Context, revision int64) error {
	s.deployed = append(s.deployed, revision)
	return nil
}

func (s *ackStore) MarkPushFailed(_ context.Context, revision int64, _ string) error {
	s.failed = append(s.failed, revision)
	return nil
}

func (s *ackStore) MarkPushTimeout(_ context.Context, revision int64) error {
	s.timeout = append(s.timeout, revision)
	return nil
}

func sendResponse(cb *AckCallbacks, streamID int64, revision int64, typeURL, nonce string) {
	cb.OnStreamResponse(context.Background(), streamID, nil, &discovery.DiscoveryResponse{
		VersionInfo: "1", TypeUrl: typeURL, Nonce: nonce,
	})
}

func sendACK(cb *AckCallbacks, streamID int64, typeURL, nonce string) {
	cb.OnStreamRequest(streamID, &discovery.DiscoveryRequest{
		ResponseNonce: nonce, TypeUrl: typeURL,
	})
}

func sendNACK(cb *AckCallbacks, streamID int64, typeURL, nonce string) {
	cb.OnStreamRequest(streamID, &discovery.DiscoveryRequest{
		ResponseNonce: nonce, TypeUrl: typeURL,
		ErrorDetail: &spb.Status{Message: "test error"},
	})
}

// 全部 expected ACK → deployed
func TestAllExpectedAcked(t *testing.T) {
	cds := "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	lds := "type.googleapis.com/envoy.config.listener.v3.Listener"
	store := &ackStore{}
	cb := NewAckCallbacks(store, nil)

	cb.TrackExpected(1, []string{lds, cds})

	sendResponse(cb, 10, 1, cds, "n1")
	sendResponse(cb, 10, 1, lds, "n2")
	sendACK(cb, 10, cds, "n1")
	sendACK(cb, 10, lds, "n2")

	if len(store.deployed) != 1 || store.deployed[0] != 1 {
		t.Fatalf("deployed = %v, want [1]", store.deployed)
	}
}

// NACK → failed，即使部分 ACK 已到
func TestNackOverridesAck(t *testing.T) {
	cds := "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	lds := "type.googleapis.com/envoy.config.listener.v3.Listener"
	store := &ackStore{}
	cb := NewAckCallbacks(store, nil)

	cb.TrackExpected(1, []string{lds, cds})

	sendResponse(cb, 10, 1, cds, "n1")
	sendResponse(cb, 10, 1, lds, "n2")
	sendACK(cb, 10, cds, "n1")  // CDS ACKed
	sendNACK(cb, 10, lds, "n2") // LDS NACKed

	if len(store.deployed) != 0 {
		t.Fatalf("deployed = %v, want []", store.deployed)
	}
	if len(store.failed) != 1 || store.failed[0] != 1 {
		t.Fatalf("failed = %v, want [1]", store.failed)
	}
}

// 超时未齐 → timeout
func TestTimeout(t *testing.T) {
	cds := "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	lds := "type.googleapis.com/envoy.config.listener.v3.Listener"
	store := &ackStore{}
	cb := NewAckCallbacks(store, nil)

	cb.TrackExpected(1, []string{lds, cds})

	sendResponse(cb, 10, 1, cds, "n1")
	sendACK(cb, 10, cds, "n1") // 只 ACK 了 CDS，LDS 没到

	time.Sleep(deployTimeout + 100*time.Millisecond)

	if len(store.deployed) != 0 {
		t.Fatalf("deployed = %v, want []", store.deployed)
	}
	if len(store.timeout) != 1 || store.timeout[0] != 1 {
		t.Fatalf("timeout = %v, want [1]", store.timeout)
	}
}

// 多 stream 各自 ACK 同一 revision
func TestMultipleStreams(t *testing.T) {
	cds := "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	store := &ackStore{}
	cb := NewAckCallbacks(store, nil)

	cb.TrackExpected(1, []string{cds})

	sendResponse(cb, 10, 1, cds, "n1")
	sendResponse(cb, 20, 1, cds, "n2")
	sendACK(cb, 10, cds, "n1")
	sendACK(cb, 20, cds, "n2")

	if len(store.deployed) != 1 || store.deployed[0] != 1 {
		t.Fatalf("deployed = %v, want [1]", store.deployed)
	}
}
