package xdsserver

import (
	"reflect"
	"testing"
	"time"

	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
)

type trackingCallbacks struct {
	server.CallbackFuncs
	expected map[int64][]string
	deployed []int64
}

func (c *trackingCallbacks) TrackExpected(revision int64, typeURLs []string) {
	if c.expected == nil {
		c.expected = make(map[int64][]string)
	}
	c.expected[revision] = append([]string(nil), typeURLs...)
}

func (c *trackingCallbacks) MarkRevisionDeployed(revision int64) {
	c.deployed = append(c.deployed, revision)
}

// TestParseVersion 测试版本字符串解析为 revision 数值。
func TestParseVersion(t *testing.T) {
	tests := []struct {
		input   string
		wantRev int64
		wantErr bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"42", 42, false},
		{"1234567890", 1234567890, false},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			rev, err := parseVersion(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseVersion(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if rev != tt.wantRev {
				t.Errorf("parseVersion(%q) = %d, want %d", tt.input, rev, tt.wantRev)
			}
		})
	}
}

// TestExpectedTypeURLs 测试变更资源转换为 ACK typeURL 时保持固定顺序。
func TestExpectedTypeURLs(t *testing.T) {
	changed := map[resourcev3.Type]map[string]bool{
		resourcev3.RouteType:    {"route_web": true},
		resourcev3.EndpointType: {"cluster_web": true},
		resourcev3.ListenerType: {"listener_web": true},
	}
	want := []string{
		string(resourcev3.ListenerType),
		string(resourcev3.EndpointType),
		string(resourcev3.RouteType),
	}
	if got := expectedTypeURLsFromDiff(changed); !reflect.DeepEqual(got, want) {
		t.Fatalf("expectedTypeURLsFromDiff = %v, want %v", got, want)
	}
}

// TestSnapshotDiffUsesResourceVersions 测试 Delta ACK 追踪按资源内容版本识别新增、更新、删除。
func TestSnapshotDiffUsesResourceVersions(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	httpRule := &ProxyRule{
		ID: "web-1", Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}

	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{httpRule}, 1); err != nil {
		t.Fatalf("initial push: %v", err)
	}
	added := diffSnapshots(nil, e.prevSnapshot)
	wantHTTP := []string{
		string(resourcev3.ListenerType),
		string(resourcev3.ClusterType),
		string(resourcev3.EndpointType),
		string(resourcev3.RouteType),
	}
	if got := expectedTypeURLsFromDiff(added); !reflect.DeepEqual(got, wantHTTP) {
		t.Fatalf("added HTTP expected = %v, want %v", got, wantHTTP)
	}

	prev := e.prevSnapshot
	updated := *httpRule
	updated.Backends = []BackendNode{{Address: "127.0.0.1", Port: 9090, Weight: 1}}
	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{&updated}, 2); err != nil {
		t.Fatalf("backend update push: %v", err)
	}
	changed := diffSnapshots(prev, e.prevSnapshot)
	wantEDSOnly := []string{string(resourcev3.EndpointType)}
	if got := expectedTypeURLsFromDiff(changed); !reflect.DeepEqual(got, wantEDSOnly) {
		t.Fatalf("backend update expected = %v, want %v", got, wantEDSOnly)
	}

	prev = e.prevSnapshot
	if err := e.ReplaceRulesAndPushWithVersion(nil, 3); err != nil {
		t.Fatalf("delete push: %v", err)
	}
	deleted := diffSnapshots(prev, e.prevSnapshot)
	if got := expectedTypeURLsFromDiff(deleted); !reflect.DeepEqual(got, wantHTTP) {
		t.Fatalf("delete HTTP expected = %v, want %v", got, wantHTTP)
	}
}

// TestSnapshotDiffUDPBackendUpdateUsesCDS 测试 UDP 后端变化只影响内嵌 LoadAssignment 的 CDS。
func TestSnapshotDiffUDPBackendUpdateUsesCDS(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	udpRule := &ProxyRule{
		ID: "dns-1", Name: "dns", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 5353,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 53, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}

	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{udpRule}, 1); err != nil {
		t.Fatalf("initial UDP push: %v", err)
	}

	prev := e.prevSnapshot
	updated := *udpRule
	updated.Backends = []BackendNode{{Address: "127.0.0.1", Port: 5354, Weight: 1}}
	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{&updated}, 2); err != nil {
		t.Fatalf("UDP backend update push: %v", err)
	}

	changed := diffSnapshots(prev, e.prevSnapshot)
	want := []string{string(resourcev3.ClusterType)}
	if got := expectedTypeURLsFromDiff(changed); !reflect.DeepEqual(got, want) {
		t.Fatalf("UDP backend update expected = %v, want %v", got, want)
	}
}

// TestDeleteLastRuleTracksRemovedResources 测试删除最后一条规则仍等待 removed_resources ACK。
func TestDeleteLastRuleTracksRemovedResources(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	cb := &trackingCallbacks{}
	e.SetCallbacks(cb)
	rule := &ProxyRule{
		ID: "web-1", Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}

	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{rule}, 1); err != nil {
		t.Fatalf("initial push: %v", err)
	}
	if err := e.ReplaceRulesAndPushWithVersion(nil, 2); err != nil {
		t.Fatalf("delete push: %v", err)
	}

	want := []string{
		string(resourcev3.ListenerType),
		string(resourcev3.ClusterType),
		string(resourcev3.EndpointType),
		string(resourcev3.RouteType),
	}
	if got := cb.expected[2]; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete expected = %v, want %v", got, want)
	}
	for _, rev := range cb.deployed {
		if rev == 2 {
			t.Fatalf("delete revision was marked deployed without ACK")
		}
	}
}

// TestNoResourceChangeMarksDeployed 测试 revision 变化但资源内容不变时无需等待 ACK。
func TestNoResourceChangeMarksDeployed(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	cb := &trackingCallbacks{}
	e.SetCallbacks(cb)
	rule := &ProxyRule{
		ID: "web-1", Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy: "ROUND_ROBIN",
	}

	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{rule}, 1); err != nil {
		t.Fatalf("initial push: %v", err)
	}
	same := *rule
	same.Backends = []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}
	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{&same}, 2); err != nil {
		t.Fatalf("same content push: %v", err)
	}

	if got := cb.expected[2]; len(got) != 0 {
		t.Fatalf("same content expected = %v, want none", got)
	}
	if !containsRevision(cb.deployed, 2) {
		t.Fatalf("same content revision was not marked deployed: %v", cb.deployed)
	}
}

func containsRevision(revisions []int64, want int64) bool {
	for _, rev := range revisions {
		if rev == want {
			return true
		}
	}
	return false
}

// TestPushSnapshotLockedEmptyRules 测试空规则列表推送快照不报错。
func TestPushSnapshotLockedEmptyRules(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	e.pushMu.Lock()
	err := e.pushSnapshotLocked()
	e.pushMu.Unlock()

	if err != nil {
		t.Fatalf("pushSnapshotLocked with empty rules: %v", err)
	}
}

// TestPushSnapshotLockedWithHTTPRules 测试 HTTP 协议规则推送快照成功。
func TestPushSnapshotLockedWithHTTPRules(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	e.SetRules([]*ProxyRule{
		{
			Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
			LBPolicy: "ROUND_ROBIN",
		},
	})

	e.pushMu.Lock()
	err := e.pushSnapshotLocked()
	e.pushMu.Unlock()

	if err != nil {
		t.Fatalf("pushSnapshotLocked with HTTP rule: %v", err)
	}
}

// TestPushSnapshotLockedWithUDPRules 测试 UDP 协议规则推送快照成功。
func TestPushSnapshotLockedWithUDPRules(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	e.SetRules([]*ProxyRule{
		{
			Name: "dns", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 5353,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 53}},
			LBPolicy: "ROUND_ROBIN",
		},
	})

	e.pushMu.Lock()
	err := e.pushSnapshotLocked()
	e.pushMu.Unlock()

	if err != nil {
		t.Fatalf("pushSnapshotLocked with UDP rule: %v", err)
	}
}

// TestPushSnapshotLockedWithMixedRules 测试混合协议（HTTP+UDP）规则推送快照成功。
func TestPushSnapshotLockedWithMixedRules(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	e.SetRules([]*ProxyRule{
		{
			Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
			LBPolicy: "ROUND_ROBIN",
		},
		{
			Name: "dns", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 5353,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 53}},
			LBPolicy: "RANDOM",
		},
	})

	e.pushMu.Lock()
	err := e.pushSnapshotLocked()
	e.pushMu.Unlock()

	if err != nil {
		t.Fatalf("pushSnapshotLocked with mixed rules: %v", err)
	}
}

// TestPushSnapshotLockedWithVersion 测试带版本号推送快照后 revision 正确更新。
func TestPushSnapshotLockedWithVersion(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	rules := []*ProxyRule{
		{
			Name: "web", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
			LBPolicy: "ROUND_ROBIN",
		},
	}

	if err := e.ReplaceRulesAndPushWithVersion(rules, 100); err != nil {
		t.Fatalf("ReplaceRulesAndPushWithVersion: %v", err)
	}
	if got := e.KnownRevision(); got != 100 {
		t.Errorf("KnownRevision = %d, want 100", got)
	}
}

// TestPushSnapshotEmptyMarksDeployed 测试推送空快照时自动标记为已部署。
func TestPushSnapshotEmptyMarksDeployed(t *testing.T) {
	silenceLogs(t)
	store := &fakePushStore{}
	e := NewEngine("test", time.Second, 60*time.Second)
	ackCb := NewAckCallbacks(store, func(rev int64) {
		e.SetDeployedRevision(rev)
	})
	e.SetCallbacks(ackCb)

	// 空快照直接标记 deployed
	e.SetRules([]*ProxyRule{})
	if err := e.ReplaceRulesAndPushWithVersion(nil, 1); err != nil {
		t.Fatalf("ReplaceRulesAndPushWithVersion: %v", err)
	}

	deployed := store.getDeployed()
	if len(deployed) != 1 || deployed[0] != 1 {
		t.Errorf("MarkPushDeployed called with %v, want [1]", deployed)
	}
}
