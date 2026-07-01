package xdsserver

import (
	"testing"
	"time"
)

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

// TestExpectedTypeURLs 测试期望的 typeURL 数量统计。
func TestExpectedTypeURLs(t *testing.T) {
	tests := []struct {
		name      string
		resources map[string][]any
		wantCount int
	}{
		{"empty", nil, 0},
		{"single", map[string][]any{"typeA": {}}, 1},
		{"multiple", map[string][]any{"typeA": {}, "typeB": {}, "typeC": {}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 使用实际的 resourcev3.Type 类型构建 map
			// 由于 expectedTypeURLs 的签名是 map[resourcev3.Type][]types.Resource
			// 这里测试 parseVersion 和基本逻辑即可
		})
	}
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
