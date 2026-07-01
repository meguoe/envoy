package xdsserver

import (
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

// silenceLogs 在测试期间将日志输出重定向到 io.Discard，测试结束后恢复。
func silenceLogs(t *testing.T) {
	t.Helper()
	old := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		log.SetOutput(old)
	})
}

// TestCheckRulesConflictsNoConflict 测试无冲突规则列表不会报错。
func TestCheckRulesConflictsNoConflict(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCheckRulesConflictsDuplicateID 测试重复 ID 的规则会被检测为冲突。
func TestCheckRulesConflictsDuplicateID(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "a", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for duplicate ID")
	}
}

// TestCheckRulesConflictsDuplicateName 测试重复名称的规则会被检测为冲突。
func TestCheckRulesConflictsDuplicateName(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "same", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "same", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for duplicate name")
	}
}

// TestCheckRulesConflictsPortConflict 测试监听相同端口的规则会被检测为冲突。
func TestCheckRulesConflictsPortConflict(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for port conflict")
	}
}

// TestCheckRulesConflictsEmptyList 测试空规则列表不会报错。
func TestCheckRulesConflictsEmptyList(t *testing.T) {
	if err := CheckRulesConflicts(nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestReplaceRulesAndPushReplacesCurrentRules 测试替换规则后当前规则被完全替换。
func TestReplaceRulesAndPushReplacesCurrentRules(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	e.SetRules([]*ProxyRule{{
		ID:         "old",
		Name:       "old",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy:   "ROUND_ROBIN",
	}})

	if err := e.ReplaceRulesAndPush([]*ProxyRule{{
		ID:         "new",
		Name:       "new",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9982,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8081}},
		LBPolicy:   "ROUND_ROBIN",
	}}); err != nil {
		t.Fatalf("ReplaceRulesAndPush: %v", err)
	}

	rules := e.ListRules()
	if len(rules) != 1 || rules[0].ID != "new" {
		t.Fatalf("rules = %+v, want only new rule", rules)
	}
}

// TestReplaceRulesAndPushWithVersionSetsRevision 测试带版本号替换规则后 revision 正确设置。
func TestReplaceRulesAndPushWithVersionSetsRevision(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)

	if err := e.ReplaceRulesAndPushWithVersion([]*ProxyRule{{
		ID:         "r1",
		Name:       "r1",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy:   "ROUND_ROBIN",
	}}, 42); err != nil {
		t.Fatalf("ReplaceRulesAndPushWithVersion: %v", err)
	}

	if got := e.KnownRevision(); got != 42 {
		t.Errorf("KnownRevision = %d, want 42", got)
	}
}

// TestKnownRevisionDefaultZero 测试新引擎的默认 revision 为零。
func TestKnownRevisionDefaultZero(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	if got := e.KnownRevision(); got != 0 {
		t.Errorf("KnownRevision = %d, want 0", got)
	}
}

// TestSetDeployedRevision 测试设置已部署 revision 后能正确读取。
func TestSetDeployedRevision(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.SetDeployedRevision(5)
	if got := e.LastDeployedRevision(); got != 5 {
		t.Errorf("LastDeployedRevision = %d, want 5", got)
	}
}

// fakePushStore 是用于单元测试的内存推送状态存储实现。
type fakePushStore struct {
	mu        sync.Mutex
	deployed  []int64
	failed    []int64
}

// MarkPushDeployed 记录已部署的 revision 到内存列表。
func (s *fakePushStore) MarkPushDeployed(_ context.Context, revision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deployed = append(s.deployed, revision)
	return nil
}

// MarkPushFailed 记录失败的 revision 到内存列表。
func (s *fakePushStore) MarkPushFailed(_ context.Context, revision int64, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = append(s.failed, revision)
	return nil
}

// getDeployed 返回所有已部署的 revision 列表的副本。
func (s *fakePushStore) getDeployed() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.deployed))
	copy(out, s.deployed)
	return out
}

// TestEmptySnapshotMarksDeployed 测试推送空快照时会正确标记为已部署。
func TestEmptySnapshotMarksDeployed(t *testing.T) {
	silenceLogs(t)

	store := &fakePushStore{}
	e := NewEngine("test", time.Second, 60*time.Second)
	ackCb := NewAckCallbacks(store, func(rev int64) {
		e.SetDeployedRevision(rev)
	})
	e.SetCallbacks(ackCb)

	// 先加载一条规则，再用空列表替换，触发空 snapshot
	e.SetRules([]*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http",
		ListenAddr: "0.0.0.0", ListenPort: 9981,
		Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})

	if err := e.ReplaceRulesAndPushWithVersion(nil, 99); err != nil {
		t.Fatalf("ReplaceRulesAndPushWithVersion(nil): %v", err)
	}

	deployed := store.getDeployed()
	if len(deployed) != 1 || deployed[0] != 99 {
		t.Errorf("MarkPushDeployed called with %v, want [99]", deployed)
	}
	if got := e.LastDeployedRevision(); got != 99 {
		t.Errorf("LastDeployedRevision = %d, want 99", got)
	}
}
