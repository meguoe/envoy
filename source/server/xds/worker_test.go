package xdsserver

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockPushStore 是用于 worker 测试的内存推送存储实现。
type mockPushStore struct {
	mu         sync.Mutex
	dbRevision int64
	rules      []*ProxyRule
	pushStatus map[int64]string
	loadErr    error
	pushCalls  []int64
	loadFn     func(ctx context.Context) ([]*ProxyRule, error)
}

// newMockPushStore 创建指定 revision 和规则的 mock 推送存储。
func newMockPushStore(rev int64, rules []*ProxyRule) *mockPushStore {
	return &mockPushStore{
		dbRevision: rev,
		rules:      rules,
		pushStatus: make(map[int64]string),
	}
}

// LoadRevision 返回当前数据库 revision。
func (s *mockPushStore) LoadRevision(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbRevision, s.loadErr
}

// Load 返回当前规则列表，支持自定义加载函数。
func (s *mockPushStore) Load(ctx context.Context) ([]*ProxyRule, error) {
	s.mu.Lock()
	if s.loadFn != nil {
		fn := s.loadFn
		s.mu.Unlock()
		return fn(ctx)
	}
	defer s.mu.Unlock()
	return s.rules, nil
}

// LogPushPending 记录一次 push pending 调用。
func (s *mockPushStore) LogPushPending(_ context.Context, rev int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushCalls = append(s.pushCalls, rev)
	return nil
}

// PushStatus 返回指定 revision 的推送状态。
func (s *mockPushStore) PushStatus(_ context.Context, rev int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pushStatus[rev], nil
}

// MarkPushFailed 记录推送失败（当前测试中为空操作）。
func (s *mockPushStore) MarkPushFailed(_ context.Context, _ int64, _ string) error { return nil }

// setRevision 更新数据库中的当前 revision。
func (s *mockPushStore) setRevision(rev int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dbRevision = rev
}

// setStatus 设置指定 revision 的推送状态。
func (s *mockPushStore) setStatus(rev int64, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushStatus[rev] = status
}

// getPushCalls 返回所有 push 调用记录的副本。
func (s *mockPushStore) getPushCalls() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, len(s.pushCalls))
	copy(out, s.pushCalls)
	return out
}

// newTestWorker 创建用于测试的推送 worker 和引擎实例。
func newTestWorker(t *testing.T, store *mockPushStore, tickerInterval time.Duration) (*RulePushWorker, *Engine) {
	t.Helper()
	engine := NewEngine("test-node", time.Second, 60*time.Second)
	worker := NewRulePushWorker(store, engine, tickerInterval)
	t.Cleanup(func() {
		worker.Stop()
	})
	return worker, engine
}

// waitForPush 轮询等待指定 revision 的 push 完成，超时则测试失败。
func waitForPush(t *testing.T, store *mockPushStore, expectedRev int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		calls := store.getPushCalls()
		if len(calls) > 0 && calls[len(calls)-1] >= expectedRev {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for push rev=%d, got calls=%v", expectedRev, store.getPushCalls())
}

// TestWorkerEventTriggersPush 测试事件通知触发 worker 执行推送。
func TestWorkerEventTriggersPush(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(1, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, _ := newTestWorker(t, store, time.Hour)
	worker.Start()

	worker.NotifyRevision(1)
	waitForPush(t, store, 1, 2*time.Second)

	if got := store.getPushCalls(); len(got) != 1 || got[0] != 1 {
		t.Errorf("push calls = %v, want [1]", got)
	}
}

// TestWorkerEventMergeSkipsIntermediate 测试连续事件合并后跳过中间 revision。
func TestWorkerEventMergeSkipsIntermediate(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(1, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, engine := newTestWorker(t, store, time.Hour)
	worker.Start()

	worker.NotifyRevision(2)
	store.setRevision(3)
	worker.NotifyRevision(3)
	store.setRevision(5)
	worker.NotifyRevision(5)

	waitForPush(t, store, 5, 2*time.Second)

	if got := engine.KnownRevision(); got != 5 {
		t.Errorf("KnownRevision = %d, want 5", got)
	}
	calls := store.getPushCalls()
	if len(calls) < 1 {
		t.Fatalf("expected at least 1 push call, got %v", calls)
	}
}

// TestWorkerRetryFailedRevision 测试 worker 重试之前失败的 revision 推送。
func TestWorkerRetryFailedRevision(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(3, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	store.setStatus(3, "failed")
	worker, engine := newTestWorker(t, store, time.Hour)
	worker.Start()

	worker.NotifyRevision(3)
	waitForPush(t, store, 3, 2*time.Second)

	if got := engine.KnownRevision(); got != 3 {
		t.Errorf("KnownRevision = %d, want 3", got)
	}
}

// TestWorkerSkipNonFailedSameRevision 测试已成功推送的相同 revision 不会重复推送。
func TestWorkerSkipNonFailedSameRevision(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(2, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, engine := newTestWorker(t, store, time.Hour)
	worker.Start()

	store.setRevision(2)
	engine.ReplaceRulesAndPushWithVersion(store.rules, 2)

	worker.NotifyRevision(2)
	time.Sleep(100 * time.Millisecond)

	if got := store.getPushCalls(); len(got) != 0 {
		t.Errorf("push calls = %v, want [] (should skip non-failed same revision)", got)
	}
}

// TestWorkerReCheckPushDuringPush 测试推送过程中检测到新 revision 后会追加推送。
func TestWorkerReCheckPushDuringPush(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(1, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, engine := newTestWorker(t, store, time.Hour)
	worker.Start()

	var loadCount atomic.Int32
	store.loadFn = func(ctx context.Context) ([]*ProxyRule, error) {
		n := loadCount.Add(1)
		if n == 1 {
			store.setRevision(2)
		}
		return store.rules, nil
	}

	worker.NotifyRevision(1)
	waitForPush(t, store, 2, 2*time.Second)

	calls := store.getPushCalls()
	if len(calls) < 2 {
		t.Errorf("expected at least 2 push calls (re-check), got %v", calls)
	}
	_ = engine
}

// TestWorkerTickerTriggersPush 测试定时器周期性触发推送。
func TestWorkerTickerTriggersPush(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(4, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, _ := newTestWorker(t, store, 50*time.Millisecond)
	worker.Start()

	waitForPush(t, store, 4, 2*time.Second)

	if got := store.getPushCalls(); len(got) < 1 {
		t.Errorf("ticker should trigger push, got calls=%v", got)
	}
}

// TestWorkerStop 测试停止 worker 后不再响应事件通知。
func TestWorkerStop(t *testing.T) {
	silenceLogs(t)
	store := newMockPushStore(1, []*ProxyRule{{
		ID: "r1", Name: "r1", Protocol: "http", ListenAddr: "0.0.0.0",
		ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}},
		LBPolicy: "ROUND_ROBIN",
	}})
	worker, _ := newTestWorker(t, store, time.Hour)
	worker.Start()
	worker.Stop()

	time.Sleep(100 * time.Millisecond)
	worker.NotifyRevision(1)
	time.Sleep(100 * time.Millisecond)

	if got := store.getPushCalls(); len(got) != 0 {
		t.Errorf("after Stop, push calls = %v, want []", got)
	}
}
