package xdsserver

import (
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func silenceLogs(t *testing.T) {
	t.Helper()
	old := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		log.SetOutput(old)
	})
}

func TestPersistFailuresAtomicity(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	var failCount atomic.Int32
	e.SetOnRulesChanged(func([]*ProxyRule) error {
		failCount.Add(1)
		return errors.New("simulated persist failure")
	})

	const goroutines = 50
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				e.notifyRulesChanged()
			}
		}()
	}
	wg.Wait()

	got := e.persistFailures.Load()
	want := uint64(goroutines * iterations)
	if got != want {
		t.Errorf("persistFailures = %d, want %d", got, want)
	}
}

func TestPersistFailuresResetOnSuccess(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	var callCount atomic.Int32

	e.SetOnRulesChanged(func([]*ProxyRule) error {
		n := callCount.Add(1)
		// Fail 3 times, then succeed
		if n <= 3 {
			return errors.New("fail")
		}
		return nil
	})

	// First 3 calls fail
	for i := 0; i < 3; i++ {
		e.notifyRulesChanged()
	}
	if got := e.persistFailures.Load(); got != 3 {
		t.Errorf("after 3 failures: persistFailures = %d, want 3", got)
	}

	// 4th call succeeds, should reset to 0
	e.notifyRulesChanged()
	if got := e.persistFailures.Load(); got != 0 {
		t.Errorf("after success: persistFailures = %d, want 0", got)
	}
}

func TestPersistFailuresConcurrentReadWrite(t *testing.T) {
	silenceLogs(t)
	e := NewEngine("test", time.Second, 60*time.Second)
	e.SetOnRulesChanged(func([]*ProxyRule) error {
		return errors.New("fail")
	})

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			e.notifyRulesChanged()
		}()
	}

	// Readers (simulating /health endpoint)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = e.PersistFailures()
			}
		}()
	}

	wg.Wait()

	// If we get here without race detector triggering, the test passes
	t.Logf("persistFailures = %d (expected %d)", e.persistFailures.Load(), goroutines)
}

func TestPersistFailuresNoCallback(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	// No callback set
	e.notifyRulesChanged()
	if got := e.persistFailures.Load(); got != 0 {
		t.Errorf("no callback: persistFailures = %d, want 0", got)
	}
}
