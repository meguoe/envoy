package xdsserver

import (
	"io"
	"log"
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

func TestCheckRulesConflictsNoConflict(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckRulesConflictsDuplicateID(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "a", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for duplicate ID")
	}
}

func TestCheckRulesConflictsDuplicateName(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "same", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "same", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for duplicate name")
	}
}

func TestCheckRulesConflictsPortConflict(t *testing.T) {
	rules := []*ProxyRule{
		{ID: "a", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "b", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8081}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := CheckRulesConflicts(rules); err == nil {
		t.Error("expected error for port conflict")
	}
}

func TestCheckRulesConflictsEmptyList(t *testing.T) {
	if err := CheckRulesConflicts(nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

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

func TestKnownRevisionDefaultZero(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	if got := e.KnownRevision(); got != 0 {
		t.Errorf("KnownRevision = %d, want 0", got)
	}
}

func TestSetDeployedRevision(t *testing.T) {
	e := NewEngine("test", time.Second, 60*time.Second)
	e.SetDeployedRevision(5)
	if got := e.LastDeployedRevision(); got != 5 {
		t.Errorf("LastDeployedRevision = %d, want 5", got)
	}
}
