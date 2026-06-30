package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	xdsserver "envoy-control-plane/internal/server/xds"
)

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")

	rules := []*xdsserver.ProxyRule{
		{ID: "bbb", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "aaa", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8081, Weight: 1}}, LBPolicy: "LEAST_REQUEST"},
	}

	if err := Save(path, rules); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("Load returned %d rules, want 2", len(loaded))
	}
	// Should be sorted by ID
	if loaded[0].ID != "aaa" || loaded[1].ID != "bbb" {
		t.Errorf("rules not sorted: ids = [%s, %s], want [aaa, bbb]", loaded[0].ID, loaded[1].ID)
	}
}

func TestLoadNonExistentFile(t *testing.T) {
	rules, err := Load("/tmp/nonexistent-rules-test-12345.json")
	if err != nil {
		t.Fatalf("Load non-existent: unexpected error: %v", err)
	}
	if rules != nil {
		t.Errorf("Load non-existent: got %d rules, want nil", len(rules))
	}
}

func TestLoadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	os.WriteFile(path, []byte{}, 0644)

	rules, err := Load(path)
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if rules != nil {
		t.Errorf("Load empty: got %d rules, want nil", len(rules))
	}
}

func TestLoadInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	os.WriteFile(path, []byte(`{bad json}`), 0644)

	_, err := Load(path)
	if err == nil {
		t.Error("Load invalid JSON: expected error")
	}
}

func TestLoadSkipsInvalidRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.json")

	// valid + invalid (empty name) + valid
	data := []xdsserver.ProxyRule{
		{ID: "aaa", Name: "valid1", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}},
		{ID: "bbb", Name: "", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}},
		{ID: "ccc", Name: "valid2", ListenAddr: "0.0.0.0", ListenPort: 9983,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}},
	}
	raw, _ := json.Marshal(data)
	os.WriteFile(path, raw, 0644)

	rules, err := Load(path)
	if err != nil {
		t.Fatalf("Load mixed: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("Load mixed: got %d rules, want 2 (invalid should be skipped)", len(rules))
	}
}

func TestLoadSkipsEmptyAndDuplicateIDs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ids.json")

	data := []xdsserver.ProxyRule{
		{ID: "", Name: "empty-id", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080}}},
		{ID: "dup", Name: "first", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080}}},
		{ID: "dup", Name: "second", ListenAddr: "0.0.0.0", ListenPort: 9983,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080}}},
	}
	raw, _ := json.Marshal(data)
	os.WriteFile(path, raw, 0644)

	rules, err := Load(path)
	if err != nil {
		t.Fatalf("Load duplicate IDs: %v", err)
	}
	if len(rules) != 1 || rules[0].Name != "first" {
		t.Fatalf("Load duplicate IDs got %#v, want only first valid duplicate", rules)
	}
}

func TestSaveEmptyList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty-list.json")

	if err := Save(path, nil); err != nil {
		t.Fatalf("Save nil: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load after Save nil: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("Load after Save nil: got %d rules, want 0", len(loaded))
	}
}

func TestAtomicWriteCreatesDirs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "dir", "rules.json")

	if err := Save(path, nil); err != nil {
		t.Fatalf("Save to nested path: %v", err)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Save should create intermediate directories")
	}
}
