package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	xdsserver "envoy-control-plane/internal/server/xds"
)

func testDSN(dbname string) string {
	host := os.Getenv("PG_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("PG_PORT")
	if port == "" {
		port = "7032"
	}
	user := os.Getenv("PG_USER")
	if user == "" {
		user = "hiddos"
	}
	pass := os.Getenv("PG_PASSWORD")
	if pass == "" {
		pass = "mxb123=-"
	}
	return BuildPgDSN(host, port, user, pass, dbname)
}

func TestBuildPgDSN(t *testing.T) {
	dsn := BuildPgDSN("localhost", "5432", "user", "pass", "mydb")
	want := "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
	if dsn != want {
		t.Errorf("BuildPgDSN = %q, want %q", dsn, want)
	}

	dsn = BuildPgDSN("10.0.0.1", "7032", "admin", "", "testdb")
	want = "postgres://admin@10.0.0.1:7032/testdb?sslmode=disable"
	if dsn != want {
		t.Errorf("BuildPgDSN (no password) = %q, want %q", dsn, want)
	}

	dsn = BuildPgDSN("localhost", "5432", "user", "p@ss:wo/rd", "my db")
	want = "postgres://user:p%40ss%3Awo%2Frd@localhost:5432/my%20db?sslmode=disable"
	if dsn != want {
		t.Errorf("BuildPgDSN (escaped) = %q, want %q", dsn, want)
	}
}

func TestValidDatabaseName(t *testing.T) {
	valid := []string{"test_hiddos_ecp", "test-hiddos-ecp", "test123", "TEST_123"}
	for _, name := range valid {
		if !validDatabaseName(name) {
			t.Errorf("validDatabaseName(%q) = false, want true", name)
		}
	}

	invalid := []string{"", "my db", `bad"name`, "bad;name", "bad/name"}
	for _, name := range invalid {
		if validDatabaseName(name) {
			t.Errorf("validDatabaseName(%q) = true, want false", name)
		}
	}
}

func newTestStore(t *testing.T) *PgStore {
	t.Helper()
	dbname := "test_hiddos_ecp_" + strings.ToLower(strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	dsn := testDSN(dbname)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPgStore: %v", err)
	}

	t.Cleanup(func() {
		store.Close()

		// 用独立 context 做清理
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		adminDSN := testDSN("postgres")
		adminConn, err := pgx.Connect(cleanupCtx, adminDSN)
		if err != nil {
			t.Logf("cleanup: 连接 postgres 失败: %v", err)
			return
		}
		defer adminConn.Close(cleanupCtx)

		// 终止目标库所有连接
		tag, _ := adminConn.Exec(cleanupCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, dbname)
		t.Logf("cleanup: terminated %s for %s", tag, dbname)
		time.Sleep(300 * time.Millisecond)

		// 删除数据库
		tag, err = adminConn.Exec(cleanupCtx, `DROP DATABASE IF EXISTS `+quotePGIdentifier(dbname))
		if err != nil {
			t.Logf("cleanup: DROP %s 失败: %v", dbname, err)
		} else {
			t.Logf("cleanup: %s %s", tag, dbname)
		}
	})

	return store
}

func TestSaveAndLoad(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rules := []*xdsserver.ProxyRule{
		{ID: "bbb", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "aaa", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8081, Weight: 1}}, LBPolicy: "LEAST_REQUEST"},
	}

	if err := s.Save(ctx, rules); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("Load returned %d rules, want 2", len(loaded))
	}
	if loaded[0].ID != "aaa" || loaded[1].ID != "bbb" {
		t.Errorf("rules not sorted: ids = [%s, %s], want [aaa, bbb]", loaded[0].ID, loaded[1].ID)
	}
}

func TestSaveEmptyList(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, nil); err != nil {
		t.Fatalf("Save nil: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("Load got %d rules, want 0", len(loaded))
	}
}

func TestSaveOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rules1 := []*xdsserver.ProxyRule{
		{ID: "aaa", Name: "first", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}, LBPolicy: "ROUND_ROBIN"},
	}
	if err := s.Save(ctx, rules1); err != nil {
		t.Fatalf("Save first: %v", err)
	}

	rules2 := []*xdsserver.ProxyRule{
		{ID: "bbb", Name: "second", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 53, Weight: 1}}, LBPolicy: "RANDOM"},
	}
	if err := s.Save(ctx, rules2); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("Load returned %d rules, want 1", len(loaded))
	}
	if loaded[0].ID != "bbb" || loaded[0].Name != "second" {
		t.Errorf("got rule %s (%s), want bbb (second)", loaded[0].ID, loaded[0].Name)
	}
}

func TestRevisionChangesAfterSave(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	before, err := s.Revision(ctx)
	if err != nil {
		t.Fatalf("Revision before: %v", err)
	}
	if err := s.Save(ctx, []*xdsserver.ProxyRule{{
		ID:         "aaa",
		Name:       "rev",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy:   "ROUND_ROBIN",
	}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	after, err := s.Revision(ctx)
	if err != nil {
		t.Fatalf("Revision after: %v", err)
	}
	if before == after {
		t.Fatalf("Revision did not change: %q", after)
	}
}

func TestLoadEmpty(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("Load got %d rules, want 0", len(loaded))
	}
}

func TestSaveMultipleBackends(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rules := []*xdsserver.ProxyRule{
		{ID: "aaa", Name: "multi", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{
				{Address: "10.0.0.1", Port: 8080, Weight: 1},
				{Address: "10.0.0.2", Port: 8080, Weight: 2},
				{Address: "10.0.0.3", Port: 8081, Weight: 1},
			}, LBPolicy: "RING_HASH"},
	}

	if err := s.Save(ctx, rules); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("Load returned %d rules, want 1", len(loaded))
	}
	if len(loaded[0].Backends) != 3 {
		t.Errorf("got %d backends, want 3", len(loaded[0].Backends))
	}
	if loaded[0].LBPolicy != "RING_HASH" {
		t.Errorf("LBPolicy = %s, want RING_HASH", loaded[0].LBPolicy)
	}
}
