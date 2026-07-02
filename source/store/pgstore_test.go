package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"

	xdsserver "envoy-control-plane/source/server/xds"
)

const testDBName = "dev_test"

func TestMain(m *testing.M) {
	godotenv.Load("../../.env")
	os.Exit(m.Run())
}

// testDSN 为测试环境构造数据库 DSN 字符串。
// 复用应用的环境变量（DB_HOST、DB_PORT、DB_USER、DB_PASSWORD）。
func testDSN(t *testing.T, dbname string) string {
	t.Helper()
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "postgres"
	}
	pass := os.Getenv("DB_PASSWORD")
	return BuildPgDSN(host, port, user, pass, dbname)
}

// dropTestDB 终止指定数据库的所有连接并删除。
func dropTestDB(t *testing.T, dbname string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, testDSN(t, "postgres"))
	if err != nil {
		t.Logf("dropTestDB: 连接 postgres 失败: %v", err)
		return
	}
	defer conn.Close(ctx)

	tag, _ := conn.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, dbname)
	t.Logf("dropTestDB: terminated %s for %s", tag, dbname)
	time.Sleep(300 * time.Millisecond)

	tag, err = conn.Exec(ctx, `DROP DATABASE IF EXISTS `+quotePGIdentifier(dbname))
	if err != nil {
		t.Logf("dropTestDB: DROP %s 失败: %v", dbname, err)
	} else {
		t.Logf("dropTestDB: %s %s", tag, dbname)
	}
}

// resetTestTables 清空所有表数据并重置 revision 为 0。
func resetTestTables(t *testing.T, store *PgStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.pool.Exec(ctx, `TRUNCATE proxy_rules, proxy_push_log RESTART IDENTITY`); err != nil {
		t.Fatalf("TRUNCATE 失败: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE revision_counter SET current_revision = 0 WHERE id = 1`); err != nil {
		t.Fatalf("重置 revision 失败: %v", err)
	}
}

// newTestStore 创建用于集成测试的 PgStore 实例，使用共享测试库。
// 每次调用会清空表数据，测试结束后自动删除数据库。
func newTestStore(t *testing.T) *PgStore {
	t.Helper()

	// 清理可能残留的旧库
	dropTestDB(t, testDBName)

	// 连接 postgres 管理库，创建测试库
	adminDSN := testDSN(t, "postgres")
	adminCtx, adminCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer adminCancel()
	adminConn, err := pgx.Connect(adminCtx, adminDSN)
	if err != nil {
		t.Fatalf("连接 postgres 失败: %v", err)
	}
	defer adminConn.Close(adminCtx)

	if _, err := adminConn.Exec(adminCtx, `CREATE DATABASE `+quotePGIdentifier(testDBName)); err != nil {
		t.Fatalf("创建测试库 %s 失败: %v", testDBName, err)
	}

	dsn := testDSN(t, testDBName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewPgStore(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPgStore: %v", err)
	}
	if err := store.InitDB(ctx); err != nil {
		store.Close()
		t.Fatalf("InitDB: %v", err)
	}

	// 每个测试开始前清空数据
	resetTestTables(t, store)

	t.Cleanup(func() {
		store.Close()
		dropTestDB(t, testDBName)
	})

	return store
}

// TestBuildPgDSN 测试 BuildPgDSN 函数生成正确的 PostgreSQL 连接字符串。
func TestBuildPgDSN(t *testing.T) {
	t.Setenv("DB_SSLMODE", "")
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

	t.Setenv("DB_SSLMODE", "verify-full")
	dsn = BuildPgDSN("db.example.com", "5432", "user", "pass", "mydb")
	want = "postgres://user:pass@db.example.com:5432/mydb?sslmode=verify-full"
	if dsn != want {
		t.Errorf("BuildPgDSN (sslmode) = %q, want %q", dsn, want)
	}
}

// TestValidDatabaseName 测试 validDatabaseName 函数对数据库名称的校验。
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

// TestSaveAndLoad 测试规则的保存和加载功能。
func TestSaveAndLoad(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rules := []*xdsserver.ProxyRule{
		{ID: "bbb", Name: "b", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}, LBPolicy: "ROUND_ROBIN"},
		{ID: "aaa", Name: "a", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8081, Weight: 1}}, LBPolicy: "LEAST_REQUEST"},
	}

	if _, _, err := s.MutateRulesAndBumpRevision(ctx, func(_ []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return rules, nil
	}); err != nil {
		t.Fatalf("MutateRulesAndBumpRevision: %v", err)
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

// TestSaveEmptyList 测试保存空规则列表的行为。
func TestSaveEmptyList(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.MutateRulesAndBumpRevision(ctx, func(_ []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("MutateRulesAndBumpRevision nil: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("Load got %d rules, want 0", len(loaded))
	}
}

// TestSaveOverwrites 测试多次保存时规则被正确覆盖。
func TestSaveOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rules1 := []*xdsserver.ProxyRule{
		{ID: "aaa", Name: "first", Protocol: "http", ListenAddr: "0.0.0.0", ListenPort: 9981,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}}, LBPolicy: "ROUND_ROBIN"},
	}
	if _, _, err := s.MutateRulesAndBumpRevision(ctx, func(_ []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return rules1, nil
	}); err != nil {
		t.Fatalf("MutateRulesAndBumpRevision first: %v", err)
	}

	rules2 := []*xdsserver.ProxyRule{
		{ID: "bbb", Name: "second", Protocol: "udp", ListenAddr: "0.0.0.0", ListenPort: 9982,
			Backends: []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 53, Weight: 1}}, LBPolicy: "RANDOM"},
	}
	if _, _, err := s.MutateRulesAndBumpRevision(ctx, func(_ []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return rules2, nil
	}); err != nil {
		t.Fatalf("MutateRulesAndBumpRevision second: %v", err)
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

// TestRevisionChangesAfterSave 测试保存规则后 revision 号正确递增。
func TestRevisionChangesAfterSave(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	before, err := s.LoadRevision(ctx)
	if err != nil {
		t.Fatalf("LoadRevision before: %v", err)
	}
	_, _, err = s.MutateRulesAndBumpRevision(ctx, func(current []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return []*xdsserver.ProxyRule{{
			ID:         "aaa",
			Name:       "rev",
			Protocol:   "http",
			ListenAddr: "0.0.0.0",
			ListenPort: 9981,
			Backends:   []xdsserver.BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
			LBPolicy:   "ROUND_ROBIN",
		}}, nil
	})
	if err != nil {
		t.Fatalf("MutateRulesAndBumpRevision: %v", err)
	}
	after, err := s.LoadRevision(ctx)
	if err != nil {
		t.Fatalf("LoadRevision after: %v", err)
	}
	if before >= after {
		t.Fatalf("Revision did not advance: before=%d after=%d", before, after)
	}
}

// TestLoadEmpty 测试在无规则时 Load 返回空列表。
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

// TestSaveMultipleBackends 测试保存包含多个后端节点的规则。
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

	if _, _, err := s.MutateRulesAndBumpRevision(ctx, func(_ []*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error) {
		return rules, nil
	}); err != nil {
		t.Fatalf("MutateRulesAndBumpRevision: %v", err)
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
