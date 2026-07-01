package store

// pgstore.go —— PostgreSQL 持久化存储
//
// 管理 proxy_rules、revision_counter 和 proxy_push_log 表，
// 提供规则 CRUD、revision 管理和推送状态追踪。

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	xdsserver "envoy-control-plane/source/server/xds"
)

const createSchemaSQL = `
CREATE TABLE IF NOT EXISTS proxy_rules (
    id          VARCHAR(32) PRIMARY KEY,
    name        VARCHAR(256) NOT NULL UNIQUE,
    protocol    VARCHAR(16)  NOT NULL DEFAULT 'http',
    listen_addr VARCHAR(256) NOT NULL,
    listen_port INTEGER      NOT NULL CHECK (listen_port BETWEEN 10 AND 65535),
    backends    JSONB        NOT NULL DEFAULT '[]',
    lb_policy   VARCHAR(64)  NOT NULL DEFAULT 'ROUND_ROBIN',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proxy_rules_name ON proxy_rules (name);
CREATE INDEX IF NOT EXISTS idx_proxy_rules_protocol ON proxy_rules (protocol);

CREATE TABLE IF NOT EXISTS revision_counter (
    id              BIGINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    current_revision BIGINT NOT NULL DEFAULT 0
);

INSERT INTO revision_counter (id, current_revision)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS proxy_push_log (
    revision   BIGINT PRIMARY KEY,
    status     VARCHAR(16) NOT NULL,
    error_msg  TEXT,
    pushed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at   TIMESTAMPTZ,
    nacked_at  TIMESTAMPTZ
);
`

// PgStore 基于 PostgreSQL 的规则持久化存储。
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore 创建 PostgreSQL 存储实例，连接目标数据库并建立连接池。数据库须已存在。
func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	parsedDSN, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析数据库连接串失败: %w", err)
	}
	dbname := strings.ToLower(parsedDSN.Database)
	if !validDatabaseName(dbname) {
		return nil, fmt.Errorf("database.dbname 只能包含字母、数字、下划线和短横线")
	}

	// 连接目标库
	port := strconv.Itoa(int(parsedDSN.Port))
	targetDSN := BuildPgDSN(parsedDSN.Host, port, parsedDSN.User, parsedDSN.Password, dbname)
	cfg, err := pgxpool.ParseConfig(targetDSN)
	if err != nil {
		return nil, fmt.Errorf("解析连接池配置失败: %w", err)
	}
	cfg.MaxConns = 5
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("创建连接池失败: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接目标数据库失败: %w", err)
	}

	return &PgStore{pool: pool}, nil
}

// InitDB 初始化数据库表结构，包括 proxy_rules、revision_counter 和 proxy_push_log 表。
func (s *PgStore) InitDB(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, createSchemaSQL); err != nil {
		return fmt.Errorf("初始化表结构失败: %w", err)
	}
	return nil
}

// DropDB 终止所有连接并删除指定名称的数据库。
func (s *PgStore) DropDB(ctx context.Context, dbName string) error {
	// 终止所有连接
	_, _ = s.pool.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`,
		dbName)

	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, quotePGIdentifier(dbName))); err != nil {
		return fmt.Errorf("删除数据库失败: %w", err)
	}
	return nil
}

// Close 关闭连接池，释放数据库资源。
func (s *PgStore) Close() {
	s.pool.Close()
}

// LoadOne 根据 ID 查询单条规则，不存在时返回 nil 而非错误。
func (s *PgStore) LoadOne(ctx context.Context, id string) (*xdsserver.ProxyRule, error) {
	r := &xdsserver.ProxyRule{}
	var backendsJSON []byte
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, protocol, listen_addr, listen_port, backends, lb_policy
		 FROM proxy_rules WHERE id = $1`, id).
		Scan(&r.ID, &r.Name, &r.Protocol, &r.ListenAddr, &r.ListenPort, &backendsJSON, &r.LBPolicy)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询规则失败: %w", err)
	}
	if err := json.Unmarshal(backendsJSON, &r.Backends); err != nil {
		return nil, fmt.Errorf("解析规则 %s backends 失败: %w", r.ID, err)
	}
	return r, nil
}

// Load 查询所有规则并按 ID 排序返回。
func (s *PgStore) Load(ctx context.Context) ([]*xdsserver.ProxyRule, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, protocol, listen_addr, listen_port, backends, lb_policy
		 FROM proxy_rules ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("查询规则失败: %w", err)
	}
	defer rows.Close()

	var rules []*xdsserver.ProxyRule
	for rows.Next() {
		r := &xdsserver.ProxyRule{}
		var backendsJSON []byte
		if err := rows.Scan(&r.ID, &r.Name, &r.Protocol, &r.ListenAddr, &r.ListenPort, &backendsJSON, &r.LBPolicy); err != nil {
			return nil, fmt.Errorf("读取规则失败: %w", err)
		}
		if err := json.Unmarshal(backendsJSON, &r.Backends); err != nil {
			return nil, fmt.Errorf("解析规则 %s backends 失败: %w", r.ID, err)
		}
		rules = append(rules, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取规则失败: %w", err)
	}

	return rules, nil
}

// LoadRevision 从 revision_counter 表读取当前 revision 值。
func (s *PgStore) LoadRevision(ctx context.Context) (int64, error) {
	var rev int64
	err := s.pool.QueryRow(ctx, `SELECT current_revision FROM revision_counter WHERE id = 1`).Scan(&rev)
	if err != nil {
		return 0, fmt.Errorf("读取 revision 失败: %w", err)
	}
	return rev, nil
}

// MutateRulesAndBumpRevision 在事务中执行规则变更并递增 revision，保证原子性。
func (s *PgStore) MutateRulesAndBumpRevision(ctx context.Context, mutate func([]*xdsserver.ProxyRule) ([]*xdsserver.ProxyRule, error)) ([]*xdsserver.ProxyRule, int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT current_revision FROM revision_counter WHERE id = 1 FOR UPDATE`); err != nil {
		return nil, 0, fmt.Errorf("锁定 revision 失败: %w", err)
	}

	rules, err := loadRulesTx(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	nextRules, err := mutate(rules)
	if err != nil {
		return nil, 0, err
	}
	if err := replaceRulesTx(ctx, tx, rules, nextRules); err != nil {
		return nil, 0, err
	}

	var newRev int64
	err = tx.QueryRow(ctx, `
		UPDATE revision_counter SET current_revision = current_revision + 1
		WHERE id = 1 RETURNING current_revision`).Scan(&newRev)
	if err != nil {
		return nil, 0, fmt.Errorf("递增 revision 失败: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("提交事务失败: %w", err)
	}
	return nextRules, newRev, nil
}

// loadRulesTx 在事务中加载所有规则。
func loadRulesTx(ctx context.Context, tx pgx.Tx) ([]*xdsserver.ProxyRule, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, name, protocol, listen_addr, listen_port, backends, lb_policy
		 FROM proxy_rules ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("查询规则失败: %w", err)
	}
	defer rows.Close()

	var rules []*xdsserver.ProxyRule
	for rows.Next() {
		r := &xdsserver.ProxyRule{}
		var backendsJSON []byte
		if err := rows.Scan(&r.ID, &r.Name, &r.Protocol, &r.ListenAddr, &r.ListenPort, &backendsJSON, &r.LBPolicy); err != nil {
			return nil, fmt.Errorf("读取规则失败: %w", err)
		}
		if err := json.Unmarshal(backendsJSON, &r.Backends); err != nil {
			return nil, fmt.Errorf("解析规则 %s backends 失败: %w", r.ID, err)
		}
		rules = append(rules, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取规则失败: %w", err)
	}
	return rules, nil
}

// replaceRulesTx 在事务中对比新旧规则列表，执行删除和 UPSERT 操作。
func replaceRulesTx(ctx context.Context, tx pgx.Tx, oldRules []*xdsserver.ProxyRule, newRules []*xdsserver.ProxyRule) error {
	oldMap := make(map[string]*xdsserver.ProxyRule, len(oldRules))
	for _, r := range oldRules {
		oldMap[r.ID] = r
	}
	newMap := make(map[string]*xdsserver.ProxyRule, len(newRules))
	for _, r := range newRules {
		newMap[r.ID] = r
	}

	// 删除旧有新无的规则
	for id := range oldMap {
		if _, exists := newMap[id]; !exists {
			if _, err := tx.Exec(ctx, `DELETE FROM proxy_rules WHERE id = $1`, id); err != nil {
				return fmt.Errorf("删除规则 %s 失败: %w", id, err)
			}
		}
	}

	// UPSERT 新规则（INSERT 或 UPDATE）
	const upsertSQL = `INSERT INTO proxy_rules (id, name, protocol, listen_addr, listen_port, backends, lb_policy)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name, protocol = EXCLUDED.protocol,
			listen_addr = EXCLUDED.listen_addr, listen_port = EXCLUDED.listen_port,
			backends = EXCLUDED.backends, lb_policy = EXCLUDED.lb_policy`
	for _, r := range newRules {
		backendsJSON, err := json.Marshal(r.Backends)
		if err != nil {
			return fmt.Errorf("序列化 backends 失败: %w", err)
		}
		if _, err := tx.Exec(ctx, upsertSQL, r.ID, r.Name, r.Protocol, r.ListenAddr, r.ListenPort, backendsJSON, r.LBPolicy); err != nil {
			return fmt.Errorf("保存规则 %s 失败: %w", r.ID, err)
		}
	}
	return nil
}

// LogPushPending 记录推送状态为 pending，已存在时重置状态。
func (s *PgStore) LogPushPending(ctx context.Context, revision int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO proxy_push_log (revision, status)
		VALUES ($1, 'pending')
		ON CONFLICT (revision) DO UPDATE SET status = 'pending', error_msg = NULL, pushed_at = NOW(), acked_at = NULL, nacked_at = NULL`,
		revision)
	if err != nil {
		return fmt.Errorf("记录 push pending 失败: %w", err)
	}
	return nil
}

// MarkPushDeployed 将指定 revision 的推送状态标记为 deployed。
func (s *PgStore) MarkPushDeployed(ctx context.Context, revision int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_push_log SET status = 'deployed', acked_at = NOW()
		WHERE revision = $1`, revision)
	if err != nil {
		return fmt.Errorf("标记 push deployed 失败: %w", err)
	}
	return nil
}

// MarkPushFailed 将指定 revision 的推送状态标记为 failed，并记录错误信息。
func (s *PgStore) MarkPushFailed(ctx context.Context, revision int64, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_push_log SET status = 'failed', error_msg = $2, nacked_at = NOW()
		WHERE revision = $1`, revision, errMsg)
	if err != nil {
		return fmt.Errorf("标记 push failed 失败: %w", err)
	}
	return nil
}

// MarkPushTimeout 将指定 revision 的推送状态标记为 timeout，等待下一轮 reconcile。
func (s *PgStore) MarkPushTimeout(ctx context.Context, revision int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_push_log SET status = 'timeout'
		WHERE revision = $1 AND status = 'pending'`, revision)
	if err != nil {
		return fmt.Errorf("标记 push timeout 失败: %w", err)
	}
	return nil
}

// PushStatus 查询指定 revision 的推送状态，不存在时返回空字符串。
func (s *PgStore) PushStatus(ctx context.Context, revision int64) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx, `SELECT status FROM proxy_push_log WHERE revision = $1`, revision).Scan(&status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("查询 push 状态失败: %w", err)
	}
	return status, nil
}

// BuildPgDSN 根据主机、端口、用户名、密码和数据库名构建 PostgreSQL DSN 连接字符串。
func BuildPgDSN(host, port, user, password, dbname string) string {
	sslmode := strings.TrimSpace(os.Getenv("DB_SSLMODE"))
	if sslmode == "" {
		sslmode = "disable"
	}
	u := url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, port),
		Path:   dbname,
	}
	q := u.Query()
	q.Set("sslmode", sslmode)
	u.RawQuery = q.Encode()
	if password != "" {
		u.User = url.UserPassword(user, password)
	} else {
		u.User = url.User(user)
	}
	return u.String()
}

// validDatabaseName 校验数据库名称是否仅包含字母、数字、下划线和短横线。
func validDatabaseName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

// quotePGIdentifier 将 PostgreSQL 标识符用双引号包裹并转义内部双引号。
func quotePGIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
