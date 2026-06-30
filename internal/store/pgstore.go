package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	xdsserver "envoy-control-plane/internal/server/xds"
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

CREATE TABLE IF NOT EXISTS proxy_rule_push_log (
    revision   BIGINT PRIMARY KEY,
    status     VARCHAR(16) NOT NULL,
    error_msg  TEXT,
    pushed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at   TIMESTAMPTZ,
    nacked_at  TIMESTAMPTZ
);
`

type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(ctx context.Context, dsn string) (*PgStore, error) {
	parsedDSN, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析数据库连接串失败: %w", err)
	}
	dbname := strings.ToLower(parsedDSN.Database)
	if !validDatabaseName(dbname) {
		return nil, fmt.Errorf("database.dbname 只能包含字母、数字、下划线和短横线")
	}

	// 连接 postgres 库来创建目标库
	port := strconv.Itoa(int(parsedDSN.Port))
	adminDSN := BuildPgDSN(parsedDSN.Host, port, parsedDSN.User, parsedDSN.Password, "postgres")

	adminConn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	defer adminConn.Close(ctx)

	if err := adminConn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("数据库不可达: %w", err)
	}

	var exists bool
	err = adminConn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, dbname).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("检查数据库是否存在失败: %w", err)
	}
	if !exists {
		if _, err := adminConn.Exec(ctx, `CREATE DATABASE `+quotePGIdentifier(dbname)); err != nil {
			return nil, fmt.Errorf("创建数据库失败: %w", err)
		}
		log.Printf("已自动创建数据库 %s", dbname)
	}

	// 连接目标库（统一用小写库名）
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

	if _, err := pool.Exec(ctx, createSchemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("初始化表结构失败: %w", err)
	}

	return &PgStore{pool: pool}, nil
}

func (s *PgStore) Close() {
	s.pool.Close()
}

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
			log.Printf("跳过非法规则: %v", err)
			continue
		}
		if err := json.Unmarshal(backendsJSON, &r.Backends); err != nil {
			log.Printf("跳过非法规则 backends 解析失败: %v", err)
			continue
		}
		rules = append(rules, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取规则失败: %w", err)
	}

	log.Printf("已加载 %d 条规则", len(rules))
	return rules, nil
}

func (s *PgStore) Save(ctx context.Context, rules []*xdsserver.ProxyRule) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM proxy_rules`); err != nil {
		return fmt.Errorf("清空规则失败: %w", err)
	}

	if len(rules) == 0 {
		return tx.Commit(ctx)
	}

	sorted := make([]*xdsserver.ProxyRule, len(rules))
	copy(sorted, rules)
	slices.SortFunc(sorted, func(a, b *xdsserver.ProxyRule) int {
		return strings.Compare(a.ID, b.ID)
	})

	const insertSQL = `INSERT INTO proxy_rules (id, name, protocol, listen_addr, listen_port, backends, lb_policy)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	for _, r := range sorted {
		backendsJSON, err := json.Marshal(r.Backends)
		if err != nil {
			return fmt.Errorf("序列化 backends 失败: %w", err)
		}
		if _, err := tx.Exec(ctx, insertSQL, r.ID, r.Name, r.Protocol, r.ListenAddr, r.ListenPort, backendsJSON, r.LBPolicy); err != nil {
			return fmt.Errorf("插入规则 %s 失败: %w", r.ID, err)
		}
	}

	return tx.Commit(ctx)
}

func (s *PgStore) Revision(ctx context.Context) (string, error) {
	var revision string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(md5(COALESCE(string_agg(
			id || E'\x1f' || name || E'\x1f' || protocol || E'\x1f' ||
			listen_addr || E'\x1f' || listen_port::text || E'\x1f' ||
			backends::text || E'\x1f' || lb_policy,
			E'\x1e' ORDER BY id), '')), '')
		FROM proxy_rules`).Scan(&revision)
	if err != nil {
		return "", fmt.Errorf("查询规则版本失败: %w", err)
	}
	return revision, nil
}

func (s *PgStore) SaveAndBumpRevision(ctx context.Context, rules []*xdsserver.ProxyRule) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM proxy_rules`); err != nil {
		return 0, fmt.Errorf("清空规则失败: %w", err)
	}

	if len(rules) > 0 {
		sorted := make([]*xdsserver.ProxyRule, len(rules))
		copy(sorted, rules)
		slices.SortFunc(sorted, func(a, b *xdsserver.ProxyRule) int {
			return strings.Compare(a.ID, b.ID)
		})

		const insertSQL = `INSERT INTO proxy_rules (id, name, protocol, listen_addr, listen_port, backends, lb_policy)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`

		for _, r := range sorted {
			backendsJSON, err := json.Marshal(r.Backends)
			if err != nil {
				return 0, fmt.Errorf("序列化 backends 失败: %w", err)
			}
			if _, err := tx.Exec(ctx, insertSQL, r.ID, r.Name, r.Protocol, r.ListenAddr, r.ListenPort, backendsJSON, r.LBPolicy); err != nil {
				return 0, fmt.Errorf("插入规则 %s 失败: %w", r.ID, err)
			}
		}
	}

	var newRev int64
	err = tx.QueryRow(ctx, `
		UPDATE revision_counter SET current_revision = current_revision + 1
		WHERE id = 1 RETURNING current_revision`).Scan(&newRev)
	if err != nil {
		return 0, fmt.Errorf("递增 revision 失败: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("提交事务失败: %w", err)
	}
	return newRev, nil
}

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
	if err := replaceRulesTx(ctx, tx, nextRules); err != nil {
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

func replaceRulesTx(ctx context.Context, tx pgx.Tx, rules []*xdsserver.ProxyRule) error {
	if _, err := tx.Exec(ctx, `DELETE FROM proxy_rules`); err != nil {
		return fmt.Errorf("清空规则失败: %w", err)
	}
	if len(rules) == 0 {
		return nil
	}

	sorted := make([]*xdsserver.ProxyRule, len(rules))
	copy(sorted, rules)
	slices.SortFunc(sorted, func(a, b *xdsserver.ProxyRule) int {
		return strings.Compare(a.ID, b.ID)
	})

	const insertSQL = `INSERT INTO proxy_rules (id, name, protocol, listen_addr, listen_port, backends, lb_policy)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	for _, r := range sorted {
		backendsJSON, err := json.Marshal(r.Backends)
		if err != nil {
			return fmt.Errorf("序列化 backends 失败: %w", err)
		}
		if _, err := tx.Exec(ctx, insertSQL, r.ID, r.Name, r.Protocol, r.ListenAddr, r.ListenPort, backendsJSON, r.LBPolicy); err != nil {
			return fmt.Errorf("插入规则 %s 失败: %w", r.ID, err)
		}
	}
	return nil
}

func (s *PgStore) LoadRevision(ctx context.Context) (int64, error) {
	var rev int64
	err := s.pool.QueryRow(ctx, `SELECT current_revision FROM revision_counter WHERE id = 1`).Scan(&rev)
	if err != nil {
		return 0, fmt.Errorf("读取 revision 失败: %w", err)
	}
	return rev, nil
}

func (s *PgStore) LogPushPending(ctx context.Context, revision int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO proxy_rule_push_log (revision, status)
		VALUES ($1, 'pending')
		ON CONFLICT (revision) DO UPDATE SET status = 'pending', error_msg = NULL, pushed_at = NOW(), acked_at = NULL, nacked_at = NULL`,
		revision)
	if err != nil {
		return fmt.Errorf("记录 push pending 失败: %w", err)
	}
	return nil
}

func (s *PgStore) MarkPushDeployed(ctx context.Context, revision int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_rule_push_log SET status = 'deployed', acked_at = NOW()
		WHERE revision = $1`, revision)
	if err != nil {
		return fmt.Errorf("标记 push deployed 失败: %w", err)
	}
	return nil
}

func (s *PgStore) MarkPushFailed(ctx context.Context, revision int64, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE proxy_rule_push_log SET status = 'failed', error_msg = $2, nacked_at = NOW()
		WHERE revision = $1`, revision, errMsg)
	if err != nil {
		return fmt.Errorf("标记 push failed 失败: %w", err)
	}
	return nil
}

func (s *PgStore) PushStatus(ctx context.Context, revision int64) (string, error) {
	var status string
	err := s.pool.QueryRow(ctx, `SELECT status FROM proxy_rule_push_log WHERE revision = $1`, revision).Scan(&status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("查询 push 状态失败: %w", err)
	}
	return status, nil
}

func BuildPgDSN(host, port, user, password, dbname string) string {
	u := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(host, port),
		Path:     dbname,
		RawQuery: "sslmode=disable",
	}
	if password != "" {
		u.User = url.UserPassword(user, password)
	} else {
		u.User = url.User(user)
	}
	return u.String()
}

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

func quotePGIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
