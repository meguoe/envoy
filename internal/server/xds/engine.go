package xdsserver

// engine.go —— xDS 引擎，对外接口
//
// Engine 封装了所有 xDS 状态，外部通过 Engine 方法操作规则
// 内部自动管理：增量缓存、快照推送、失败回滚
//
// 锁策略：
//   - mu        保护 rules map 的读写
//   - pushMu    串行化「规则修改 + 快照推送」，保证原子性
//   - CRUD 方法在 pushMu 内先修改 rules，再推送快照，失败则回滚
//   - ListRules / GetRule 只读，不持有 pushMu，可以与推送并发

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	grpc "google.golang.org/grpc"

	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
)

// ruleRes 缓存单条规则构建好的 protobuf 资源
type ruleRes struct {
	owner    *ProxyRule
	endpoint types.Resource // EDS
	cluster  types.Resource // CDS
	route    types.Resource // RDS
	listener types.Resource // LDS
}

// ValidationError 业务校验错误，区别于系统错误（如推送失败）
type ValidationError struct {
	Msg string
}

func (e *ValidationError) Error() string { return e.Msg }

// ErrRuleNotFound 规则不存在错误
var ErrRuleNotFound = errors.New("rule not found")

// Engine xDS 引擎，封装所有 xDS 状态
type Engine struct {
	nodeID          string
	snapCache       cache.SnapshotCache
	rules           map[string]*ProxyRule
	resCache        map[string]*ruleRes // 注意: 仅在 pushMu 保护下读写, 不可并发访问
	versionSeq      uint64
	persistFailures atomic.Uint64 // 持久化连续失败计数
	mu              sync.RWMutex  // 保护 rules
	pushMu          sync.Mutex    // 串行化「修改 + 推送」
	persistMu       sync.Mutex    // 串行化持久化写入，避免旧快照后写覆盖新快照
	onRulesChanged  func([]*ProxyRule) error
	connectTimeout  time.Duration
	udpIdleTimeout  time.Duration
	ctx             context.Context
	cancel          context.CancelFunc

	// 冲突检测索引（在 pushMu + mu 保护下维护）
	nameIndex map[string]string // ruleName → ruleID
	portIndex map[string]string // "addr:port:proto" → ruleID
}

// NewEngine 创建 xDS 引擎
func NewEngine(nodeID string, connectTimeout, udpIdleTimeout time.Duration) *Engine {
	if connectTimeout <= 0 {
		connectTimeout = time.Second
	}
	if udpIdleTimeout <= 0 {
		udpIdleTimeout = 60 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		nodeID:         nodeID,
		snapCache:      cache.NewSnapshotCache(true, cache.IDHash{}, nil),
		rules:          make(map[string]*ProxyRule),
		resCache:       make(map[string]*ruleRes),
		connectTimeout: connectTimeout,
		udpIdleTimeout: udpIdleTimeout,
		nameIndex:      make(map[string]string),
		portIndex:      make(map[string]string),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Close 取消引擎上下文，释放资源
func (e *Engine) Close() {
	e.cancel()
}

// SetOnRulesChanged 设置规则变更后的持久化回调（同步调用）
// 每次 CRUD 成功并推送快照后自动调用，返回 error 表示持久化失败
// 注意：必须在任何 CRUD 操作之前调用（启动时），不保证并发安全
func (e *Engine) SetOnRulesChanged(fn func([]*ProxyRule) error) {
	e.onRulesChanged = fn
}

// notifyRulesChanged 同步执行持久化回调
// 在 pushMu 保护下、快照推送成功后调用
// 持久化失败不回滚（Envoy 已收到新配置），但记录失败计数并告警
func (e *Engine) notifyRulesChanged() {
	e.notifyRulesChangedWith(e.ListRules())
}

func (e *Engine) notifyRulesChangedWith(rules []*ProxyRule) {
	if e.onRulesChanged == nil {
		return
	}
	if err := e.onRulesChanged(rules); err != nil {
		n := e.persistFailures.Add(1)
		log.Printf("持久化失败 (累计 %d 次): %v", n, err)
		if n >= 3 {
			log.Printf("警告: 持久化已连续失败 %d 次，内存规则与文件可能不一致", n)
		}
	} else {
		e.persistFailures.Store(0)
	}
}

func (e *Engine) persistRulesAfterPush(rules []*ProxyRule) {
	e.persistMu.Lock()
	e.pushMu.Unlock()
	e.notifyRulesChangedWith(rules)
	e.persistMu.Unlock()
}

// ─── gRPC 服务器 ───────────────────────────────────────────────────────

// NewGRPCServer 创建 xDS gRPC 服务器（不启动）
// opts 可选的 gRPC 服务器选项（如 TLS 凭证）
func (e *Engine) NewGRPCServer(opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts,
		grpc.UnaryInterceptor(UnaryServerInterceptor()),
		grpc.StreamInterceptor(StreamServerInterceptor()),
	)
	srv := server.NewServer(e.ctx, e.snapCache, nil)
	gs := grpc.NewServer(opts...)
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(gs, srv)
	clusterservice.RegisterClusterDiscoveryServiceServer(gs, srv)
	endpointservice.RegisterEndpointDiscoveryServiceServer(gs, srv)
	listenerservice.RegisterListenerDiscoveryServiceServer(gs, srv)
	routeservice.RegisterRouteDiscoveryServiceServer(gs, srv)
	return gs
}

// StartGRPC 启动 xDS gRPC 服务器（阻塞）
func (e *Engine) StartGRPC(addr string, gs *grpc.Server) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("监听 gRPC 失败: %w", err)
	}
	log.Printf("xDS gRPC 启动 %s", addr)
	return gs.Serve(lis)
}

// ─── CRUD 操作 ─────────────────────────────────────────────────────────

// checkNameConflict 检查规则名是否被其他规则占用
// 调用方必须持有 e.mu
func (e *Engine) checkNameConflict(rule *ProxyRule) *ValidationError {
	if existingID, ok := e.nameIndex[rule.Name]; ok && existingID != rule.ID {
		return &ValidationError{
			Msg: fmt.Sprintf("规则名 %q 已被规则 %s 占用", rule.Name, existingID),
		}
	}
	return nil
}

// checkPortConflict 检查 listen_addr:listen_port + protocol 是否被其他规则占用
// 调用方必须持有 e.mu
func (e *Engine) checkPortConflict(rule *ProxyRule) *ValidationError {
	key := rule.ListenAddr + ":" + fmt.Sprintf("%d", rule.ListenPort) + ":" + rule.Protocol
	if existingID, ok := e.portIndex[key]; ok && existingID != rule.ID {
		return &ValidationError{
			Msg: fmt.Sprintf("%s://%s:%d 已被规则 %s 占用",
				rule.Protocol, rule.ListenAddr, rule.ListenPort, existingID),
		}
	}
	return nil
}

// addIndexes 添加规则到冲突检测索引
// 调用方必须持有 e.mu
func (e *Engine) addIndexes(rule *ProxyRule) {
	e.nameIndex[rule.Name] = rule.ID
	key := rule.ListenAddr + ":" + fmt.Sprintf("%d", rule.ListenPort) + ":" + rule.Protocol
	e.portIndex[key] = rule.ID
}

// removeIndexes 从冲突检测索引移除规则
// 调用方必须持有 e.mu
func (e *Engine) removeIndexes(rule *ProxyRule) {
	delete(e.nameIndex, rule.Name)
	key := rule.ListenAddr + ":" + fmt.Sprintf("%d", rule.ListenPort) + ":" + rule.Protocol
	delete(e.portIndex, key)
}

// SetRules 批量加载规则（用于启动时从文件加载，不触发推送）
func (e *Engine) SetRules(list []*ProxyRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	seenIDs := make(map[string]struct{}, len(list))
	for _, r := range list {
		if r.ID == "" {
			log.Printf("跳过 ID 为空的规则: %s", r.Name)
			continue
		}
		if _, ok := seenIDs[r.ID]; ok {
			log.Printf("跳过 ID 重复规则 %s: %s", r.ID, r.Name)
			continue
		}
		if _, ok := e.rules[r.ID]; ok {
			log.Printf("跳过 ID 冲突规则 %s: %s", r.ID, r.Name)
			continue
		}
		if err := e.checkNameConflict(r); err != nil {
			log.Printf("跳过名称冲突规则 %s: %v", r.ID, err)
			continue
		}
		if err := e.checkPortConflict(r); err != nil {
			log.Printf("跳过端口冲突规则 %s: %v", r.ID, err)
			continue
		}
		e.rules[r.ID] = r
		e.addIndexes(r)
		seenIDs[r.ID] = struct{}{}
	}
}

// CreateRule 创建规则并推送到 Envoy
// 流程：校验 → 生成ID → 加锁 → 端口冲突检测 → 写入内存 → 推送快照 → 失败则回滚
func (e *Engine) CreateRule(rule *ProxyRule) (*ProxyRule, error) {
	if err := ValidateRule(rule); err != nil {
		return nil, err
	}
	NormalizeRule(rule)
	rule.ID = GenerateID()

	e.pushMu.Lock()

	e.mu.Lock()
	if err := e.checkNameConflict(rule); err != nil {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, err
	}
	if err := e.checkPortConflict(rule); err != nil {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, err
	}
	e.rules[rule.ID] = rule
	e.addIndexes(rule)
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		delete(e.rules, rule.ID)
		e.removeIndexes(rule)
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, fmt.Errorf("推送快照失败: %w", err)
	}

	e.persistRulesAfterPush(e.ListRules())
	log.Printf("创建规则: %s (%s)  %s:%d -> %d 后端 [%s]",
		rule.ID, rule.Name, rule.ListenAddr, rule.ListenPort, len(rule.Backends), rule.LBPolicy)
	return rule, nil
}

// UpdateRule 更新规则并推送到 Envoy
func (e *Engine) UpdateRule(id string, rule *ProxyRule) (*ProxyRule, error) {
	if err := ValidateRule(rule); err != nil {
		return nil, err
	}
	NormalizeRule(rule)

	e.pushMu.Lock()

	e.mu.Lock()
	oldRule, existed := e.rules[id]
	if !existed {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, ErrRuleNotFound
	}
	rule.ID = id
	if err := e.checkNameConflict(rule); err != nil {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, err
	}
	if err := e.checkPortConflict(rule); err != nil {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, err
	}
	e.rules[id] = rule
	e.removeIndexes(oldRule)
	e.addIndexes(rule)
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		if _, stillThere := e.rules[id]; stillThere {
			e.rules[id] = oldRule
			e.removeIndexes(rule)
			e.addIndexes(oldRule)
		}
		e.mu.Unlock()
		e.pushMu.Unlock()
		return nil, fmt.Errorf("推送快照失败: %w", err)
	}

	e.persistRulesAfterPush(e.ListRules())
	log.Printf("更新规则: %s (%s)  %s:%d -> %d 后端 [%s]",
		rule.ID, rule.Name, rule.ListenAddr, rule.ListenPort, len(rule.Backends), rule.LBPolicy)
	return rule, nil
}

// DeleteRule 删除规则并推送到 Envoy
func (e *Engine) DeleteRule(id string) error {
	e.pushMu.Lock()

	e.mu.Lock()
	oldRule, existed := e.rules[id]
	if !existed {
		e.mu.Unlock()
		e.pushMu.Unlock()
		return ErrRuleNotFound
	}
	delete(e.rules, id)
	e.removeIndexes(oldRule)
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		e.rules[id] = oldRule
		e.addIndexes(oldRule)
		e.mu.Unlock()
		e.pushMu.Unlock()
		return fmt.Errorf("推送快照失败: %w", err)
	}

	e.persistRulesAfterPush(e.ListRules())
	log.Printf("删除规则: %s", id)
	return nil
}

// ListRules 返回所有规则的深拷贝（按 ID 排序）
func (e *Engine) ListRules() []*ProxyRule {
	e.mu.RLock()
	list := make([]*ProxyRule, 0, len(e.rules))
	for _, r := range e.rules {
		cp := *r
		if r.Backends != nil {
			cp.Backends = make([]BackendNode, len(r.Backends))
			copy(cp.Backends, r.Backends)
		}
		list = append(list, &cp)
	}
	e.mu.RUnlock()

	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})
	return list
}

// GetRule 获取单条规则的深拷贝
func (e *Engine) GetRule(id string) (*ProxyRule, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.rules[id]
	if !ok {
		return nil, false
	}
	cp := *r
	if r.Backends != nil {
		cp.Backends = make([]BackendNode, len(r.Backends))
		copy(cp.Backends, r.Backends)
	}
	return &cp, true
}

// PushSnapshot 手动触发快照推送（用于启动时初始化）
func (e *Engine) PushSnapshot() error {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()
	return e.pushSnapshotLocked()
}

// IsEnvoyConnected 检查是否有 Envoy 节点连接
func (e *Engine) IsEnvoyConnected() bool {
	info := e.snapCache.GetStatusInfo(e.nodeID)
	return info != nil && info.GetNumWatches() > 0
}

// PersistFailures 返回持久化连续失败次数
func (e *Engine) PersistFailures() uint64 {
	return e.persistFailures.Load()
}

// GetEnvoyNodes 返回所有已连接的 Envoy 节点信息
// 仅返回持有活跃 gRPC watch 的节点（watches > 0 || delta_watches > 0）
func (e *Engine) GetEnvoyNodes() []EnvoyNodeInfo {
	var nodes []EnvoyNodeInfo
	for _, key := range e.snapCache.GetStatusKeys() {
		info := e.snapCache.GetStatusInfo(key)
		if info == nil {
			continue
		}
		// 没有活跃 watch 说明 Envoy 已断开，跳过
		if info.GetNumWatches() == 0 && info.GetNumDeltaWatches() == 0 {
			continue
		}
		entry := EnvoyNodeInfo{
			NodeID:           key,
			Watches:          info.GetNumWatches(),
			DeltaWatches:     info.GetNumDeltaWatches(),
			LastRequestTime:  info.GetLastWatchRequestTime().Format(time.RFC3339),
			LastDeltaReqTime: info.GetLastDeltaWatchRequestTime().Format(time.RFC3339),
		}
		if node := info.GetNode(); node != nil {
			entry.ID = node.GetId()
			entry.Cluster = node.GetCluster()
			entry.UserAgent = node.GetUserAgentName()
			entry.Version = node.GetUserAgentVersion()
		}
		nodes = append(nodes, entry)
	}
	return nodes
}
