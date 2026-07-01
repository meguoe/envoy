package xdsserver

// engine.go —— xDS 引擎，对外接口
//
// Engine 封装了所有 xDS 状态，外部通过 Engine 方法操作规则。
// 内部自动管理：增量缓存、快照推送。
// 规则持久化由 PostgreSQL 负责，引擎只负责规则加载和 xDS 快照推送。
//
// 锁策略：
//   - mu        保护 rules map 的读写
//   - pushMu    串行化「规则修改 + 快照推送」，保证原子性
//   - ReplaceRulesAndPush* 方法在 pushMu 内先修改 rules，再推送快照，失败则恢复旧规则
//   - ListRules / GetRule 只读，不持有 pushMu，可以与推送并发

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
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
	generation uint64         // 构建时的 rulesGen
	endpoint   types.Resource // EDS
	cluster    types.Resource // CDS
	route      types.Resource // RDS
	listener   types.Resource // LDS
}

// ValidationError 业务校验错误，区别于系统错误（如推送失败）
type ValidationError struct {
	Msg string
}

// Error 返回校验错误的描述信息。
func (e *ValidationError) Error() string { return e.Msg }

// ErrRuleNotFound 规则不存在错误
var ErrRuleNotFound = errors.New("rule not found")

// Engine xDS 引擎，封装所有 xDS 状态
type Engine struct {
	nodeID         string
	snapCache      cache.SnapshotCache
	rules          map[string]*ProxyRule
	resCache       map[string]*ruleRes // 注意: 仅在 pushMu 保护下读写, 不可并发访问
	versionSeq     uint64
	rulesGen       uint64       // 每次 setRulesNoPush 递增，用于检测缓存是否过期
	mu             sync.RWMutex // 保护 rules
	pushMu         sync.Mutex   // 串行化「修改 + 推送」
	connectTimeout time.Duration
	udpIdleTimeout time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	callbacks      server.Callbacks

	knownRevision    atomic.Int64 // Engine 已知的 DB revision
	deployedRevision atomic.Int64 // Envoy 已 ACK 的 revision

	// 冲突检测索引（在 pushMu + mu 保护下维护）
	nameIndex map[string]string // ruleName → ruleID
	portIndex map[string]string // "addr:port:proto" → ruleID

	// 上一次快照的资源名集合，用于 diff 计算变更资源
	prevSnapshot map[resourcev3.Type]map[string]bool
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

// SetCallbacks 设置 xDS 回调（ACK/NACK 追踪）
// 必须在 NewGRPCServer 之前调用
func (e *Engine) SetCallbacks(cb server.Callbacks) {
	e.callbacks = cb
}

// ─── gRPC 服务器 ───────────────────────────────────────────────────────

// NewGRPCServer 创建 xDS gRPC 服务器（不启动）
// opts 可选的 gRPC 服务器选项（如 TLS 凭证）
func (e *Engine) NewGRPCServer(opts ...grpc.ServerOption) *grpc.Server {
	opts = append(opts,
		grpc.UnaryInterceptor(UnaryServerInterceptor()),
		grpc.StreamInterceptor(StreamServerInterceptor()),
	)
	srv := server.NewServer(e.ctx, e.snapCache, e.callbacks)
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

// CheckRulesConflicts 检查规则列表内部的冲突（ID重复、名称冲突、端口冲突）
// 不修改引擎状态，也不修改输入规则，仅做验证
func CheckRulesConflicts(list []*ProxyRule) error {
	nameIndex := make(map[string]string, len(list))
	portIndex := make(map[string]string, len(list))
	seenIDs := make(map[string]struct{}, len(list))

	for _, r := range list {
		if err := ValidateRule(r); err != nil {
			return err
		}
		ruleName := r.Name
		ruleProtocol := r.Protocol
		if ruleProtocol == "" {
			ruleProtocol = ProtocolHTTP
		}
		ruleProtocol = strings.ToLower(ruleProtocol)
		if r.ID == "" {
			return &ValidationError{Msg: fmt.Sprintf("规则 ID 为空: %s", r.Name)}
		}
		if _, ok := seenIDs[r.ID]; ok {
			return &ValidationError{Msg: fmt.Sprintf("规则 ID 重复: %s", r.ID)}
		}
		if existingID, ok := nameIndex[ruleName]; ok {
			return &ValidationError{Msg: fmt.Sprintf("规则名 %q 已被规则 %s 占用", ruleName, existingID)}
		}
		key := r.ListenAddr + ":" + fmt.Sprintf("%d", r.ListenPort) + ":" + ruleProtocol
		if existingID, ok := portIndex[key]; ok {
			return &ValidationError{Msg: fmt.Sprintf("%s://%s:%d 已被规则 %s 占用", ruleProtocol, r.ListenAddr, r.ListenPort, existingID)}
		}
		nameIndex[ruleName] = r.ID
		portIndex[key] = r.ID
		seenIDs[r.ID] = struct{}{}
	}
	return nil
}

// SetRules 批量加载规则（用于启动时从数据库加载，不触发推送）
func (e *Engine) SetRules(list []*ProxyRule) {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()
	e.setRulesNoPush(list)
}

// setRulesNoPush 内部方法：用新规则列表替换当前规则，不触发快照推送。
func (e *Engine) setRulesNoPush(list []*ProxyRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rulesGen++
	e.rules = make(map[string]*ProxyRule, len(list))
	e.resCache = make(map[string]*ruleRes)
	e.nameIndex = make(map[string]string, len(list))
	e.portIndex = make(map[string]string, len(list))
	seenIDs := make(map[string]struct{}, len(list))
	for _, r := range list {
		if err := ValidateRule(r); err != nil {
			slog.Warn("跳过非法规则", "rule_id", r.ID, "error", err)
			continue
		}
		cp := *r
		if r.Backends != nil {
			cp.Backends = make([]BackendNode, len(r.Backends))
			copy(cp.Backends, r.Backends)
		}
		NormalizeRule(&cp)
		if cp.ID == "" {
			slog.Warn("跳过 ID 为空的规则", "name", cp.Name)
			continue
		}
		if _, ok := seenIDs[cp.ID]; ok {
			slog.Warn("跳过 ID 重复规则", "rule_id", cp.ID, "name", cp.Name)
			continue
		}
		if _, ok := e.rules[cp.ID]; ok {
			slog.Warn("跳过 ID 冲突规则", "rule_id", cp.ID, "name", cp.Name)
			continue
		}
		if err := e.checkNameConflict(&cp); err != nil {
			slog.Warn("跳过名称冲突规则", "rule_id", cp.ID, "error", err)
			continue
		}
		if err := e.checkPortConflict(&cp); err != nil {
			slog.Warn("跳过端口冲突规则", "rule_id", cp.ID, "error", err)
			continue
		}
		e.rules[cp.ID] = &cp
		e.addIndexes(&cp)
		seenIDs[cp.ID] = struct{}{}
	}
}

// ReplaceRulesAndPush 从数据库重新加载规则并推送快照，不触发持久化回写。
func (e *Engine) ReplaceRulesAndPush(list []*ProxyRule) error {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()
	oldRules := e.ListRules()
	e.setRulesNoPush(list)
	if err := e.pushSnapshotLocked(); err != nil {
		e.setRulesNoPush(oldRules)
		return fmt.Errorf("推送快照失败: %w", err)
	}
	return nil
}

// ReplaceRulesAndPushWithVersion 从数据库重新加载规则并推送快照，使用指定 revision 作为版本号。
func (e *Engine) ReplaceRulesAndPushWithVersion(list []*ProxyRule, revision int64) error {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()
	oldRules := e.ListRules()
	e.setRulesNoPush(list)
	if err := e.pushSnapshotLockedWithVersion(fmt.Sprintf("%d", revision)); err != nil {
		e.setRulesNoPush(oldRules)
		return fmt.Errorf("推送快照失败: %w", err)
	}
	e.knownRevision.Store(revision)
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

// KnownRevision 返回 Engine 已知的 DB revision
func (e *Engine) KnownRevision() int64 {
	return e.knownRevision.Load()
}

// LastDeployedRevision 返回 Envoy 已 ACK 的 revision
func (e *Engine) LastDeployedRevision() int64 {
	return e.deployedRevision.Load()
}

// SetDeployedRevision 设置已部署的 revision
func (e *Engine) SetDeployedRevision(rev int64) {
	e.deployedRevision.Store(rev)
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
