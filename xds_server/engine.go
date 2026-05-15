package xdsServer

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
	"time"

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
	endpoint interface{ ProtoMessage() } // EDS（UDP 时为 nil）
	cluster  interface{ ProtoMessage() } // CDS
	route    interface{ ProtoMessage() } // RDS（UDP 时为 nil）
	listener interface{ ProtoMessage() } // LDS
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
	resCache        map[string]*ruleRes
	lastContentHash string
	versionSeq      uint64
	mu              sync.RWMutex // 保护 rules
	pushMu          sync.Mutex   // 串行化「修改 + 推送」
	onRulesChanged  func()
}

// NewEngine 创建 xDS 引擎
func NewEngine(nodeID string) *Engine {
	return &Engine{
		nodeID:    nodeID,
		snapCache: cache.NewSnapshotCache(false, cache.IDHash{}, nil),
		rules:     make(map[string]*ProxyRule),
		resCache:  make(map[string]*ruleRes),
	}
}

// SetOnRulesChanged 设置规则变更后的持久化回调
func (e *Engine) SetOnRulesChanged(fn func()) {
	e.onRulesChanged = fn
}

// ─── gRPC 服务器 ───────────────────────────────────────────────────────

// NewGRPCServer 创建 xDS gRPC 服务器（不启动）
func (e *Engine) NewGRPCServer() *grpc.Server {
	srv := server.NewServer(context.Background(), e.snapCache, nil)
	gs := grpc.NewServer()
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
		return fmt.Errorf("listen gRPC: %w", err)
	}
	log.Printf("xDS gRPC on %s", addr)
	return gs.Serve(lis)
}

// ─── CRUD 操作 ─────────────────────────────────────────────────────────

// checkNameConflict 检查规则名是否被其他规则占用
// 调用方必须持有 e.mu
func (e *Engine) checkNameConflict(rule *ProxyRule) *ValidationError {
	for _, r := range e.rules {
		if r.ID == rule.ID {
			continue
		}
		if r.Name == rule.Name {
			return &ValidationError{
				Msg: fmt.Sprintf("规则名 %q 已被规则 %s 占用", rule.Name, r.ID),
			}
		}
	}
	return nil
}

// checkPortConflict 检查 listen_addr:listen_port + protocol 是否被其他规则占用
// 调用方必须持有 e.mu
func (e *Engine) checkPortConflict(rule *ProxyRule) *ValidationError {
	for _, r := range e.rules {
		if r.ID == rule.ID {
			continue
		}
		if r.ListenAddr == rule.ListenAddr && r.ListenPort == rule.ListenPort && r.Protocol == rule.Protocol {
			return &ValidationError{
				Msg: fmt.Sprintf("%s://%s:%d 已被规则 %s 占用",
					rule.Protocol, rule.ListenAddr, rule.ListenPort, r.ID),
			}
		}
	}
	return nil
}

// SetRules 批量加载规则（用于启动时从文件加载，不触发推送）
func (e *Engine) SetRules(list []*ProxyRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range list {
		if err := e.checkNameConflict(r); err != nil {
			log.Printf("⚠️  跳过名称冲突规则 %s: %v", r.ID, err)
			continue
		}
		if err := e.checkPortConflict(r); err != nil {
			log.Printf("⚠️  跳过端口冲突规则 %s: %v", r.ID, err)
			continue
		}
		e.rules[r.ID] = r
	}
}

// CreateRule 创建规则并推送到 Envoy
// 流程：校验 → 生成ID → 加锁 → 端口冲突检测 → 写入内存 → 推送快照 → 失败则回滚
func (e *Engine) CreateRule(rule *ProxyRule) (*ProxyRule, error) {
	if err := ValidateRule(rule); err != nil {
		return nil, err
	}
	rule.ID = GenerateID()

	e.pushMu.Lock()
	defer e.pushMu.Unlock()

	e.mu.Lock()
	if err := e.checkNameConflict(rule); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if err := e.checkPortConflict(rule); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.rules[rule.ID] = rule
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		delete(e.rules, rule.ID)
		e.mu.Unlock()
		return nil, fmt.Errorf("push snapshot: %w", err)
	}

	log.Printf("➕ Rule created: %s (%s)  %s:%d → %d backends [%s]",
		rule.ID, rule.Name, rule.ListenAddr, rule.ListenPort, len(rule.Backends), rule.LBPolicy)
	return rule, nil
}

// UpdateRule 更新规则并推送到 Envoy
func (e *Engine) UpdateRule(id string, rule *ProxyRule) (*ProxyRule, error) {
	if err := ValidateRule(rule); err != nil {
		return nil, err
	}

	e.pushMu.Lock()
	defer e.pushMu.Unlock()

	e.mu.Lock()
	oldRule, existed := e.rules[id]
	if !existed {
		e.mu.Unlock()
		return nil, ErrRuleNotFound
	}
	rule.ID = id
	if err := e.checkNameConflict(rule); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if err := e.checkPortConflict(rule); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.rules[id] = rule
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		if _, stillThere := e.rules[id]; stillThere {
			e.rules[id] = oldRule
		}
		e.mu.Unlock()
		return nil, fmt.Errorf("push snapshot: %w", err)
	}

	e.mu.RLock()
	_, stillThere := e.rules[id]
	e.mu.RUnlock()
	if !stillThere {
		return nil, ErrRuleNotFound
	}

	log.Printf("✏️  Rule updated: %s (%s)  %s:%d → %d backends [%s]",
		rule.ID, rule.Name, rule.ListenAddr, rule.ListenPort, len(rule.Backends), rule.LBPolicy)
	return rule, nil
}

// DeleteRule 删除规则并推送到 Envoy
func (e *Engine) DeleteRule(id string) error {
	e.pushMu.Lock()
	defer e.pushMu.Unlock()

	e.mu.Lock()
	oldRule, existed := e.rules[id]
	if !existed {
		e.mu.Unlock()
		return ErrRuleNotFound
	}
	delete(e.rules, id)
	e.mu.Unlock()

	if err := e.pushSnapshotLocked(); err != nil {
		e.mu.Lock()
		e.rules[id] = oldRule
		e.mu.Unlock()
		return fmt.Errorf("push snapshot: %w", err)
	}

	log.Printf("➖ Rule deleted: %s", id)
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

// GetEnvoyNodes 返回所有已连接的 Envoy 节点信息
func (e *Engine) GetEnvoyNodes() []map[string]any {
	var nodes []map[string]any
	for _, key := range e.snapCache.GetStatusKeys() {
		info := e.snapCache.GetStatusInfo(key)
		if info == nil {
			continue
		}
		node := info.GetNode()
		entry := map[string]any{
			"node_id":             key,
			"watches":             info.GetNumWatches(),
			"delta_watches":       info.GetNumDeltaWatches(),
			"last_request_time":   info.GetLastWatchRequestTime().Format(time.RFC3339),
			"last_delta_req_time": info.GetLastDeltaWatchRequestTime().Format(time.RFC3339),
		}
		if node != nil {
			entry["id"] = node.GetId()
			entry["cluster"] = node.GetCluster()
			entry["user_agent"] = node.GetUserAgentName()
			entry["version"] = node.GetUserAgentVersion()
			entry["listening_addresses"] = node.GetListeningAddresses()
		}
		nodes = append(nodes, entry)
	}
	return nodes
}
