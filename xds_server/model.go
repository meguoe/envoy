package xdsServer

// model.go —— 数据模型与常量
//
// ProxyRule 和 BackendNode 是 xDS 配置数据，
// 定义在 xds 包中，HTTP API 层和持久化层通过 import xds 包使用

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// ─── ID 生成 ───────────────────────────────────────────────────────────

// GenerateID 生成 16 字符的随机 hex ID
func GenerateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ─── 协议常量 ──────────────────────────────────────────────────────────

const (
	ProtocolHTTP = "http"
	ProtocolUDP  = "udp"
)

var validProtocols = map[string]bool{
	ProtocolHTTP: true,
	ProtocolUDP:  true,
}

// ─── 负载均衡策略 ──────────────────────────────────────────────────────

var validLBPolicies = map[string]bool{
	"ROUND_ROBIN":   true,
	"LEAST_REQUEST": true,
	"RANDOM":        true,
	"RING_HASH":     true,
}

// ─── 数据模型 ──────────────────────────────────────────────────────────

// BackendNode 代表一个后端服务节点
type BackendNode struct {
	Address string `json:"address"`
	Port    uint32 `json:"port"`
	Weight  uint32 `json:"weight,omitempty"`
}

// ProxyRule 定义一条完整的代理规则
type ProxyRule struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Protocol   string        `json:"protocol"`
	ListenAddr string        `json:"listen_addr"`
	ListenPort uint32        `json:"listen_port"`
	Backends   []BackendNode `json:"backends"`
	LBPolicy   string        `json:"lb_policy"`
}

// ─── 校验 ──────────────────────────────────────────────────────────────

// ValidateRule 校验规则，不修改输入参数
// 返回 ValidationError 表示校验失败
//
// 注意: ListenPort == 0 会被拦截。对于 UpdateRule 场景，
// 调用方应在 ValidateRule 之前将未传字段合并旧值（如 handleUpdate），
// 避免 0 值被误判为"未传"而非"非法"。
func ValidateRule(rule *ProxyRule) error {
	if rule.Name == "" {
		return &ValidationError{Msg: "name 不能为空"}
	}
	if rule.ListenAddr == "" {
		return &ValidationError{Msg: "listen_addr 不能为空"}
	}
	if rule.ListenPort == 0 {
		return &ValidationError{Msg: "listen_port 不能为空"}
	}
	if rule.ListenPort > 65535 {
		return &ValidationError{Msg: "listen_port 超出范围 (1-65535)"}
	}
	if len(rule.Backends) == 0 {
		return &ValidationError{Msg: "backends 不能为空，至少需要一个后端节点"}
	}

	protocol := strings.ToLower(rule.Protocol)
	if rule.Protocol != "" && !validProtocols[protocol] {
		return &ValidationError{Msg: "protocol 无效，可选值: http, udp"}
	}

	lbPolicy := strings.ToUpper(rule.LBPolicy)
	if rule.LBPolicy != "" && !validLBPolicies[lbPolicy] {
		return &ValidationError{Msg: "lb_policy 无效，可选值: ROUND_ROBIN, LEAST_REQUEST, RANDOM, RING_HASH"}
	}

	for i, b := range rule.Backends {
		if b.Address == "" || b.Port == 0 {
			return &ValidationError{Msg: fmt.Sprintf("backends[%d]: address 和 port 不能为空", i)}
		}
	}

	return nil
}

// NormalizeRule 规范化规则字段（设置默认值、统一大小写）
// 调用方应确保规则已通过 ValidateRule 校验
func NormalizeRule(rule *ProxyRule) {
	if rule.Protocol == "" {
		rule.Protocol = ProtocolHTTP
	}
	rule.Protocol = strings.ToLower(rule.Protocol)

	if rule.LBPolicy == "" {
		rule.LBPolicy = "ROUND_ROBIN"
	}
	rule.LBPolicy = strings.ToUpper(rule.LBPolicy)

	for i := range rule.Backends {
		if rule.Backends[i].Weight == 0 {
			rule.Backends[i].Weight = 1
		}
	}
}
