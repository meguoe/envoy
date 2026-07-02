package xdsserver

// model.go —— 数据模型与常量
//
// ProxyRule 和 BackendNode 是 xDS 配置数据，
// 定义在 xds 包中，HTTP API 层和持久化层通过 import xds 包使用

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"regexp"
	"strings"
)

// ─── ID 生成 ───────────────────────────────────────────────────────────

// GenerateID 生成 16 字符的随机 hex ID
func GenerateID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("生成规则 ID 失败: %w", err)
	}
	return hex.EncodeToString(b), nil
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

var validNameChars = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$`)

var validDNSName = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$`)

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

// ─── Envoy 节点信息 ────────────────────────────────────────────────────

// EnvoyNodeInfo 表示一个已连接的 Envoy 节点
type EnvoyNodeInfo struct {
	NodeID           string `json:"node_id"`
	ID               string `json:"id,omitempty"`
	Cluster          string `json:"cluster,omitempty"`
	UserAgent        string `json:"user_agent,omitempty"`
	Version          string `json:"version,omitempty"`
	Watches          int    `json:"watches"`
	DeltaWatches     int    `json:"delta_watches"`
	LastRequestTime  string `json:"last_request_time"`
	LastDeltaReqTime string `json:"last_delta_req_time"`
}

// ─── 校验 ──────────────────────────────────────────────────────────────

// ValidateRule 校验规则，不修改输入参数
// 返回 ValidationError 表示校验失败
//
// 注意: ListenPort 校验范围为 10-65535。
// 对于 UpdateRule 场景，调用方应先处理 ListenPort==0 的继承逻辑再调用 ValidateRule，
// 或在 HTTP 层对 ListenPort 做前置校验（如 handleUpdate）。
func ValidateRule(rule *ProxyRule) error {
	if rule.Name == "" {
		return &ValidationError{Msg: "name 不能为空"}
	}
	if !validNameChars.MatchString(rule.Name) {
		return &ValidationError{Msg: "name 仅允许字母、数字、下划线、短横线，且首尾必须为字母或数字"}
	}
	if rule.ListenAddr == "" {
		return &ValidationError{Msg: "listen_addr 不能为空"}
	}
	if err := validateAddress(rule.ListenAddr); err != nil {
		return &ValidationError{Msg: fmt.Sprintf("listen_addr: %v", err)}
	}
	if rule.ListenPort < 10 || rule.ListenPort > 65535 {
		return &ValidationError{Msg: "listen_port 超出范围 (10-65535)"}
	}
	if len(rule.Backends) == 0 {
		return &ValidationError{Msg: "backends 不能为空，至少需要一个后端节点"}
	}

	protocol := strings.ToLower(rule.Protocol)
	if protocol != "" && !validProtocols[protocol] {
		return &ValidationError{Msg: "protocol 无效，可选值: http, udp"}
	}

	lbPolicy := strings.ToUpper(rule.LBPolicy)
	if lbPolicy != "" && !validLBPolicies[lbPolicy] {
		return &ValidationError{Msg: "lb_policy 无效，可选值: ROUND_ROBIN, LEAST_REQUEST, RANDOM, RING_HASH"}
	}

	for i, b := range rule.Backends {
		if b.Address == "" || b.Port == 0 {
			return &ValidationError{Msg: fmt.Sprintf("backends[%d]: address 和 port 不能为空", i)}
		}
		if b.Port > 65535 {
			return &ValidationError{Msg: fmt.Sprintf("backends[%d].port 超出范围 (1-65535)", i)}
		}
		if err := validateAddress(b.Address); err != nil {
			return &ValidationError{Msg: fmt.Sprintf("backends[%d].address: %v", i, err)}
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

// validateAddress 验证地址是合法 IP 或合法 DNS 名称
func validateAddress(addr string) error {
	if net.ParseIP(addr) != nil {
		return nil
	}
	if !validDNSName.MatchString(addr) {
		return fmt.Errorf("地址格式无效 %q: 需要合法 IP 或 DNS 名称", addr)
	}
	if strings.Contains(addr, "..") {
		return fmt.Errorf("地址格式无效 %q: 不能包含连续的点", addr)
	}
	if strings.HasPrefix(addr, ".") || strings.HasSuffix(addr, ".") {
		return fmt.Errorf("地址格式无效 %q: 不能以点开头或结尾", addr)
	}
	return nil
}
