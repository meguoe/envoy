package xdsserver

import (
	"testing"
)

// TestValidateRuleValid 测试合法规则通过校验。
func TestValidateRuleValid(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		Protocol:   "http",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy:   "ROUND_ROBIN",
	}
	if err := ValidateRule(rule); err != nil {
		t.Errorf("valid rule should pass: %v", err)
	}
}

// TestValidateRuleEmptyName 测试空名称的规则校验失败。
func TestValidateRuleEmptyName(t *testing.T) {
	rule := &ProxyRule{
		Name:       "",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for empty name")
	}
}

// TestValidateRuleInvalidName 测试包含特殊字符的非法名称被拒绝。
func TestValidateRuleInvalidName(t *testing.T) {
	tests := []struct {
		name string
		rule *ProxyRule
	}{
		{"special chars", &ProxyRule{Name: "web/api", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"spaces", &ProxyRule{Name: "web app", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"starts with dash", &ProxyRule{Name: "-web", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"ends with dash", &ProxyRule{Name: "web-", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"colon", &ProxyRule{Name: "web:8080", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"dot", &ProxyRule{Name: "web.app", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRule(tt.rule); err == nil {
				t.Errorf("name %q should be invalid", tt.rule.Name)
			}
		})
	}
}

// TestValidateRuleValidName 测试合法名称（连字符、下划线等）通过校验。
func TestValidateRuleValidName(t *testing.T) {
	tests := []struct {
		name string
		rule *ProxyRule
	}{
		{"simple", &ProxyRule{Name: "web", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"underscore", &ProxyRule{Name: "web_app", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"hyphen", &ProxyRule{Name: "web-app", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
		{"mixed", &ProxyRule{Name: "my-web_app-1", ListenAddr: "0.0.0.0", ListenPort: 9981, Backends: []BackendNode{{Address: "127.0.0.1", Port: 8080}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateRule(tt.rule); err != nil {
				t.Errorf("name %q should be valid: %v", tt.rule.Name, err)
			}
		})
	}
}

// TestValidateRuleEmptyListenAddr 测试空监听地址的规则校验失败。
func TestValidateRuleEmptyListenAddr(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for empty listen_addr")
	}
}

// TestValidateRulePortTooLow 测试端口号过小（小于10）的规则校验失败。
func TestValidateRulePortTooLow(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 5,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for port < 10")
	}
}

// TestValidateRulePortTooHigh 测试端口号过大（大于65535）的规则校验失败。
func TestValidateRulePortTooHigh(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 65536,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for port > 65535")
	}
}

// TestValidateRulePortBoundary 测试端口边界值（10和65535）校验通过。
func TestValidateRulePortBoundary(t *testing.T) {
	for _, port := range []uint32{10, 65535} {
		rule := &ProxyRule{
			Name:       "web",
			ListenAddr: "0.0.0.0",
			ListenPort: port,
			Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		}
		if err := ValidateRule(rule); err != nil {
			t.Errorf("port %d should be valid: %v", port, err)
		}
	}
}

// TestValidateRuleEmptyBackends 测试无后端节点的规则校验失败。
func TestValidateRuleEmptyBackends(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   nil,
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for empty backends")
	}
}

// TestValidateRuleInvalidProtocol 测试非法协议类型的规则校验失败。
func TestValidateRuleInvalidProtocol(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		Protocol:   "grpc",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for invalid protocol")
	}
}

// TestValidateRuleInvalidLBPolicy 测试非法负载均衡策略的规则校验失败。
func TestValidateRuleInvalidLBPolicy(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy:   "INVALID",
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for invalid lb_policy")
	}
}

// TestValidateRuleBackendNoAddress 测试后端地址为空的规则校验失败。
func TestValidateRuleBackendNoAddress(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for backend with empty address")
	}
}

// TestValidateRuleBackendNoPort 测试后端端口为零的规则校验失败。
func TestValidateRuleBackendNoPort(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 0, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for backend with port 0")
	}
}

// TestValidateRuleBackendPortTooHigh 测试后端端口号过大（大于65535）的规则校验失败。
func TestValidateRuleBackendPortTooHigh(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 65536, Weight: 1}},
	}
	if err := ValidateRule(rule); err == nil {
		t.Error("expected error for backend port > 65535")
	}
}

// TestValidateRuleEmptyProtocolAllowed 测试空协议字段允许通过校验。
func TestValidateRuleEmptyProtocolAllowed(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err != nil {
		t.Errorf("empty protocol should be allowed: %v", err)
	}
}

// TestValidateRuleEmptyLBPolicyAllowed 测试空负载均衡策略字段允许通过校验。
func TestValidateRuleEmptyLBPolicyAllowed(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	if err := ValidateRule(rule); err != nil {
		t.Errorf("empty lb_policy should be allowed: %v", err)
	}
}

// TestValidateRuleUDPProtocol 测试 UDP 协议规则通过校验。
func TestValidateRuleUDPProtocol(t *testing.T) {
	rule := &ProxyRule{
		Name:       "dns",
		Protocol:   "udp",
		ListenAddr: "0.0.0.0",
		ListenPort: 5353,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 53, Weight: 1}},
	}
	if err := ValidateRule(rule); err != nil {
		t.Errorf("udp protocol should be valid: %v", err)
	}
}

// TestValidateRuleAllLBPolicies 测试所有合法负载均衡策略均通过校验。
func TestValidateRuleAllLBPolicies(t *testing.T) {
	for _, policy := range []string{"ROUND_ROBIN", "LEAST_REQUEST", "RANDOM", "RING_HASH"} {
		rule := &ProxyRule{
			Name:       "web",
			ListenAddr: "0.0.0.0",
			ListenPort: 9981,
			Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
			LBPolicy:   policy,
		}
		if err := ValidateRule(rule); err != nil {
			t.Errorf("lb_policy %q should be valid: %v", policy, err)
		}
	}
}

// TestNormalizeRuleDefaults 测试规则标准化时填充默认值（协议、策略、权重）。
func TestNormalizeRuleDefaults(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 0}},
	}
	NormalizeRule(rule)
	if rule.Protocol != "http" {
		t.Errorf("default protocol = %q, want http", rule.Protocol)
	}
	if rule.LBPolicy != "ROUND_ROBIN" {
		t.Errorf("default lb_policy = %q, want ROUND_ROBIN", rule.LBPolicy)
	}
	if rule.Backends[0].Weight != 1 {
		t.Errorf("default weight = %d, want 1", rule.Backends[0].Weight)
	}
}

// TestNormalizeRuleProtocolLowercase 测试协议字段被标准化为小写。
func TestNormalizeRuleProtocolLowercase(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		Protocol:   "HTTP",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
	}
	NormalizeRule(rule)
	if rule.Protocol != "http" {
		t.Errorf("protocol = %q, want http (lowercase)", rule.Protocol)
	}
}

// TestNormalizeRuleLBPolicyUppercase 测试负载均衡策略被标准化为大写。
func TestNormalizeRuleLBPolicyUppercase(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 1}},
		LBPolicy:   "least_request",
	}
	NormalizeRule(rule)
	if rule.LBPolicy != "LEAST_REQUEST" {
		t.Errorf("lb_policy = %q, want LEAST_REQUEST (uppercase)", rule.LBPolicy)
	}
}

// TestNormalizeRuleWeightPreserved 测试非零权重在标准化后保持不变。
func TestNormalizeRuleWeightPreserved(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080, Weight: 5}},
	}
	NormalizeRule(rule)
	if rule.Backends[0].Weight != 5 {
		t.Errorf("weight = %d, want 5 (non-zero preserved)", rule.Backends[0].Weight)
	}
}

// TestNormalizeRuleMultipleBackends 测试多后端节点权重标准化，零权重被设为1。
func TestNormalizeRuleMultipleBackends(t *testing.T) {
	rule := &ProxyRule{
		Name:       "web",
		ListenAddr: "0.0.0.0",
		ListenPort: 9981,
		Backends: []BackendNode{
			{Address: "127.0.0.1", Port: 8080, Weight: 0},
			{Address: "127.0.0.1", Port: 8081, Weight: 3},
			{Address: "127.0.0.1", Port: 8082, Weight: 0},
		},
	}
	NormalizeRule(rule)
	if rule.Backends[0].Weight != 1 {
		t.Errorf("backend 0 weight = %d, want 1", rule.Backends[0].Weight)
	}
	if rule.Backends[1].Weight != 3 {
		t.Errorf("backend 1 weight = %d, want 3 (preserved)", rule.Backends[1].Weight)
	}
	if rule.Backends[2].Weight != 1 {
		t.Errorf("backend 2 weight = %d, want 1", rule.Backends[2].Weight)
	}
}

// TestGenerateIDLength 测试生成的 ID 长度为16。
func TestGenerateIDLength(t *testing.T) {
	id, err := GenerateID()
	if err != nil {
		t.Fatalf("GenerateID: %v", err)
	}
	if len(id) != 16 {
		t.Errorf("GenerateID() length = %d, want 16", len(id))
	}
}

// TestGenerateIDUniqueness 测试连续生成100个 ID 均不重复。
func TestGenerateIDUniqueness(t *testing.T) {
	seen := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		id, err := GenerateID()
		if err != nil {
			t.Fatalf("GenerateID: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		seen[id] = true
	}
}

// TestValidateRuleInvalidListenAddr 测试非法监听地址格式被拒绝。
func TestValidateRuleInvalidListenAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{"spaces", "0 . 0 . 0"},
		{"special chars", "0.0.0.0:80"},
		{"empty label", "192.168..1"},
		{"leading dash", "-host.example.com"},
		{"trailing dot only", "."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ProxyRule{
				Name:       "web",
				ListenAddr: tt.addr,
				ListenPort: 9981,
				Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080}},
			}
			if err := ValidateRule(rule); err == nil {
				t.Errorf("listen_addr %q should be invalid", tt.addr)
			}
		})
	}
}

// TestValidateRuleValidListenAddr 测试合法监听地址（IPv4/IPv6/域名）通过校验。
func TestValidateRuleValidListenAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{"ipv4", "0.0.0.0"},
		{"ipv6", "::"},
		{"ipv6 loopback", "::1"},
		{"dns", "example.com"},
		{"dns subdomain", "api.internal.example.com"},
		{"localhost", "localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ProxyRule{
				Name:       "web",
				ListenAddr: tt.addr,
				ListenPort: 9981,
				Backends:   []BackendNode{{Address: "127.0.0.1", Port: 8080}},
			}
			if err := ValidateRule(rule); err != nil {
				t.Errorf("listen_addr %q should be valid: %v", tt.addr, err)
			}
		})
	}
}

// TestValidateRuleInvalidBackendAddress 测试非法后端地址格式被拒绝。
func TestValidateRuleInvalidBackendAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"spaces", "127. 0.0.1"},
		{"special chars", "host:name"},
		{"leading dash", "-host"},
		{"trailing dash", "host-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ProxyRule{
				Name:       "web",
				ListenAddr: "0.0.0.0",
				ListenPort: 9981,
				Backends:   []BackendNode{{Address: tt.address, Port: 8080}},
			}
			if err := ValidateRule(rule); err == nil {
				t.Errorf("backend address %q should be invalid", tt.address)
			}
		})
	}
}

// TestValidateRuleValidBackendAddress 测试合法后端地址（IPv4/IPv6/域名）通过校验。
func TestValidateRuleValidBackendAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"ipv4", "127.0.0.1"},
		{"ipv6", "::1"},
		{"dns", "backend.example.com"},
		{"dns subdomain", "api.internal.example.com"},
		{"localhost", "localhost"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ProxyRule{
				Name:       "web",
				ListenAddr: "0.0.0.0",
				ListenPort: 9981,
				Backends:   []BackendNode{{Address: tt.address, Port: 8080}},
			}
			if err := ValidateRule(rule); err != nil {
				t.Errorf("backend address %q should be valid: %v", tt.address, err)
			}
		})
	}
}

// TestValidateAddress 测试地址校验函数对合法和非法地址的判断。
func TestValidateAddress(t *testing.T) {
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{"127.0.0.1", false},
		{"::1", false},
		{"0.0.0.0", false},
		{"example.com", false},
		{"api.internal.example.com", false},
		{"localhost", false},
		{"", true},
		{"0 . 0 . 0", true},
		{"-host", true},
		{"host-", true},
		{"host:name", true},
		{"host name", true},
		{".", true},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			err := validateAddress(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateAddress(%q) error = %v, wantErr %v", tt.addr, err, tt.wantErr)
			}
		})
	}
}
