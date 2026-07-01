package config

import (
	"os"
	"testing"
)

// testConfig 返回用于单元测试的默认配置实例。
func testConfig() Config {
	cfg := Config{}
	cfg.Server.NodeID = "envoy-local"
	cfg.Server.GRPCAddr = "127.0.0.1:18001"
	cfg.Server.APIAddr = "127.0.0.1:18000"
	cfg.Server.LogLevel = "INFO"
	cfg.API.MaxBodyBytes = 1 << 20
	return cfg
}

// TestLoadDefaults 测试配置文件加载时默认值的正确性。
func TestLoadDefaults(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.MkdirAll("certs/mtls", 0755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"certs/mtls/ca.crt", "certs/mtls/server.crt", "certs/mtls/server.key", "certs/mtls/client.crt", "certs/mtls/client.key"} {
		os.WriteFile(f, []byte("dummy"), 0644)
	}
	if err := os.MkdirAll("certs/https", 0755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"certs/https/server.crt", "certs/https/server.key"} {
		os.WriteFile(f, []byte("dummy"), 0644)
	}
	if err := os.WriteFile("config.yaml", []byte(`server:
  node_id: test-node`), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load("config.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.NodeID != "test-node" {
		t.Errorf("node_id = %q, want test-node", cfg.Server.NodeID)
	}
	if cfg.Server.GRPCAddr != "127.0.0.1:18001" {
		t.Errorf("default grpc_addr = %q, want 127.0.0.1:18001", cfg.Server.GRPCAddr)
	}
	if cfg.Server.APIAddr != "127.0.0.1:18000" {
		t.Errorf("default api_addr = %q, want 127.0.0.1:18000", cfg.Server.APIAddr)
	}
	if cfg.Server.LogLevel != "INFO" {
		t.Errorf("default log_level = %q, want INFO", cfg.Server.LogLevel)
	}
	if cfg.API.MaxBodyBytes != 5<<20 {
		t.Errorf("default max_body_bytes = %d, want %d", cfg.API.MaxBodyBytes, 5<<20)
	}
	if cfg.API.Timeout.ReadHeaderTimeout != 5e9 {
		t.Errorf("default read_header_timeout = %v, want 5s", cfg.API.Timeout.ReadHeaderTimeout)
	}
	if cfg.API.Timeout.ReadTimeout != 10e9 {
		t.Errorf("default read_timeout = %v, want 10s", cfg.API.Timeout.ReadTimeout)
	}
	if cfg.API.Timeout.WriteTimeout != 10e9 {
		t.Errorf("default write_timeout = %v, want 10s", cfg.API.Timeout.WriteTimeout)
	}
	if cfg.API.Timeout.IdleTimeout != 60e9 {
		t.Errorf("default idle_timeout = %v, want 60s", cfg.API.Timeout.IdleTimeout)
	}
}

// TestValidateEmptyNodeID 测试空节点ID时校验应报错。
func TestValidateEmptyNodeID(t *testing.T) {
	cfg := testConfig()
	cfg.Server.NodeID = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty node_id")
	}
}

// TestValidateBadGRPCAddr 测试各种无效gRPC地址格式的校验。
func TestValidateBadGRPCAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
	}{
		{"empty", ""},
		{"no port", "127.0.0.1"},
		{"bad host", "not-an-ip:18000"},
		{"zero port", "127.0.0.1:0"},
		{"bad port", "127.0.0.1:abc"},
		{"port too high", "127.0.0.1:65536"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Server.GRPCAddr = tt.addr
			if err := cfg.validate(); err == nil {
				t.Errorf("grpc_addr %q: expected error", tt.addr)
			}
		})
	}
}

// TestValidateBadAPIAddr 测试无效API地址时校验应报错。
func TestValidateBadAPIAddr(t *testing.T) {
	cfg := testConfig()
	cfg.Server.APIAddr = "bad"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for bad api_addr")
	}
}

// TestValidateInvalidLogLevel 测试无效日志级别时校验应报错。
func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := testConfig()
	cfg.Server.LogLevel = "BOGUS"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid log_level")
	}
}

// TestValidateLogLevelCaseInsensitive 测试日志级别不区分大小写。
func TestValidateLogLevelCaseInsensitive(t *testing.T) {
	cfg := testConfig()
	for _, level := range []string{"debug", "info", "warn", "error", "Debug", "INFO"} {
		cfg.Server.LogLevel = level
		if err := cfg.validate(); err != nil {
			t.Errorf("log_level %q should be valid: %v", level, err)
		}
	}
}

// TestValidateNegativeTimeout 测试负数超时时间时校验应报错。
func TestValidateNegativeTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.API.Timeout.ReadTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative read_timeout")
	}
}

// TestValidateReadHeaderTimeoutExceedsReadTimeout 测试读取头部超时超过读取超时时校验应报错。
func TestValidateReadHeaderTimeoutExceedsReadTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.API.Timeout.ReadHeaderTimeout = 20e9
	cfg.API.Timeout.ReadTimeout = 10e9
	if err := cfg.validate(); err == nil {
		t.Error("expected error when read_header_timeout > read_timeout")
	}
}

// TestValidateListenAddrValid 测试有效监听地址时校验应通过。
func TestValidateListenAddrValid(t *testing.T) {
	cfg := testConfig()
	cfg.Server.GRPCAddr = "0.0.0.0:18000"
	cfg.Server.APIAddr = "127.0.0.1:18001"
	if err := cfg.validate(); err != nil {
		t.Errorf("valid addresses should pass: %v", err)
	}
}

// TestListenAddrColonPort 测试冒号端口格式的地址校验。
func TestListenAddrColonPort(t *testing.T) {
	cfg := testConfig()
	cfg.Server.GRPCAddr = ":18000"
	if err := cfg.validate(); err != nil {
		t.Errorf(":port format should be valid: %v", err)
	}
}

// TestValidateHTTPSEmptyCertFile 测试HTTPS启用时空证书文件路径时校验应报错。
func TestValidateHTTPSEmptyCertFile(t *testing.T) {
	cfg := testConfig()
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.CertFile = ""
	cfg.API.TLS.KeyFile = "./key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty https.cert_file")
	}
}

// TestValidateHTTPSNonexistentCertFile 测试HTTPS启用时不存在的证书文件路径校验应报错。
func TestValidateHTTPSNonexistentCertFile(t *testing.T) {
	cfg := testConfig()
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.CertFile = "/nonexistent/cert.pem"
	cfg.API.TLS.KeyFile = "/nonexistent/key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for nonexistent https.cert_file")
	}
}

// TestValidateHTTPSNonexistentKeyFile 测试HTTPS启用时不存在的密钥文件路径校验应报错。
func TestValidateHTTPSNonexistentKeyFile(t *testing.T) {
	cfg := testConfig()
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.CertFile = "./cert.pem"
	cfg.API.TLS.KeyFile = "/nonexistent/key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for nonexistent https.key_file")
	}
}

// TestValidateHTTPSEmptyKeyFile 测试HTTPS启用时空密钥文件路径时校验应报错。
func TestValidateHTTPSEmptyKeyFile(t *testing.T) {
	cfg := testConfig()
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.CertFile = "./cert.pem"
	cfg.API.TLS.KeyFile = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty https.key_file")
	}
}

// TestValidateHTTPSDisabledSkipsValidation 测试HTTPS未启用时跳过证书文件校验。
func TestValidateHTTPSDisabledSkipsValidation(t *testing.T) {
	cfg := testConfig()
	cfg.API.TLS.Enabled = false
	if err := cfg.validate(); err != nil {
		t.Errorf("disabled HTTPS should not validate: %v", err)
	}
}

// TestHasHTTPS 测试HasHTTPS方法在不同配置下的返回值。
func TestHasHTTPS(t *testing.T) {
	cfg := testConfig()
	if cfg.HasHTTPS() {
		t.Error("disabled HTTPS should return false")
	}
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.CertFile = "./cert.pem"
	cfg.API.TLS.KeyFile = "./key.pem"
	if !cfg.HasHTTPS() {
		t.Error("enabled HTTPS with files should return true")
	}
	cfg.API.TLS.CertFile = ""
	if cfg.HasHTTPS() {
		t.Error("empty cert_file should return false")
	}
}

// TestValidateNegativeConnectTimeout 测试负数连接超时时校验应报错。
func TestValidateNegativeConnectTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.XDS.ConnectTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative connect_timeout")
	}
}

// TestValidateNegativeUDPIdleTimeout 测试负数UDP空闲超时时校验应报错。
func TestValidateNegativeUDPIdleTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.XDS.UDPIdleTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative udp_idle_timeout")
	}
}

// TestValidateNegativeRateLimitRPS 测试负数限流QPS时校验应报错。
func TestValidateNegativeRateLimitRPS(t *testing.T) {
	cfg := testConfig()
	cfg.API.RateLimit.RPS = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative rate_limit.rps")
	}
}

// TestValidateNegativeRateLimitBurst 测试负数限流突发容量时校验应报错。
func TestValidateNegativeRateLimitBurst(t *testing.T) {
	cfg := testConfig()
	cfg.API.RateLimit.Burst = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative rate_limit.burst")
	}
}

// TestValidateRateLimitBurstLessThanRPS 测试限流突发容量小于QPS时校验应报错。
func TestValidateRateLimitBurstLessThanRPS(t *testing.T) {
	cfg := testConfig()
	cfg.API.RateLimit.RPS = 20
	cfg.API.RateLimit.Burst = 10
	if err := cfg.validate(); err == nil {
		t.Error("expected error when burst < rps")
	}
}
