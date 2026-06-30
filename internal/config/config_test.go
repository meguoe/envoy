package config

import (
	"os"
	"testing"
)

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
	if err := os.WriteFile("config.yaml", []byte(`node_id: test-node`), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load("config.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.NodeID != "test-node" {
		t.Errorf("node_id = %q, want test-node", cfg.NodeID)
	}
	if cfg.GRPCAddr != "127.0.0.1:18001" {
		t.Errorf("default grpc_addr = %q, want 127.0.0.1:18001", cfg.GRPCAddr)
	}
	if cfg.APIAddr != "127.0.0.1:18000" {
		t.Errorf("default api_addr = %q, want 127.0.0.1:18000", cfg.APIAddr)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("default database.host = %q, want localhost", cfg.Database.Host)
	}
	if cfg.Database.Port != "7032" {
		t.Errorf("default database.port = %q, want 7032", cfg.Database.Port)
	}
	if cfg.Database.User != "hiddos" {
		t.Errorf("default database.user = %q, want hiddos", cfg.Database.User)
	}
	if cfg.Database.DBName != "hiddos-ecp" {
		t.Errorf("default database.dbname = %q, want hiddos-ecp", cfg.Database.DBName)
	}
	if cfg.LogLevel != "INFO" {
		t.Errorf("default log_level = %q, want INFO", cfg.LogLevel)
	}
	if cfg.MaxBodyBytes != 1<<20 {
		t.Errorf("default max_body_bytes = %d, want %d", cfg.MaxBodyBytes, 1<<20)
	}
	if cfg.HttpTimeout.ReadHeaderTimeout != 5e9 {
		t.Errorf("default read_header_timeout = %v, want 5s", cfg.HttpTimeout.ReadHeaderTimeout)
	}
	if cfg.HttpTimeout.ReadTimeout != 10e9 {
		t.Errorf("default read_timeout = %v, want 10s", cfg.HttpTimeout.ReadTimeout)
	}
	if cfg.HttpTimeout.WriteTimeout != 10e9 {
		t.Errorf("default write_timeout = %v, want 10s", cfg.HttpTimeout.WriteTimeout)
	}
	if cfg.HttpTimeout.IdleTimeout != 60e9 {
		t.Errorf("default idle_timeout = %v, want 60s", cfg.HttpTimeout.IdleTimeout)
	}
	if len(cfg.Auth.AllowedIPs) != 1 || cfg.Auth.AllowedIPs[0] != "127.0.0.1" {
		t.Errorf("default allowed_ips = %v, want [127.0.0.1]", cfg.Auth.AllowedIPs)
	}
	if len(cfg.Auth.TrustedProxies) != 0 {
		t.Errorf("default trusted_proxies = %v, want []", cfg.Auth.TrustedProxies)
	}
}

func TestValidateEmptyNodeID(t *testing.T) {
	cfg := defaultConfig()
	cfg.NodeID = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty node_id")
	}
}

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
			cfg := defaultConfig()
			cfg.GRPCAddr = tt.addr
			if err := cfg.validate(); err == nil {
				t.Errorf("grpc_addr %q: expected error", tt.addr)
			}
		})
	}
}

func TestValidateBadAPIAddr(t *testing.T) {
	cfg := defaultConfig()
	cfg.APIAddr = "bad"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for bad api_addr")
	}
}

func TestValidateEmptyDatabaseHost(t *testing.T) {
	cfg := defaultConfig()
	cfg.Database.Host = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty database.host")
	}
}

func TestValidateEmptyDatabasePort(t *testing.T) {
	cfg := defaultConfig()
	cfg.Database.Port = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty database.port")
	}
}

func TestValidateInvalidDatabasePort(t *testing.T) {
	cfg := defaultConfig()
	cfg.Database.Port = "abc"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid database.port")
	}
}

func TestValidateEmptyDatabaseUser(t *testing.T) {
	cfg := defaultConfig()
	cfg.Database.User = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty database.user")
	}
}

func TestValidateEmptyDatabaseDBName(t *testing.T) {
	cfg := defaultConfig()
	cfg.Database.DBName = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty database.dbname")
	}
}

func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := defaultConfig()
	cfg.LogLevel = "BOGUS"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid log_level")
	}
}

func TestValidateLogLevelCaseInsensitive(t *testing.T) {
	cfg := defaultConfig()
	for _, level := range []string{"debug", "info", "warn", "error", "Debug", "INFO"} {
		cfg.LogLevel = level
		if err := cfg.validate(); err != nil {
			t.Errorf("log_level %q should be valid: %v", level, err)
		}
	}
}

func TestValidateNegativeTimeout(t *testing.T) {
	cfg := defaultConfig()
	cfg.HttpTimeout.ReadTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative read_timeout")
	}
}

func TestValidateReadHeaderTimeoutExceedsReadTimeout(t *testing.T) {
	cfg := defaultConfig()
	cfg.HttpTimeout.ReadHeaderTimeout = 20e9
	cfg.HttpTimeout.ReadTimeout = 10e9
	if err := cfg.validate(); err == nil {
		t.Error("expected error when read_header_timeout > read_timeout")
	}
}

func TestValidateListenAddrValid(t *testing.T) {
	cfg := defaultConfig()
	cfg.GRPCAddr = "0.0.0.0:18000"
	cfg.APIAddr = "127.0.0.1:18001"
	if err := cfg.validate(); err != nil {
		t.Errorf("valid addresses should pass: %v", err)
	}
}

func TestHasAuth(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.AllowedIPs = nil
	if cfg.HasAuth() {
		t.Error("empty api_key + empty allowed_ips should not have auth")
	}
	cfg.Auth.APIKey = "secret"
	if !cfg.HasAuth() {
		t.Error("api_key set should make HasAuth true")
	}
}

func TestListenAddrColonPort(t *testing.T) {
	cfg := defaultConfig()
	cfg.GRPCAddr = ":18000"
	if err := cfg.validate(); err != nil {
		t.Errorf(":port format should be valid: %v", err)
	}
}

func TestValidateHTTPSEmptyCertFile(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTPS.Enabled = true
	cfg.HTTPS.CertFile = ""
	cfg.HTTPS.KeyFile = "./key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty https.cert_file")
	}
}

func TestValidateHTTPSNonexistentCertFile(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTPS.Enabled = true
	cfg.HTTPS.CertFile = "/nonexistent/cert.pem"
	cfg.HTTPS.KeyFile = "/nonexistent/key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for nonexistent https.cert_file")
	}
}

func TestValidateHTTPSNonexistentKeyFile(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTPS.Enabled = true
	cfg.HTTPS.CertFile = "./cert.pem"
	cfg.HTTPS.KeyFile = "/nonexistent/key.pem"
	if err := cfg.validate(); err == nil {
		t.Error("expected error for nonexistent https.key_file")
	}
}

func TestValidateHTTPSEmptyKeyFile(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTPS.Enabled = true
	cfg.HTTPS.CertFile = "./cert.pem"
	cfg.HTTPS.KeyFile = ""
	if err := cfg.validate(); err == nil {
		t.Error("expected error for empty https.key_file")
	}
}

func TestValidateHTTPSDisabledSkipsValidation(t *testing.T) {
	cfg := defaultConfig()
	cfg.HTTPS.Enabled = false
	if err := cfg.validate(); err != nil {
		t.Errorf("disabled HTTPS should not validate: %v", err)
	}
}

func TestHasHTTPS(t *testing.T) {
	cfg := defaultConfig()
	if cfg.HasHTTPS() {
		t.Error("disabled HTTPS should return false")
	}
	cfg.HTTPS.Enabled = true
	cfg.HTTPS.CertFile = "./cert.pem"
	cfg.HTTPS.KeyFile = "./key.pem"
	if !cfg.HasHTTPS() {
		t.Error("enabled HTTPS with files should return true")
	}
	cfg.HTTPS.CertFile = ""
	if cfg.HasHTTPS() {
		t.Error("empty cert_file should return false")
	}
}

func TestValidateBadAllowedIP(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.AllowedIPs = []string{"not-an-ip"}
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid allowed_ips")
	}
}

func TestValidateAllowedCIDR(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.AllowedIPs = []string{"10.0.0.0/8"}
	if err := cfg.validate(); err != nil {
		t.Errorf("valid CIDR should pass: %v", err)
	}
}

func TestValidateBadCIDR(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.AllowedIPs = []string{"10.0.0.0/33"}
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid CIDR")
	}
}

func TestValidateBadTrustedProxy(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.TrustedProxies = []string{"bad"}
	if err := cfg.validate(); err == nil {
		t.Error("expected error for invalid trusted_proxies")
	}
}

func TestValidateNegativeConnectTimeout(t *testing.T) {
	cfg := defaultConfig()
	cfg.ConnectTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative connect_timeout")
	}
}

func TestValidateNegativeUDPIdleTimeout(t *testing.T) {
	cfg := defaultConfig()
	cfg.UDPIdleTimeout = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative udp_idle_timeout")
	}
}

func TestValidateNegativeRateLimitRPS(t *testing.T) {
	cfg := defaultConfig()
	cfg.RateLimit.RPS = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative rate_limit.rps")
	}
}

func TestValidateNegativeRateLimitBurst(t *testing.T) {
	cfg := defaultConfig()
	cfg.RateLimit.Burst = -1
	if err := cfg.validate(); err == nil {
		t.Error("expected error for negative rate_limit.burst")
	}
}

func TestValidateRateLimitBurstLessThanRPS(t *testing.T) {
	cfg := defaultConfig()
	cfg.RateLimit.RPS = 20
	cfg.RateLimit.Burst = 10
	if err := cfg.validate(); err == nil {
		t.Error("expected error when burst < rps")
	}
}

func TestValidateIPorCIDRTrimSpace(t *testing.T) {
	cfg := defaultConfig()
	cfg.Auth.AllowedIPs = []string{" 10.0.0.0/8 ", " 127.0.0.1 "}
	if err := cfg.validate(); err != nil {
		t.Errorf("trimmed IP/CIDR should pass: %v", err)
	}
}
