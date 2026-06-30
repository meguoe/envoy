package config

// config.go —— 控制面配置
//
// 默认读取 ./config.yaml。

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	xdsserver "envoy-control-plane/internal/server/xds"
	"gopkg.in/yaml.v3"
)

const DefaultPath = "config.yaml"

const (
	defaultGRPCAddr     = "127.0.0.1:18001"
	defaultAPIAddr      = "127.0.0.1:18000"
	defaultMaxBodyBytes = 1 << 20
)

type Config struct {
	NodeID       string `yaml:"node_id"`
	GRPCAddr     string `yaml:"grpc_addr"`
	APIAddr      string `yaml:"api_addr"`
	LogLevel     string `yaml:"log_level"`
	MaxBodyBytes int64  `yaml:"max_body_bytes"`
	Auth         struct {
		APIKey         string   `yaml:"api_key"`
		AllowedIPs     []string `yaml:"allowed_ips"`
		TrustedProxies []string `yaml:"trusted_proxies"`
	} `yaml:"auth"`
	HttpTimeout struct {
		ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
		ReadTimeout       time.Duration `yaml:"read_timeout"`
		WriteTimeout      time.Duration `yaml:"write_timeout"`
		IdleTimeout       time.Duration `yaml:"idle_timeout"`
	} `yaml:"http_timeout"`
	TLS struct {
		ServerCert string `yaml:"server_cert"`
		ServerKey  string `yaml:"server_key"`
		CACert     string `yaml:"ca_cert"`
		ClientURI  string `yaml:"client_uri"`
	} `yaml:"tls"`
	HTTPS struct {
		Enabled  bool   `yaml:"enabled"`
		CertFile string `yaml:"cert_file"`
		KeyFile  string `yaml:"key_file"`
	} `yaml:"https"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	UDPIdleTimeout time.Duration `yaml:"udp_idle_timeout"`
	RateLimit      struct {
		RPS   float64 `yaml:"rps"`
		Burst float64 `yaml:"burst"`
	} `yaml:"rate_limit"`
	Database struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DBName   string `yaml:"dbname"`
	} `yaml:"database"`
}

func Load(configPath string) (Config, error) {
	if configPath == "" {
		configPath = DefaultPath
	}
	return loadFromFile(configPath)
}

func loadFromFile(configPath string) (Config, error) {
	cfg := defaultConfig()
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, fmt.Errorf("读取配置文件失败: %w", err)
	} else if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析配置文件失败: %w", err)
	}

	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}

	if err := cfg.validate(); err != nil {
		return cfg, fmt.Errorf("配置校验失败: %w", err)
	}
	return cfg, nil
}

var validLogLevels = map[string]bool{
	"DEBUG": true, "INFO": true, "WARN": true, "ERROR": true,
}

func (c Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id 不能为空")
	}
	if err := validateListenAddr(c.GRPCAddr, "grpc_addr"); err != nil {
		return err
	}
	if err := validateListenAddr(c.APIAddr, "api_addr"); err != nil {
		return err
	}
	if c.Database.Host == "" {
		return fmt.Errorf("database.host 不能为空")
	}
	if c.Database.Port == "" {
		return fmt.Errorf("database.port 不能为空")
	}
	if portNum, err := strconv.Atoi(c.Database.Port); err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("database.port 无效 %q: 必须是 1-65535", c.Database.Port)
	}
	if c.Database.User == "" {
		return fmt.Errorf("database.user 不能为空")
	}
	if c.Database.DBName == "" {
		return fmt.Errorf("database.dbname 不能为空")
	}
	level := strings.ToUpper(strings.TrimSpace(c.LogLevel))
	if !validLogLevels[level] {
		return fmt.Errorf("log_level 无效，可选值: DEBUG, INFO, WARN, ERROR")
	}
	for _, addr := range c.Auth.AllowedIPs {
		if err := validateIPorCIDR(addr, "auth.allowed_ips"); err != nil {
			return err
		}
	}
	for _, addr := range c.Auth.TrustedProxies {
		if err := validateIPorCIDR(addr, "auth.trusted_proxies"); err != nil {
			return err
		}
	}
	if c.HttpTimeout.ReadHeaderTimeout < 0 {
		return fmt.Errorf("http_timeout.read_header_timeout 不能为负数")
	}
	if c.HttpTimeout.ReadTimeout > 0 && c.HttpTimeout.ReadHeaderTimeout > c.HttpTimeout.ReadTimeout {
		return fmt.Errorf("http_timeout.read_header_timeout 不能大于 read_timeout")
	}
	if c.HttpTimeout.ReadTimeout < 0 {
		return fmt.Errorf("http_timeout.read_timeout 不能为负数")
	}
	if c.HttpTimeout.WriteTimeout < 0 {
		return fmt.Errorf("http_timeout.write_timeout 不能为负数")
	}
	if c.HttpTimeout.IdleTimeout < 0 {
		return fmt.Errorf("http_timeout.idle_timeout 不能为负数")
	}
	if c.HTTPS.Enabled {
		if c.HTTPS.CertFile == "" {
			return fmt.Errorf("https.cert_file 不能为空")
		}
		if c.HTTPS.KeyFile == "" {
			return fmt.Errorf("https.key_file 不能为空")
		}
		if err := validateFile(c.HTTPS.CertFile, "https.cert_file"); err != nil {
			return err
		}
		if err := validateFile(c.HTTPS.KeyFile, "https.key_file"); err != nil {
			return err
		}
	}
	if c.ConnectTimeout < 0 {
		return fmt.Errorf("connect_timeout 不能为负数")
	}
	if c.UDPIdleTimeout < 0 {
		return fmt.Errorf("udp_idle_timeout 不能为负数")
	}
	if c.RateLimit.RPS < 0 {
		return fmt.Errorf("rate_limit.rps 不能为负数")
	}
	if c.RateLimit.Burst < 0 {
		return fmt.Errorf("rate_limit.burst 不能为负数")
	}
	if c.RateLimit.RPS > 0 && c.RateLimit.Burst < c.RateLimit.RPS {
		return fmt.Errorf("rate_limit.burst 不能小于 rate_limit.rps")
	}
	return nil
}

func validateListenAddr(addr, field string) error {
	if addr == "" {
		return fmt.Errorf("%s 不能为空", field)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s 格式无效 %q: %w", field, addr, err)
	}
	if host != "" && net.ParseIP(host) == nil {
		return fmt.Errorf("%s 主机地址无效 %q", field, host)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("%s 端口无效 %q: 必须是 1-65535", field, port)
	}
	return nil
}

func validateFile(path, field string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%s 文件不可读 %q: %w", field, path, err)
	}
	f.Close()
	return nil
}

func validateIPorCIDR(addr, field string) error {
	addr = strings.TrimSpace(addr)
	if strings.Contains(addr, "/") {
		if _, _, err := net.ParseCIDR(addr); err != nil {
			return fmt.Errorf("%s CIDR 格式无效 %q: %w", field, addr, err)
		}
	} else {
		if net.ParseIP(addr) == nil {
			return fmt.Errorf("%s IP 格式无效 %q", field, addr)
		}
	}
	return nil
}

func defaultConfig() Config {
	cfg := Config{
		NodeID:       "envoy-local",
		GRPCAddr:     defaultGRPCAddr,
		APIAddr:      defaultAPIAddr,
		LogLevel:     "INFO",
		MaxBodyBytes: defaultMaxBodyBytes,
	}
	cfg.Database.Host = "localhost"
	cfg.Database.Port = "7032"
	cfg.Database.User = "hiddos"
	cfg.Database.DBName = "hiddos-ecp"
	cfg.Auth.AllowedIPs = []string{"127.0.0.1"}
	cfg.HttpTimeout.ReadHeaderTimeout = 5 * time.Second
	cfg.HttpTimeout.ReadTimeout = 10 * time.Second
	cfg.HttpTimeout.WriteTimeout = 10 * time.Second
	cfg.HttpTimeout.IdleTimeout = 60 * time.Second
	cfg.TLS.ServerCert = "./certs/mtls/server.crt"
	cfg.TLS.ServerKey = "./certs/mtls/server.key"
	cfg.TLS.CACert = "./certs/mtls/ca.crt"
	cfg.TLS.ClientURI = "spiffe://local/envoy/envoy-local"
	cfg.ConnectTimeout = 1 * time.Second
	cfg.UDPIdleTimeout = 60 * time.Second
	cfg.RateLimit.RPS = 20
	cfg.RateLimit.Burst = 40
	return cfg
}

func (c Config) HasAuth() bool {
	return c.Auth.APIKey != "" || len(c.Auth.AllowedIPs) > 0
}

func (c Config) HasHTTPS() bool {
	return c.HTTPS.Enabled && c.HTTPS.CertFile != "" && c.HTTPS.KeyFile != ""
}

func (c Config) TLSConfig() *xdsserver.TLSConfig {
	return &xdsserver.TLSConfig{
		ServerCert: c.TLS.ServerCert,
		ServerKey:  c.TLS.ServerKey,
		CACert:     c.TLS.CACert,
		ClientURI:  c.TLS.ClientURI,
	}
}
