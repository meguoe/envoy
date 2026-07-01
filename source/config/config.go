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

	xdsserver "envoy-control-plane/source/server/xds"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

const DefaultPath = "config.yaml"

const (
	defaultGRPCAddr     = "127.0.0.1:18001"
	defaultAPIAddr      = "127.0.0.1:18000"
	defaultMaxBodyBytes = 5 << 20
)

type Config struct {
	Server struct {
		NodeID   string `yaml:"node_id"`
		GRPCAddr string `yaml:"grpc_addr"`
		APIAddr  string `yaml:"api_addr"`
		LogLevel string `yaml:"log_level"`
	} `yaml:"server"`
	API struct {
		MaxBodyBytes int64 `yaml:"max_body_bytes"`
		Auth         struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"auth"`
		RateLimit struct {
			RPS   float64 `yaml:"rps"`
			Burst float64 `yaml:"burst"`
		} `yaml:"rate_limit"`
		Timeout struct {
			ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
			ReadTimeout       time.Duration `yaml:"read_timeout"`
			WriteTimeout      time.Duration `yaml:"write_timeout"`
			IdleTimeout       time.Duration `yaml:"idle_timeout"`
		} `yaml:"timeout"`
		TLS struct {
			Enabled  bool   `yaml:"enabled"`
			CertFile string `yaml:"cert_file"`
			KeyFile  string `yaml:"key_file"`
		} `yaml:"tls"`
	} `yaml:"api"`
	XDS struct {
		ConnectTimeout time.Duration `yaml:"connect_timeout"`
		UDPIdleTimeout time.Duration `yaml:"udp_idle_timeout"`
		TLS            struct {
			Enabled    bool   `yaml:"enabled"`
			ServerCert string `yaml:"server_cert"`
			ServerKey  string `yaml:"server_key"`
			CACert     string `yaml:"ca_cert"`
			ClientURI  string `yaml:"client_uri"`
		} `yaml:"tls"`
	} `yaml:"xds"`
}

// Load 加载指定路径的配置文件，若路径为空则使用默认路径 config.yaml。
func Load(configPath string) (Config, error) {
	if configPath == "" {
		configPath = DefaultPath
	}
	return loadFromFile(configPath)
}

// loadFromFile 从文件读取 YAML 配置并合并默认值，同时加载 .env 环境变量。
func loadFromFile(configPath string) (Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("加载 .env 文件失败: %w", err)
	}

	cfg := defaultConfig()
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, fmt.Errorf("读取配置文件失败: %w", err)
	} else if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析配置文件失败: %w", err)
	}

	if cfg.API.MaxBodyBytes <= 0 {
		cfg.API.MaxBodyBytes = defaultMaxBodyBytes
	}

	if err := cfg.validate(); err != nil {
		return cfg, fmt.Errorf("配置校验失败: %w", err)
	}
	return cfg, nil
}

var validLogLevels = map[string]bool{
	"DEBUG": true, "INFO": true, "WARN": true, "ERROR": true,
}

// validate 校验配置项的合法性，包括地址格式、超时参数、TLS 证书路径等。
func (c Config) validate() error {
	if c.Server.NodeID == "" {
		return fmt.Errorf("server.node_id 不能为空")
	}
	if err := validateListenAddr(c.Server.GRPCAddr, "server.grpc_addr"); err != nil {
		return err
	}
	if err := validateListenAddr(c.Server.APIAddr, "server.api_addr"); err != nil {
		return err
	}
	level := strings.ToUpper(strings.TrimSpace(c.Server.LogLevel))
	if !validLogLevels[level] {
		return fmt.Errorf("server.log_level 无效，可选值: DEBUG, INFO, WARN, ERROR")
	}
	if c.API.Timeout.ReadHeaderTimeout < 0 {
		return fmt.Errorf("api.timeout.read_header_timeout 不能为负数")
	}
	if c.API.Timeout.ReadTimeout > 0 && c.API.Timeout.ReadHeaderTimeout > c.API.Timeout.ReadTimeout {
		return fmt.Errorf("api.timeout.read_header_timeout 不能大于 read_timeout")
	}
	if c.API.Timeout.ReadTimeout < 0 {
		return fmt.Errorf("api.timeout.read_timeout 不能为负数")
	}
	if c.API.Timeout.WriteTimeout < 0 {
		return fmt.Errorf("api.timeout.write_timeout 不能为负数")
	}
	if c.API.Timeout.IdleTimeout < 0 {
		return fmt.Errorf("api.timeout.idle_timeout 不能为负数")
	}
	if c.API.TLS.Enabled {
		if c.API.TLS.CertFile == "" {
			return fmt.Errorf("api.tls.cert_file 不能为空")
		}
		if c.API.TLS.KeyFile == "" {
			return fmt.Errorf("api.tls.key_file 不能为空")
		}
		if err := validateFile(c.API.TLS.CertFile, "api.tls.cert_file"); err != nil {
			return err
		}
		if err := validateFile(c.API.TLS.KeyFile, "api.tls.key_file"); err != nil {
			return err
		}
	}
	if c.XDS.TLS.Enabled {
		if c.XDS.TLS.ServerCert == "" {
			return fmt.Errorf("xds.tls.server_cert 不能为空")
		}
		if c.XDS.TLS.ServerKey == "" {
			return fmt.Errorf("xds.tls.server_key 不能为空")
		}
		if err := validateFile(c.XDS.TLS.ServerCert, "xds.tls.server_cert"); err != nil {
			return err
		}
		if err := validateFile(c.XDS.TLS.ServerKey, "xds.tls.server_key"); err != nil {
			return err
		}
		if c.XDS.TLS.CACert != "" {
			if err := validateFile(c.XDS.TLS.CACert, "xds.tls.ca_cert"); err != nil {
				return err
			}
		}
	}
	if c.XDS.ConnectTimeout < 0 {
		return fmt.Errorf("xds.connect_timeout 不能为负数")
	}
	if c.XDS.UDPIdleTimeout < 0 {
		return fmt.Errorf("xds.udp_idle_timeout 不能为负数")
	}
	if c.API.RateLimit.RPS < 0 {
		return fmt.Errorf("api.rate_limit.rps 不能为负数")
	}
	if c.API.RateLimit.Burst < 0 {
		return fmt.Errorf("api.rate_limit.burst 不能为负数")
	}
	if c.API.RateLimit.RPS > 0 && c.API.RateLimit.Burst < c.API.RateLimit.RPS {
		return fmt.Errorf("api.rate_limit.burst 不能小于 api.rate_limit.rps")
	}
	return nil
}

// validateListenAddr 校验监听地址格式是否合法，需为有效的 host:port 形式。
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

// validateFile 检查指定路径的文件是否存在且可读。
func validateFile(path, field string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%s 文件不可读 %q: %w", field, path, err)
	}
	f.Close()
	return nil
}

// defaultConfig 返回包含所有默认值的配置实例。
func defaultConfig() Config {
	cfg := Config{}
	cfg.Server.NodeID = "envoy-local"
	cfg.Server.APIAddr = defaultAPIAddr
	cfg.Server.GRPCAddr = defaultGRPCAddr
	cfg.Server.LogLevel = "INFO"
	cfg.XDS.ConnectTimeout = 1 * time.Second
	cfg.XDS.UDPIdleTimeout = 60 * time.Second
	cfg.XDS.TLS.Enabled = true
	cfg.XDS.TLS.CACert = "certs/mtls/ca.crt"
	cfg.XDS.TLS.ServerKey = "certs/mtls/server.key"
	cfg.XDS.TLS.ServerCert = "certs/mtls/server.crt"
	cfg.XDS.TLS.ClientURI = "spiffe://local/envoy/envoy-local"
	cfg.API.MaxBodyBytes = defaultMaxBodyBytes
	cfg.API.Auth.Enabled = true
	cfg.API.RateLimit.RPS = 20
	cfg.API.RateLimit.Burst = 40
	cfg.API.Timeout.ReadHeaderTimeout = 5 * time.Second
	cfg.API.Timeout.ReadTimeout = 10 * time.Second
	cfg.API.Timeout.WriteTimeout = 10 * time.Second
	cfg.API.Timeout.IdleTimeout = 60 * time.Second
	cfg.API.TLS.Enabled = true
	cfg.API.TLS.KeyFile = "certs/https/server.key"
	cfg.API.TLS.CertFile = "certs/https/server.crt"
	return cfg
}

// HasAuth 返回是否启用了 API 认证且 API_KEY 环境变量已设置。
func (c Config) HasAuth() bool {
	return c.API.Auth.Enabled && os.Getenv("API_KEY") != ""
}

// HasHTTPS 返回是否启用了 HTTPS 且证书和私钥路径均已配置。
func (c Config) HasHTTPS() bool {
	return c.API.TLS.Enabled && c.API.TLS.CertFile != "" && c.API.TLS.KeyFile != ""
}

// GenerateDefaultConfig 将默认配置写入指定路径的 YAML 文件。
func GenerateDefaultConfig(configPath string) error {
	if configPath == "" {
		configPath = DefaultPath
	}
	content := `server:
    node_id: envoy-local
    api_addr: 127.0.0.1:18000
    grpc_addr: 127.0.0.1:18001
    log_level: INFO
xds:
    connect_timeout: 1s
    udp_idle_timeout: 1m0s
    tls:
        enabled: true
        ca_cert: "certs/mtls/ca.crt"
        server_key: "certs/mtls/server.key"
        server_cert: "certs/mtls/server.crt"
        client_uri: "spiffe://local/envoy/envoy-local"
api:
    max_body_bytes: 5242880
    auth:
        enabled: true
    tls:
        enabled: true
        key_file: "certs/https/server.key"
        cert_file: "certs/https/server.crt"
    rate_limit:
        rps: 20
        burst: 40
    timeout:
        read_timeout: 10s
        write_timeout: 10s
        idle_timeout: 1m0s
        read_header_timeout: 5s
`
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}
	return nil
}

// GenerateDefaultConfigWithCerts 生成默认配置文件并创建 mTLS 和 HTTPS 证书。
func GenerateDefaultConfigWithCerts(configPath string) error {
	if err := GenerateDefaultConfig(configPath); err != nil {
		return err
	}
	cfg := defaultConfig()
	certDir := "certs/mtls"
	if !certExists(certDir) {
		if err := GenerateCerts(CertConfig{
			Dir:       certDir,
			Type:      "mtls",
			ClientURI: cfg.XDS.TLS.ClientURI,
			ServerDNS: "xds-server",
		}); err != nil {
			return fmt.Errorf("生成 mTLS 证书失败: %w", err)
		}
	}
	httpsDir := "certs/https"
	if !certExists(httpsDir) {
		if err := GenerateCerts(CertConfig{
			Dir:       httpsDir,
			Type:      "https",
			ServerDNS: "xds-server",
		}); err != nil {
			return fmt.Errorf("生成 HTTPS 证书失败: %w", err)
		}
	}
	return nil
}

// ValidateConfig 加载并校验指定路径的配置文件，返回可能的错误。
func ValidateConfig(configPath string) error {
	if configPath == "" {
		configPath = DefaultPath
	}
	_, err := Load(configPath)
	return err
}

// TLSConfig 将配置中的 TLS 参数转换为 xDS 引擎所需的 TLSConfig 结构体。
func (c Config) TLSConfig() *xdsserver.TLSConfig {
	if !c.XDS.TLS.Enabled || c.XDS.TLS.ServerCert == "" || c.XDS.TLS.ServerKey == "" {
		return nil
	}
	return &xdsserver.TLSConfig{
		ServerCert: c.XDS.TLS.ServerCert,
		ServerKey:  c.XDS.TLS.ServerKey,
		CACert:     c.XDS.TLS.CACert,
		ClientURI:  c.XDS.TLS.ClientURI,
	}
}
