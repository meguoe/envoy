package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// CertConfig 证书生成配置参数。
type CertConfig struct {
	Dir       string
	Type      string // "mtls" or "https"
	ClientURI string
	ValidDays int
	ServerDNS string
	ServerIPs []string
}

// randomSerialNumber 生成 128 位随机整数作为 X.509 证书序列号。
func randomSerialNumber() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("生成随机序列号失败: %w", err)
	}
	return serial, nil
}

// GenerateCerts 根据配置生成 CA、服务端和客户端证书，写入指定目录。
func GenerateCerts(cfg CertConfig) error {
	if cfg.Dir == "" {
		if cfg.Type == "https" {
			cfg.Dir = "certs/https"
		} else {
			cfg.Dir = "certs/mtls"
		}
	}
	if cfg.ValidDays <= 0 {
		cfg.ValidDays = 3650
	}
	if cfg.ServerDNS == "" {
		cfg.ServerDNS = "xds-server"
	}
	if len(cfg.ServerIPs) == 0 {
		cfg.ServerIPs = []string{"127.0.0.1"}
	}

	if certExists(cfg.Dir) {
		return &CertExistsError{Dir: cfg.Dir}
	}

	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return fmt.Errorf("创建证书目录失败: %w", err)
	}

	// CA
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("生成 CA 密钥失败: %w", err)
	}
	caSerial, err := randomSerialNumber()
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "Envoy Control Plane CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Duration(cfg.ValidDays) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("生成 CA 证书失败: %w", err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return fmt.Errorf("解析 CA 证书失败: %w", err)
	}
	if err := writeCert(filepath.Join(cfg.Dir, "ca.crt"), caCertDER); err != nil {
		return fmt.Errorf("写入 CA 证书失败: %w", err)
	}
	if err := writeKeyToFile(filepath.Join(cfg.Dir, "ca.key"), caKey); err != nil {
		return fmt.Errorf("写入 CA 私钥失败: %w", err)
	}

	// Server cert
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("生成服务端密钥失败: %w", err)
	}
	var serverIPs []net.IP
	for _, ip := range cfg.ServerIPs {
		if parsed := net.ParseIP(ip); parsed != nil {
			serverIPs = append(serverIPs, parsed)
		}
	}
	serverSerial, err := randomSerialNumber()
	if err != nil {
		return err
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject:      pkix.Name{CommonName: cfg.ServerDNS},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Duration(cfg.ValidDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  serverIPs,
		DNSNames:     []string{cfg.ServerDNS},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("生成服务端证书失败: %w", err)
	}
	if err := writeCert(filepath.Join(cfg.Dir, "server.crt"), serverCertDER); err != nil {
		return fmt.Errorf("写入服务端证书失败: %w", err)
	}
	if err := writeKeyToFile(filepath.Join(cfg.Dir, "server.key"), serverKey); err != nil {
		return fmt.Errorf("写入服务端私钥失败: %w", err)
	}

	if cfg.Type == "mtls" {
		// Client cert
		clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("生成客户端密钥失败: %w", err)
		}
		clientSerial, err := randomSerialNumber()
		if err != nil {
			return err
		}
		clientTemplate := &x509.Certificate{
			SerialNumber: clientSerial,
			Subject:      pkix.Name{CommonName: "envoy-client"},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(time.Duration(cfg.ValidDays) * 24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		if cfg.ClientURI != "" {
			u, err := url.Parse(cfg.ClientURI)
			if err != nil {
				return fmt.Errorf("解析 client_uri 失败: %w", err)
			}
			clientTemplate.URIs = []*url.URL{u}
		}
		clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
		if err != nil {
			return fmt.Errorf("生成客户端证书失败: %w", err)
		}
		if err := writeCert(filepath.Join(cfg.Dir, "client.crt"), clientCertDER); err != nil {
			return fmt.Errorf("写入客户端证书失败: %w", err)
		}
		if err := writeKeyToFile(filepath.Join(cfg.Dir, "client.key"), clientKey); err != nil {
			return fmt.Errorf("写入客户端私钥失败: %w", err)
		}
		fmt.Printf("证书已生成到 %s/\n", cfg.Dir)
		fmt.Printf("  ca.crt / ca.key         - CA\n")
		fmt.Printf("  server.crt / server.key - 服务端\n")
		fmt.Printf("  client.crt / client.key - 客户端\n")
	} else {
		fmt.Printf("证书已生成到 %s/\n", cfg.Dir)
		fmt.Printf("  ca.crt / ca.key         - CA\n")
		fmt.Printf("  server.crt / server.key - 服务端\n")
	}
	return nil
}

// writeCert 将 DER 编码的证书写入 PEM 格式文件。
func writeCert(path string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	}), 0644)
}

// writeKeyToFile 将 ECDSA 私钥序列化为 PEM 格式并写入文件，权限为 0600。
func writeKeyToFile(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("序列化私钥失败: %w", err)
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: der,
	}), 0600)
}

// certExists 检查证书目录下是否已存在 ca.crt、server.crt 或 server.key。
func certExists(dir string) bool {
	for _, name := range []string{"ca.crt", "server.crt", "server.key"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return true
		}
	}
	return false
}

// CertExistsError 表示证书目录已存在证书文件的错误。
type CertExistsError struct {
	Dir string
}

// Error 返回证书已存在的错误描述信息。
func (e *CertExistsError) Error() string {
	return fmt.Sprintf("证书目录 %s 已存在证书文件", e.Dir)
}
