package xdsserver

// tls.go —— mTLS 配置
//
// 提供 gRPC 服务器 mTLS 设置。证书缺失时启动失败，不降级明文。

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	grpcCreds "google.golang.org/grpc/credentials"
)

// TLSConfig mTLS 配置参数
type TLSConfig struct {
	ServerCert string // 服务器证书路径
	ServerKey  string // 服务器私钥路径
	CACert     string // CA 证书路径
	ClientURI  string // 允许的客户端 URI SAN
}

// ServerCredentials 创建 gRPC 服务器 TLS 凭证
func (c *TLSConfig) ServerCredentials() (grpcCreds.TransportCredentials, error) {
	tlsConfig, err := c.serverTLSConfig()
	if err != nil {
		return nil, err
	}
	return grpcCreds.NewTLS(tlsConfig), nil
}

func (c *TLSConfig) serverTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.ServerCert, c.ServerKey)
	if err != nil {
		return nil, fmt.Errorf("加载服务器证书失败: %w", err)
	}

	caCert, err := os.ReadFile(c.CACert)
	if err != nil {
		return nil, fmt.Errorf("读取 CA 证书失败: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("解析 CA 证书失败")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("客户端未提供证书")
			}
			for _, uri := range cs.PeerCertificates[0].URIs {
				if uri.String() == c.ClientURI {
					return nil
				}
			}
			return fmt.Errorf("客户端证书 URI SAN 不匹配: want %s", c.ClientURI)
		},
	}

	return tlsConfig, nil
}
