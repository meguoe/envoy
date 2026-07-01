package xdsserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCert 创建自签名的 CA、服务端和客户端证书用于测试。
func generateTestCert(t *testing.T, dir string, clientURI string) (caPath, serverCertPath, serverKeyPath string) {
	t.Helper()

	// CA
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, _ := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	caPath = filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}), 0644); err != nil {
		t.Fatalf("写入 CA 证书失败: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caCertDER)

	// Server cert
	serverKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "xds-server"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"xds-server"},
	}
	serverCertDER, _ := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	serverCertPath = filepath.Join(dir, "server.crt")
	serverKeyPath = filepath.Join(dir, "server.key")
	if err := os.WriteFile(serverCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}), 0644); err != nil {
		t.Fatalf("写入服务端证书失败: %v", err)
	}
	serverKeyDER, _ := x509.MarshalECPrivateKey(serverKey)
	if err := os.WriteFile(serverKeyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}), 0644); err != nil {
		t.Fatalf("写入服务端私钥失败: %v", err)
	}

	// Client cert with URI SAN
	clientKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "envoy-client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if clientURI != "" {
		u, err := url.Parse(clientURI)
		if err != nil {
			t.Fatalf("invalid clientURI %q: %v", clientURI, err)
		}
		clientTemplate.URIs = []*url.URL{u}
	}
	clientCertDER, _ := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	clientCertPath := filepath.Join(dir, "client.crt")
	clientKeyPath := filepath.Join(dir, "client.key")
	if err := os.WriteFile(clientCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}), 0644); err != nil {
		t.Fatalf("写入客户端证书失败: %v", err)
	}
	clientKeyDER, _ := x509.MarshalECPrivateKey(clientKey)
	if err := os.WriteFile(clientKeyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER}), 0644); err != nil {
		t.Fatalf("写入客户端私钥失败: %v", err)
	}

	return caPath, serverCertPath, serverKeyPath
}

// TestTLSConfigServerCredentials 测试有效证书配置能正确生成 gRPC 服务端凭证。
func TestTLSConfigServerCredentials(t *testing.T) {
	dir := t.TempDir()
	caPath, serverCertPath, serverKeyPath := generateTestCert(t, dir, "")

	cfg := &TLSConfig{
		ServerCert: serverCertPath,
		ServerKey:  serverKeyPath,
		CACert:     caPath,
		ClientURI:  "spiffe://local/envoy/envoy-local",
	}

	creds, err := cfg.ServerCredentials()
	if err != nil {
		t.Fatalf("ServerCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("ServerCredentials returned nil")
	}
}

// TestTLSConfigBadCertPath 测试无效证书路径时返回错误。
func TestTLSConfigBadCertPath(t *testing.T) {
	cfg := &TLSConfig{
		ServerCert: "/nonexistent/server.crt",
		ServerKey:  "/nonexistent/server.key",
		CACert:     "/nonexistent/ca.crt",
		ClientURI:  "spiffe://local/envoy/envoy-local",
	}

	_, err := cfg.ServerCredentials()
	if err == nil {
		t.Error("ServerCredentials with bad paths: expected error")
	}
}

// TestTLSConfigBadCA 测试无效 CA 证书时返回错误。
func TestTLSConfigBadCA(t *testing.T) {
	dir := t.TempDir()
	_, serverCertPath, serverKeyPath := generateTestCert(t, dir, "")

	// Write invalid CA cert
	badCAPath := filepath.Join(dir, "bad-ca.crt")
	if err := os.WriteFile(badCAPath, []byte("not a cert"), 0644); err != nil {
		t.Fatalf("写入无效 CA 证书失败: %v", err)
	}

	cfg := &TLSConfig{
		ServerCert: serverCertPath,
		ServerKey:  serverKeyPath,
		CACert:     badCAPath,
		ClientURI:  "spiffe://local/envoy/envoy-local",
	}

	_, err := cfg.ServerCredentials()
	if err == nil {
		t.Error("ServerCredentials with bad CA: expected error")
	}
}

// TestTLSVerifyConnectionMatch 测试 SPIFFE URI 匹配时 TLS 握手成功。
func TestTLSVerifyConnectionMatch(t *testing.T) {
	dir := t.TempDir()
	caPath, serverCertPath, serverKeyPath := generateTestCert(t, dir, "spiffe://local/envoy/envoy-local")

	serverTLS := newTestServerTLS(t, caPath, serverCertPath, serverKeyPath)
	clientTLS := newTestClientTLS(t, dir, caPath)

	clientErr, serverErr := handshakePair(serverTLS, clientTLS)
	if clientErr != nil {
		t.Fatalf("client handshake with matching URI: %v", clientErr)
	}
	if serverErr != nil {
		t.Fatalf("server handshake with matching URI: %v", serverErr)
	}
}

// TestTLSVerifyConnectionMismatch 测试 SPIFFE URI 不匹配时 TLS 握手失败。
func TestTLSVerifyConnectionMismatch(t *testing.T) {
	dir := t.TempDir()
	// Client cert has a DIFFERENT SPIFFE URI
	caPath, serverCertPath, serverKeyPath := generateTestCert(t, dir, "spiffe://local/envoy/wrong-client")

	serverTLS := newTestServerTLS(t, caPath, serverCertPath, serverKeyPath)
	clientTLS := newTestClientTLS(t, dir, caPath)
	clientErr, serverErr := handshakePair(serverTLS, clientTLS)
	if clientErr == nil && serverErr == nil {
		t.Fatal("handshake with wrong URI should have failed")
	}
}

// newTestServerTLS 创建用于测试的 gRPC 服务器 TLS 配置。
func newTestServerTLS(t *testing.T, caPath, serverCertPath, serverKeyPath string) *tls.Config {
	t.Helper()
	cfg := &TLSConfig{
		ServerCert: serverCertPath,
		ServerKey:  serverKeyPath,
		CACert:     caPath,
		ClientURI:  "spiffe://local/envoy/envoy-local",
	}
	tlsConfig, err := cfg.serverTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	return tlsConfig
}

// newTestClientTLS 创建用于测试的 gRPC 客户端 TLS 配置。
func newTestClientTLS(t *testing.T, dir, caPath string) *tls.Config {
	t.Helper()
	caCertPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCertPEM) {
		t.Fatal("failed to parse CA cert")
	}
	clientCertPEM, _ := os.ReadFile(filepath.Join(dir, "client.crt"))
	clientKeyPEM, _ := os.ReadFile(filepath.Join(dir, "client.key"))
	clientCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		ServerName:   "xds-server",
	}
}

// handshakePair 在管道上执行 TLS 握手并返回客户端和服务端的错误。
func handshakePair(serverTLS, clientTLS *tls.Config) (clientErr, serverErr error) {
	c1, c2 := net.Pipe()
	_ = c1.SetDeadline(time.Now().Add(3 * time.Second))
	_ = c2.SetDeadline(time.Now().Add(3 * time.Second))
	defer c1.Close()
	defer c2.Close()

	serverConn := tls.Server(c1, serverTLS)
	clientConn := tls.Client(c2, clientTLS)
	serverErrCh := make(chan error, 1)
	clientErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- serverConn.Handshake()
	}()
	go func() {
		clientErrCh <- clientConn.Handshake()
	}()
	return <-clientErrCh, <-serverErrCh
}
