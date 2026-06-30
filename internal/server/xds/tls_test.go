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

// generateTestCert creates a self-signed CA + server cert + client cert
// with the given URI SAN for testing.
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
	os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER}), 0644)
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
	os.WriteFile(serverCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER}), 0644)
	serverKeyDER, _ := x509.MarshalECPrivateKey(serverKey)
	os.WriteFile(serverKeyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}), 0644)

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
	os.WriteFile(clientCertPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCertDER}), 0644)
	clientKeyDER, _ := x509.MarshalECPrivateKey(clientKey)
	os.WriteFile(clientKeyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER}), 0644)

	return caPath, serverCertPath, serverKeyPath
}

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

func TestTLSConfigBadCA(t *testing.T) {
	dir := t.TempDir()
	_, serverCertPath, serverKeyPath := generateTestCert(t, dir, "")

	// Write invalid CA cert
	badCAPath := filepath.Join(dir, "bad-ca.crt")
	os.WriteFile(badCAPath, []byte("not a cert"), 0644)

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
