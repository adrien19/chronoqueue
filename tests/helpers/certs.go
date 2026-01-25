package helpers

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCertificates holds paths to test certificates for TLS/mTLS testing.
type TestCertificates struct {
	CACert     string // Path to CA certificate
	ServerCert string // Path to server certificate
	ServerKey  string // Path to server key
	ClientCert string // Path to client certificate
	ClientKey  string // Path to client key
	CAPool     *x509.CertPool
	TempDir    string // Temporary directory for certificates
}

// GenerateTestCertificates creates a complete set of test certificates for TLS/mTLS testing.
// It generates:
// - A root CA certificate
// - A server certificate signed by the CA
// - A client certificate signed by the CA
//
// All certificates are written to a temporary directory that is automatically
// cleaned up when the test completes.
//
// Example:
//
//	func TestWithTLS(t *testing.T) {
//	    certs := helpers.GenerateTestCertificates(t)
//	    // Use certs.ServerCert, certs.ServerKey, etc.
//	}
func GenerateTestCertificates(t *testing.T) *TestCertificates {
	// Create temporary directory for certificates
	tempDir, err := os.MkdirTemp("", "chronoqueue-test-certs-*")
	require.NoError(t, err, "Failed to create temp directory for certificates")

	// Cleanup temp directory when test completes
	t.Cleanup(func() {
		_ = os.RemoveAll(tempDir)
	})

	// Generate CA
	caKey, caCert := generateCA(t)
	caKeyPath := filepath.Join(tempDir, "ca.key")
	caCertPath := filepath.Join(tempDir, "ca.crt")
	writeKeyFile(t, caKeyPath, caKey)
	writeCertFile(t, caCertPath, caCert)

	// Generate server certificate
	serverKey, serverCert := generateCert(t, caCert, caKey, "localhost", false)
	serverKeyPath := filepath.Join(tempDir, "server.key")
	serverCertPath := filepath.Join(tempDir, "server.crt")
	writeKeyFile(t, serverKeyPath, serverKey)
	writeCertFile(t, serverCertPath, serverCert)

	// Generate client certificate
	clientKey, clientCert := generateCert(t, caCert, caKey, "test-client", true)
	clientKeyPath := filepath.Join(tempDir, "client.key")
	clientCertPath := filepath.Join(tempDir, "client.crt")
	writeKeyFile(t, clientKeyPath, clientKey)
	writeCertFile(t, clientCertPath, clientCert)

	// Create CA pool for verification
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	t.Logf("Generated test certificates in: %s", tempDir)

	return &TestCertificates{
		CACert:     caCertPath,
		ServerCert: serverCertPath,
		ServerKey:  serverKeyPath,
		ClientCert: clientCertPath,
		ClientKey:  clientKeyPath,
		CAPool:     caPool,
		TempDir:    tempDir,
	}
}

// LoadClientTLSConfig creates a TLS configuration for a client using the test certificates.
// This is useful for creating gRPC clients with mTLS.
func (c *TestCertificates) LoadClientTLSConfig(t *testing.T) *tls.Config {
	// Load client certificate
	clientCert, err := tls.LoadX509KeyPair(c.ClientCert, c.ClientKey)
	require.NoError(t, err, "Failed to load client certificate")

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      c.CAPool,
		ServerName:   "localhost",
	}
}

// generateCA creates a new root CA certificate.
func generateCA(t *testing.T) (*rsa.PrivateKey, *x509.Certificate) {
	// Generate RSA key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "Failed to generate CA key")

	// Create CA certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"ChronoQueue Test CA"},
			CommonName:   "ChronoQueue Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Self-sign the CA certificate
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err, "Failed to create CA certificate")

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err, "Failed to parse CA certificate")

	return key, cert
}

// generateCert creates a new certificate signed by the CA.
func generateCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, commonName string, isClient bool) (*rsa.PrivateKey, *x509.Certificate) {
	// Generate RSA key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "Failed to generate key")

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err, "Failed to generate serial number")

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"ChronoQueue Test"},
			CommonName:   commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	// Add DNS names for server certificates
	if !isClient {
		template.DNSNames = []string{"localhost", "127.0.0.1"}
	}

	// Sign the certificate with the CA
	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	require.NoError(t, err, "Failed to create certificate")

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err, "Failed to parse certificate")

	return key, cert
}

// writeKeyFile writes a private key to a PEM file.
func writeKeyFile(t *testing.T, path string, key *rsa.PrivateKey) {
	keyFile, err := os.Create(path)
	require.NoError(t, err, fmt.Sprintf("Failed to create key file: %s", path))
	defer func() { _ = keyFile.Close() }()

	keyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}

	err = pem.Encode(keyFile, keyPEM)
	require.NoError(t, err, fmt.Sprintf("Failed to write key file: %s", path))
}

// writeCertFile writes a certificate to a PEM file.
func writeCertFile(t *testing.T, path string, cert *x509.Certificate) {
	certFile, err := os.Create(path)
	require.NoError(t, err, fmt.Sprintf("Failed to create cert file: %s", path))
	defer func() { _ = certFile.Close() }()

	certPEM := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	}

	err = pem.Encode(certFile, certPEM)
	require.NoError(t, err, fmt.Sprintf("Failed to write cert file: %s", path))
}
