package cluster

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestActiveClient(t *testing.T) {
	t.Run("no active cluster", func(t *testing.T) {
		store := NewStore("")
		client, err := store.ActiveClient()
		if client != nil || !errors.Is(err, ErrNoActiveCluster) {
			t.Fatalf("ActiveClient() = (%v, %v), want (nil, ErrNoActiveCluster)", client, err)
		}
	})

	t.Run("invalid endpoint", func(t *testing.T) {
		store := NewStore("")
		store.Seed("invalid", "://", true)
		client, err := store.ActiveClient()
		if client != nil || err == nil {
			t.Fatalf("ActiveClient() = (%v, %v), want construction error", client, err)
		}
	})

	t.Run("successful cached acquisition", func(t *testing.T) {
		store := NewStore("")
		store.Seed("local", "127.0.0.1:1", true)
		first, err := store.ActiveClient()
		if err != nil {
			t.Fatalf("first ActiveClient(): %v", err)
		}
		defer store.CloseAll()
		second, err := store.ActiveClient()
		if err != nil {
			t.Fatalf("second ActiveClient(): %v", err)
		}
		if first != second {
			t.Fatal("ActiveClient() did not return the cached client")
		}
	})
}

func TestClientOptionsTransportAndCredentials(t *testing.T) {
	t.Run("plaintext", func(t *testing.T) {
		opts, err := clientOptions(Cluster{BrokerAddress: "localhost:9000", TransportMode: TransportPlaintext})
		if err != nil {
			t.Fatalf("clientOptions(): %v", err)
		}
		if opts.TLSCredentials != nil {
			t.Fatal("plaintext transport configured TLS credentials")
		}
	})

	t.Run("trusted TLS with server name", func(t *testing.T) {
		config, err := clusterTLSConfig(Cluster{TransportMode: TransportTLS, TLSServerName: "queue.example.com"})
		if err != nil {
			t.Fatalf("clusterTLSConfig(): %v", err)
		}
		if config.ServerName != "queue.example.com" || config.InsecureSkipVerify {
			t.Fatalf("unexpected TLS config: %+v", config)
		}
	})

	t.Run("explicit verification disable", func(t *testing.T) {
		config, err := clusterTLSConfig(Cluster{TransportMode: TransportTLS, SkipTLSVerify: true})
		if err != nil {
			t.Fatalf("clusterTLSConfig(): %v", err)
		}
		if !config.InsecureSkipVerify {
			t.Fatal("expected TLS verification to be disabled")
		}
	})

	certFile, keyFile, certPEM := writeTestCertificate(t)
	t.Run("hostname mismatch remains enforced", func(t *testing.T) {
		config, err := clusterTLSConfig(Cluster{TransportMode: TransportTLS, TLSServerName: "wrong.example.com"})
		if err != nil {
			t.Fatalf("clusterTLSConfig(): %v", err)
		}
		block, _ := pem.Decode(certPEM)
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatalf("parse certificate: %v", err)
		}
		if err := certificate.VerifyHostname(config.ServerName); err == nil {
			t.Fatal("expected hostname mismatch")
		}
	})
	t.Run("custom CA", func(t *testing.T) {
		caFile := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
			t.Fatalf("write CA: %v", err)
		}
		config, err := clusterTLSConfig(Cluster{TransportMode: TransportTLS, CACertFile: caFile})
		if err != nil {
			t.Fatalf("clusterTLSConfig(): %v", err)
		}
		if config.RootCAs == nil || config.RootCAs.Equal(x509.NewCertPool()) {
			t.Fatal("custom CA was not loaded")
		}
	})

	t.Run("mutual TLS", func(t *testing.T) {
		config, err := clusterTLSConfig(Cluster{TransportMode: TransportTLS, ClientCertFile: certFile, ClientKeyFile: keyFile})
		if err != nil {
			t.Fatalf("clusterTLSConfig(): %v", err)
		}
		if len(config.Certificates) != 1 {
			t.Fatal("client certificate was not loaded")
		}
	})

	t.Run("invalid credentials", func(t *testing.T) {
		t.Setenv("MISSING_CHRONOQUEUE_KEY", "")
		_, err := clientOptions(Cluster{BrokerAddress: "localhost:9000", TransportMode: TransportTLS, APIKeyEnv: "MISSING_CHRONOQUEUE_KEY"})
		if err == nil || !strings.Contains(err.Error(), "is not set") {
			t.Fatalf("expected missing credential error, got %v", err)
		}
	})

	t.Run("per-cluster API key reference", func(t *testing.T) {
		t.Setenv("PRODUCTION_CHRONOQUEUE_KEY", "cluster-secret")
		opts, err := clientOptions(Cluster{BrokerAddress: "localhost:9000", TransportMode: TransportPlaintext, APIKeyEnv: "PRODUCTION_CHRONOQUEUE_KEY"})
		if err != nil {
			t.Fatalf("clientOptions(): %v", err)
		}
		if opts.APIKey != "cluster-secret" {
			t.Fatalf("API key = %q, want referenced environment value", opts.APIKey)
		}
	})

	t.Run("incomplete client certificate", func(t *testing.T) {
		_, err := clientOptions(Cluster{BrokerAddress: "localhost:9000", TransportMode: TransportTLS, ClientCertFile: certFile})
		if err == nil || !strings.Contains(err.Error(), "configured together") {
			t.Fatalf("expected incomplete certificate error, got %v", err)
		}
	})
}

func TestStoreSwitchesCachedClientsByCluster(t *testing.T) {
	store := NewStore("")
	if err := store.Add(Cluster{Name: "First", Slug: "first", BrokerAddress: "localhost:9001", TransportMode: TransportPlaintext}); err != nil {
		t.Fatalf("add first cluster: %v", err)
	}
	if err := store.Add(Cluster{Name: "Second", Slug: "second", BrokerAddress: "localhost:9002", TransportMode: TransportPlaintext}); err != nil {
		t.Fatalf("add second cluster: %v", err)
	}
	defer store.CloseAll()
	first, err := store.ActiveClient()
	if err != nil {
		t.Fatalf("first ActiveClient(): %v", err)
	}
	if err := store.SetActive("second"); err != nil {
		t.Fatalf("SetActive(second): %v", err)
	}
	second, err := store.ActiveClient()
	if err != nil {
		t.Fatalf("second ActiveClient(): %v", err)
	}
	if first == second {
		t.Fatal("switching clusters reused the wrong client")
	}
	if err := store.SetActive("first"); err != nil {
		t.Fatalf("SetActive(first): %v", err)
	}
	firstAgain, err := store.ActiveClient()
	if err != nil {
		t.Fatalf("third ActiveClient(): %v", err)
	}
	if firstAgain != first {
		t.Fatal("switching back did not reuse the correct cached client")
	}
}

func TestStoreMigratesLegacyTransportAndInvalidatesChangedClients(t *testing.T) {
	storeFile := filepath.Join(t.TempDir(), "clusters.json")
	legacy := `[{"slug":"local","name":"Local","brokerAddress":"localhost:9000","skipSSLCheck":true,"isActive":true}]`
	if err := os.WriteFile(storeFile, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy store: %v", err)
	}
	store := NewStore(storeFile)
	if err := store.Load(); err != nil {
		t.Fatalf("Load(): %v", err)
	}
	loaded, ok := store.Get("local")
	if !ok || loaded.TransportMode != TransportPlaintext {
		t.Fatalf("legacy transport = %q, want plaintext", loaded.TransportMode)
	}

	first, err := store.ActiveClient()
	if err != nil {
		t.Fatalf("first ActiveClient(): %v", err)
	}
	defer store.CloseAll()
	if err := store.Update("local", Cluster{Name: "Local", BrokerAddress: "localhost:9000", TransportMode: TransportTLS, SkipTLSVerify: true}); err != nil {
		t.Fatalf("Update(): %v", err)
	}
	second, err := store.ActiveClient()
	if err != nil {
		t.Fatalf("second ActiveClient(): %v", err)
	}
	if first == second {
		t.Fatal("connection configuration change did not invalidate the cached client")
	}
}

func TestStorePersistsCredentialReferencesNotSecrets(t *testing.T) {
	storeFile := filepath.Join(t.TempDir(), "clusters.json")
	t.Setenv("PRODUCTION_CHRONOQUEUE_KEY", "do-not-persist")
	store := NewStore(storeFile)
	if err := store.Add(Cluster{Name: "Production", BrokerAddress: "localhost:9000", TransportMode: TransportTLS, APIKeyEnv: "PRODUCTION_CHRONOQUEUE_KEY"}); err != nil {
		t.Fatalf("Add(): %v", err)
	}
	data, err := os.ReadFile(storeFile)
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	if strings.Contains(string(data), "do-not-persist") || !strings.Contains(string(data), "PRODUCTION_CHRONOQUEUE_KEY") {
		t.Fatalf("unexpected persisted credentials: %s", data)
	}
	var persisted []map[string]any
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("parse store: %v", err)
	}
}

func writeTestCertificate(t *testing.T) (string, string, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "queue.example.com"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"queue.example.com"}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile, certPEM
}
