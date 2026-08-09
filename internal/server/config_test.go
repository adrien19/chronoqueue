package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticationDefaults(t *testing.T) {
	t.Setenv("AUTH_ENABLED", "")
	t.Setenv("API_KEYS", "")

	assert.False(t, DefaultConfig().AuthEnabled)
	assert.True(t, ProductionConfig().AuthEnabled)
}

func TestValidateAuthentication(t *testing.T) {
	config := DefaultConfig()
	config.AuthEnabled = true

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no API keys configured")

	config.APIKeys = []string{"secret"}
	assert.NoError(t, config.Validate())

	config.APIKeys = []string{""}
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be empty")
}

func TestTLSDefaultsAndProductionValidation(t *testing.T) {
	t.Setenv("CHRONOQUEUE_TLS_ENABLED", "")
	t.Setenv("CERT_FILE", "")
	t.Setenv("KEY_FILE", "")
	t.Setenv("CA_CERT_FILE", "")

	assert.False(t, DefaultConfig().EnableTLS)
	productionConfig := ProductionConfig()
	assert.True(t, productionConfig.EnableTLS)
	productionConfig.StorageType = "sqlite"

	productionConfig.AuthEnabled = true
	productionConfig.APIKeys = []string{"secret"}
	productionConfig.EnableTLS = false
	err := productionConfig.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS must be enabled in production")

	productionConfig.EnableTLS = true
	err = productionConfig.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cert-file or key-file not specified")

	productionConfig.CertFile = "server.crt"
	productionConfig.KeyFile = "server.key"
	assert.NoError(t, productionConfig.Validate())
}

func TestPostgresSecurityDefaults(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_SSLMODE", "")
	t.Setenv("POSTGRES_ROOT_CERT", "")

	developmentConfig := DefaultConfig()
	assert.Equal(t, "chronoqueue", developmentConfig.PostgresPassword)
	assert.Equal(t, "disable", developmentConfig.PostgresSSLMode)

	productionConfig := ProductionConfig()
	assert.Empty(t, productionConfig.PostgresPassword)
	assert.Equal(t, "verify-full", productionConfig.PostgresSSLMode)
}

func TestValidateProductionPostgresSecurity(t *testing.T) {
	config := ProductionConfig()
	config.AuthEnabled = true
	config.APIKeys = []string{"secret"}
	config.EnableTLS = true
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"
	config.PostgresPassword = ""
	config.PostgresSSLMode = "verify-full"
	config.PostgresRootCertFile = ""

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres password is required")

	config.PostgresPassword = "secret"
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres-root-cert is required")

	config.PostgresRootCertFile = "root.crt"
	assert.NoError(t, config.Validate())

	config.PostgresSSLMode = "disable"
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres sslmode must be")

	config.PostgresSSLMode = "require"
	config.PostgresRootCertFile = ""
	assert.NoError(t, config.Validate())
}

func TestValidateProductionPostgresDSN(t *testing.T) {
	config := ProductionConfig()
	config.AuthEnabled = true
	config.APIKeys = []string{"secret"}
	config.EnableTLS = true
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"

	config.PostgresDSN = "postgres://user:secret@db/chronoqueue?sslmode=disable"
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres sslmode must be")

	config.PostgresDSN = "host=db user=user password=secret dbname=chronoqueue sslmode=verify-full"
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres-root-cert is required")

	config.PostgresDSN = "host=db user=user password=secret dbname=chronoqueue sslmode=verify-full sslrootcert=/certs/root.crt"
	assert.NoError(t, config.Validate())
}

func TestValidatePostgresClientCertificatePair(t *testing.T) {
	config := DefaultConfig()
	config.PostgresClientCertFile = "client.crt"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres client cert and key files must be specified together")

	config.PostgresClientKeyFile = "client.key"
	assert.NoError(t, config.Validate())
}

func TestHTTPGatewayTimeouts(t *testing.T) {
	config := DefaultConfig()
	assert.Equal(t, 5*time.Second, config.HTTPReadHeaderTimeout)
	assert.Equal(t, 15*time.Second, config.HTTPReadTimeout)
	assert.Equal(t, 30*time.Second, config.HTTPWriteTimeout)
	assert.Equal(t, 60*time.Second, config.HTTPIdleTimeout)

	server := (&Server{config: config}).newHTTPServer(http.NotFoundHandler())
	assert.Equal(t, config.HTTPReadHeaderTimeout, server.ReadHeaderTimeout)
	assert.Equal(t, config.HTTPReadTimeout, server.ReadTimeout)
	assert.Equal(t, config.HTTPWriteTimeout, server.WriteTimeout)
	assert.Equal(t, config.HTTPIdleTimeout, server.IdleTimeout)

	config.HTTPReadHeaderTimeout = 0
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP gateway timeouts must be greater than 0")
}

func TestHTTPGatewayTimeoutsFromEnvironment(t *testing.T) {
	t.Setenv("HTTP_READ_HEADER_TIMEOUT", "2s")
	t.Setenv("HTTP_READ_TIMEOUT", "3s")
	t.Setenv("HTTP_WRITE_TIMEOUT", "4s")
	t.Setenv("HTTP_IDLE_TIMEOUT", "5s")

	config := DefaultConfig()
	assert.Equal(t, 2*time.Second, config.HTTPReadHeaderTimeout)
	assert.Equal(t, 3*time.Second, config.HTTPReadTimeout)
	assert.Equal(t, 4*time.Second, config.HTTPWriteTimeout)
	assert.Equal(t, 5*time.Second, config.HTTPIdleTimeout)
}

func TestHTTPGatewayTimeoutsFromFlags(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	AddServerFlags(cmd, DefaultConfig())
	require.NoError(t, cmd.ParseFlags([]string{
		"--http-read-header-timeout=6s",
		"--http-read-timeout=7s",
		"--http-write-timeout=8s",
		"--http-idle-timeout=9s",
	}))

	config, err := ParseConfigFromFlags(cmd)
	require.NoError(t, err)
	assert.Equal(t, 6*time.Second, config.HTTPReadHeaderTimeout)
	assert.Equal(t, 7*time.Second, config.HTTPReadTimeout)
	assert.Equal(t, 8*time.Second, config.HTTPWriteTimeout)
	assert.Equal(t, 9*time.Second, config.HTTPIdleTimeout)
}

func TestTLSConfigFromEnvironment(t *testing.T) {
	t.Setenv("CERT_FILE", "/certs/server.crt")
	t.Setenv("KEY_FILE", "/certs/server.key")
	t.Setenv("CA_CERT_FILE", "/certs/ca.crt")
	t.Setenv("GATEWAY_CLIENT_CERT_FILE", "/certs/gateway.crt")
	t.Setenv("GATEWAY_CLIENT_KEY_FILE", "/certs/gateway.key")

	config := ProductionConfig()
	assert.Equal(t, "/certs/server.crt", config.CertFile)
	assert.Equal(t, "/certs/server.key", config.KeyFile)
	assert.Equal(t, "/certs/ca.crt", config.CACertFile)
	assert.Equal(t, "/certs/gateway.crt", config.GatewayClientCertFile)
	assert.Equal(t, "/certs/gateway.key", config.GatewayClientKeyFile)
}

func TestValidateGatewayMTLSCredentials(t *testing.T) {
	config := DefaultConfig()
	config.EnableTLS = true
	config.GatewayUseTLS = true
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"
	config.CACertFile = "ca.crt"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gateway client cert or key file not specified")

	config.GatewayClientCertFile = "gateway.crt"
	config.GatewayClientKeyFile = "gateway.key"
	assert.NoError(t, config.Validate())
}
