package server

import (
	"testing"

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
