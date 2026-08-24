package server

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/pkg/log"
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
	productionConfig.MetricsBearerToken = "metrics-secret"
	assert.True(t, productionConfig.EnableTLS)
	productionConfig.StorageType = "sqlite"
	productionConfig.EncryptionKeySourceType = "VAULT"

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
	config.MetricsBearerToken = "metrics-secret"
	config.EncryptionKeySourceType = "VAULT"
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
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres sslmode must be")
}

func TestValidateProductionPostgresDSN(t *testing.T) {
	t.Setenv("POSTGRES_ROOT_CERT", "")
	config := ProductionConfig()
	config.MetricsBearerToken = "metrics-secret"
	config.EncryptionKeySourceType = "VAULT"
	config.AuthEnabled = true
	config.APIKeys = []string{"secret"}
	config.EnableTLS = true
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"

	config.PostgresDSN = "postgres://user:secret@db/chronoqueue?sslmode=disable"
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres sslmode must be")

	config.PostgresDSN = "postgres://user:secret@db/chronoqueue"
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must specify sslmode")

	config.PostgresDSN = "postgres://user:super-secret@db:notaport/chronoqueue?sslmode=verify-full"
	err = config.Validate()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "super-secret")

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
	t.Setenv("HTTP_READ_HEADER_TIMEOUT", "")
	t.Setenv("HTTP_READ_TIMEOUT", "")
	t.Setenv("HTTP_WRITE_TIMEOUT", "")
	t.Setenv("HTTP_IDLE_TIMEOUT", "")
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

func TestServiceIntervalValidation(t *testing.T) {
	tests := []struct {
		name              string
		schedulerInterval string
		reclaimInterval   string
		wantError         bool
	}{
		{
			name:              "valid non-zero intervals",
			schedulerInterval: "1",
			reclaimInterval:   "1",
		},
		{
			name:              "invalid scheduler interval",
			schedulerInterval: "0",
			reclaimInterval:   "5000",
			wantError:         true,
		},
		{
			name:              "invalid reclaim interval",
			schedulerInterval: "1000",
			reclaimInterval:   "-1",
			wantError:         true,
		},
		{
			name:              "maximum representable intervals",
			schedulerInterval: "9223372036854",
			reclaimInterval:   "9223372036854",
		},
		{
			name:              "scheduler interval overflows duration",
			schedulerInterval: "9223372036855",
			reclaimInterval:   "5000",
			wantError:         true,
		},
		{
			name:              "reclaim interval overflows duration",
			schedulerInterval: "1000",
			reclaimInterval:   "9223372036855",
			wantError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SCHEDULER_INTERVAL_MS", tt.schedulerInterval)
			t.Setenv("RECLAIM_INTERVAL_MS", tt.reclaimInterval)
			config := DefaultConfig()

			err := config.Validate()
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Positive(t, config.SchedulerIntervalMs)
			require.Positive(t, config.ReclaimIntervalMs)
		})
	}
}

func TestHTTPGatewayTimeoutsFromFlags(t *testing.T) {
	t.Setenv("METRICS_BEARER_TOKEN", "metrics-secret")
	cmd := &cobra.Command{Use: "test"}
	AddServerFlags(cmd, DefaultConfig())
	require.NoError(t, cmd.ParseFlags([]string{
		"--http-read-header-timeout=6s",
		"--http-read-timeout=7s",
		"--http-write-timeout=8s",
		"--http-idle-timeout=9s",
		"--enable-encryption",
		"--encryption-key-source=LOCAL",
		"--rate-limit-enabled",
		"--rate-limit-requests-per-second=12.5",
		"--rate-limit-burst=25",
		"--rate-limit-max-buckets=500",
		"--metrics-auth-enabled",
	}))

	config, err := ParseConfigFromFlags(cmd, DefaultConfig())
	require.NoError(t, err)
	assert.Equal(t, 6*time.Second, config.HTTPReadHeaderTimeout)
	assert.Equal(t, 7*time.Second, config.HTTPReadTimeout)
	assert.Equal(t, 8*time.Second, config.HTTPWriteTimeout)
	assert.Equal(t, 9*time.Second, config.HTTPIdleTimeout)
	assert.True(t, config.EncryptionEnabled)
	assert.Equal(t, "LOCAL", config.EncryptionKeySourceType)
	assert.True(t, config.RateLimitEnabled)
	assert.Equal(t, 12.5, config.RateLimitRequestsPerSecond)
	assert.Equal(t, 25, config.RateLimitBurst)
	assert.Equal(t, 500, config.RateLimitMaxBuckets)
	assert.True(t, config.MetricsEnabled)
	assert.True(t, config.MetricsAuthEnabled)
	assert.Equal(t, "metrics-secret", config.MetricsBearerToken)
}

func TestParseConfigFromFlagsPreservesProductionDefaults(t *testing.T) {
	t.Setenv("ENABLE_CORS", "")
	t.Setenv("ALLOW_ORIGINS", "")
	t.Setenv("LOG_FORMAT", "")
	base := ProductionConfig()
	base.StorageType = "sqlite"
	base.CertFile = "server.crt"
	base.KeyFile = "server.key"
	base.AuthEnabled = true
	base.APIKeys = []string{"secret"}
	base.EncryptionKeySourceType = "VAULT"
	base.MetricsBearerToken = "metrics-secret"

	cmd := &cobra.Command{Use: "test"}
	AddServerFlags(cmd, DefaultConfig())
	config, err := ParseConfigFromFlags(cmd, base)
	require.NoError(t, err)
	assert.False(t, config.EnableCORS)
	assert.Empty(t, config.AllowOrigins)
	assert.Equal(t, "json", config.LogFormat)

	require.NoError(t, cmd.Flags().Set("enable-cors", "true"))
	require.NoError(t, cmd.Flags().Set("cors-origins", "https://console.example"))
	require.NoError(t, cmd.Flags().Set("log-format", "text"))
	config, err = ParseConfigFromFlags(cmd, base)
	require.NoError(t, err)
	assert.True(t, config.EnableCORS)
	assert.Equal(t, []string{"https://console.example"}, config.AllowOrigins)
	assert.Equal(t, "text", config.LogFormat)
}

func TestSafePostgresDSNSummary(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		contains []string
		secret   string
		want     string
	}{
		{
			name:     "URL DSN",
			dsn:      "postgres://app:p%40ss%3Aword@db.example:5433/queue?sslmode=verify-full",
			contains: []string{`host="db.example"`, `port="5433"`, `dbname="queue"`, `user="app"`, `sslmode="verify-full"`},
			secret:   "p@ss:word",
		},
		{
			name:     "keyword DSN",
			dsn:      "user=app password='secret value' host=db dbname=queue sslmode=require",
			contains: []string{`host="db"`, `dbname="queue"`, `user="app"`, `sslmode="require"`},
			secret:   "secret value",
		},
		{
			name:   "malformed URL DSN",
			dsn:    "postgres://app:super-secret@db:notaport/queue?sslmode=verify-full",
			secret: "super-secret",
			want:   "configured (details redacted)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := safePostgresDSNSummary(tt.dsn)
			if tt.want != "" {
				assert.Equal(t, tt.want, summary)
			}
			for _, expected := range tt.contains {
				assert.Contains(t, summary, expected)
			}
			assert.NotContains(t, summary, tt.secret)
			assert.NotContains(t, strings.ToLower(summary), "password")
		})
	}
}

func TestPrintStartupInfoDoesNotExposePostgresPassword(t *testing.T) {
	config := DefaultConfig()
	config.PostgresDSN = "postgres://app:super-secret@db.example/queue?sslmode=require"
	server := &Server{config: config, logger: log.NewLogger()}

	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer
	defer func() { os.Stdout = originalStdout }()

	server.printStartupInfo()
	require.NoError(t, writer.Close())
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.NotContains(t, string(output), "super-secret")
	assert.Contains(t, string(output), `host="db.example"`)
}

func TestPayloadEncryptionProductionPolicy(t *testing.T) {
	t.Setenv("ENABLE_ENCRYPTION", "")
	t.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "")
	t.Setenv("ALLOW_LOCAL_ENCRYPTION_KEY_IN_PRODUCTION", "")

	assert.False(t, DefaultConfig().EncryptionEnabled)
	config := ProductionConfig()
	config.MetricsBearerToken = "metrics-secret"
	assert.True(t, config.EncryptionEnabled)
	config.StorageType = "sqlite"
	config.AuthEnabled = true
	config.APIKeys = []string{"secret"}
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encryption key source type")

	config.EncryptionEnabled = false
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payload encryption must be enabled")

	config.EncryptionEnabled = true
	config.EncryptionKeySourceType = "LOCAL"
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit production override")

	config.AllowLocalEncryptionKeyInProduction = true
	assert.NoError(t, config.Validate())

	config.AllowLocalEncryptionKeyInProduction = false
	config.EncryptionKeySourceType = "VAULT"
	assert.NoError(t, config.Validate())
}

func TestRateLimitDefaultsAndValidation(t *testing.T) {
	t.Setenv("RATE_LIMIT_ENABLED", "")
	t.Setenv("RATE_LIMIT_REQUESTS_PER_SECOND", "")
	t.Setenv("RATE_LIMIT_BURST", "")
	t.Setenv("RATE_LIMIT_MAX_BUCKETS", "")

	assert.False(t, DefaultConfig().RateLimitEnabled)
	productionConfig := ProductionConfig()
	assert.True(t, productionConfig.RateLimitEnabled)
	assert.Equal(t, float64(100), productionConfig.RateLimitRequestsPerSecond)
	assert.Equal(t, 200, productionConfig.RateLimitBurst)
	assert.Equal(t, 10000, productionConfig.RateLimitMaxBuckets)

	config := DefaultConfig()
	config.RateLimitEnabled = true
	config.RateLimitRequestsPerSecond = 0
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit requests per second")

	config.RateLimitRequestsPerSecond = math.NaN()
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit requests per second")

	config.RateLimitRequestsPerSecond = 1
	config.RateLimitMaxBuckets = 0
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max buckets")
}

func TestMetricsDefaultsAndProductionValidation(t *testing.T) {
	t.Setenv("METRICS_ENABLED", "")
	t.Setenv("METRICS_AUTH_ENABLED", "")
	t.Setenv("METRICS_BEARER_TOKEN", "")

	developmentConfig := DefaultConfig()
	assert.True(t, developmentConfig.MetricsEnabled)
	assert.False(t, developmentConfig.MetricsAuthEnabled)
	productionConfig := ProductionConfig()
	assert.True(t, productionConfig.MetricsEnabled)
	assert.True(t, productionConfig.MetricsAuthEnabled)

	productionConfig.StorageType = "sqlite"
	productionConfig.AuthEnabled = true
	productionConfig.APIKeys = []string{"secret"}
	productionConfig.EncryptionKeySourceType = "VAULT"
	productionConfig.CertFile = "server.crt"
	productionConfig.KeyFile = "server.key"
	productionConfig.MetricsAuthEnabled = false
	err := productionConfig.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metrics authentication must be enabled")

	productionConfig.MetricsAuthEnabled = true
	err = productionConfig.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no bearer token configured")

	productionConfig.MetricsBearerToken = "metrics-secret"
	assert.NoError(t, productionConfig.Validate())

	productionConfig.MetricsEnabled = false
	productionConfig.MetricsAuthEnabled = false
	productionConfig.MetricsBearerToken = ""
	assert.NoError(t, productionConfig.Validate())
}

func TestMetricsHTTPHandler(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		metricsHTTPHandler(&Config{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		assert.Equal(t, http.StatusNotFound, recorder.Code)
	})

	t.Run("authentication required", func(t *testing.T) {
		config := &Config{MetricsEnabled: true, MetricsAuthEnabled: true, MetricsBearerToken: "metrics-secret"}
		handler := metricsHTTPHandler(config)

		for _, authorization := range []string{"", "Bearer wrong-secret", "Basic bWV0cmljczptZXRyaWNz"} {
			unauthorized := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			request.Header.Set("Authorization", authorization)
			handler.ServeHTTP(unauthorized, request)
			assert.Equal(t, http.StatusUnauthorized, unauthorized.Code)
			assert.Equal(t, `Bearer realm="ChronoQueue metrics"`, unauthorized.Header().Get("WWW-Authenticate"))
		}

		authorized := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		request.Header.Set("Authorization", "Bearer metrics-secret")
		handler.ServeHTTP(authorized, request)
		assert.Equal(t, http.StatusOK, authorized.Code)
		assert.Contains(t, authorized.Body.String(), "# HELP")
	})

	t.Run("authentication disabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		metricsHTTPHandler(&Config{MetricsEnabled: true}).ServeHTTP(
			recorder,
			httptest.NewRequest(http.MethodGet, "/metrics", nil),
		)

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "# HELP")
	})
}

func TestProductionRejectsInsecureGateway(t *testing.T) {
	config := ProductionConfig()
	config.MetricsBearerToken = "metrics-secret"
	config.StorageType = "sqlite"
	config.AuthEnabled = true
	config.APIKeys = []string{"secret"}
	config.EncryptionKeySourceType = "VAULT"
	config.CertFile = "server.crt"
	config.KeyFile = "server.key"
	config.GatewayInsecure = true

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gateway-insecure cannot be enabled in production")
}

func TestTLSConfigFromEnvironment(t *testing.T) {
	t.Setenv("CERT_FILE", "/certs/server.crt")
	t.Setenv("KEY_FILE", "/certs/server.key")
	t.Setenv("CA_CERT_FILE", "/certs/ca.crt")
	t.Setenv("GATEWAY_CLIENT_CERT_FILE", "/certs/gateway.crt")
	t.Setenv("GATEWAY_CLIENT_KEY_FILE", "/certs/gateway.key")

	config := ProductionConfig()
	assert.Equal(t, config.EnableTLS, config.GatewayUseTLS)
	assert.Equal(t, "/certs/server.crt", config.CertFile)
	assert.Equal(t, "/certs/server.key", config.KeyFile)
	assert.Equal(t, "/certs/ca.crt", config.CACertFile)
	assert.Equal(t, "/certs/gateway.crt", config.GatewayClientCertFile)
	assert.Equal(t, "/certs/gateway.key", config.GatewayClientKeyFile)
}

func TestGatewayTLSInheritsServerTLS(t *testing.T) {
	t.Setenv("CHRONOQUEUE_TLS_ENABLED", "true")
	t.Setenv("CERT_FILE", "server.crt")
	t.Setenv("KEY_FILE", "server.key")
	t.Setenv("CA_CERT_FILE", "ca.crt")
	t.Setenv("GATEWAY_CLIENT_CERT_FILE", "")
	t.Setenv("GATEWAY_CLIENT_KEY_FILE", "")

	config := DefaultConfig()
	assert.True(t, config.GatewayUseTLS)
	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gateway client cert or key file not specified")

	cmd := &cobra.Command{Use: "test"}
	AddServerFlags(cmd, config)
	require.NoError(t, cmd.Flags().Set("gateway-use-tls", "false"))
	config, err = ParseConfigFromFlags(cmd, config)
	require.Error(t, err)
	assert.False(t, config.GatewayUseTLS)
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
