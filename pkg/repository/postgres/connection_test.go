package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionConfig_DSN(t *testing.T) {
	tests := []struct {
		name     string
		config   ConnectionConfig
		expected string
	}{
		{
			name: "basic connection without certificates",
			config: ConnectionConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "testuser",
				Password: "testpass",
				Database: "testdb",
				SSLMode:  "disable",
			},
			expected: "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=disable",
		},
		{
			name: "connection with client certificate only",
			config: ConnectionConfig{
				Host:           "localhost",
				Port:           5432,
				User:           "testuser",
				Password:       "testpass",
				Database:       "testdb",
				SSLMode:        "verify-full",
				ClientCertFile: "/certs/client.crt",
			},
			expected: "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=verify-full sslcert=/certs/client.crt",
		},
		{
			name: "connection with client certificate and key",
			config: ConnectionConfig{
				Host:           "localhost",
				Port:           5432,
				User:           "testuser",
				Password:       "testpass",
				Database:       "testdb",
				SSLMode:        "verify-full",
				ClientCertFile: "/certs/client.crt",
				ClientKeyFile:  "/certs/client.key",
			},
			expected: "host=localhost port=5432 user=testuser password=testpass dbname=testdb sslmode=verify-full sslcert=/certs/client.crt sslkey=/certs/client.key",
		},
		{
			name: "full mTLS configuration",
			config: ConnectionConfig{
				Host:           "pg.example.com",
				Port:           5432,
				User:           "appuser",
				Password:       "secret",
				Database:       "production",
				SSLMode:        "verify-full",
				ClientCertFile: "/certs/client.crt",
				ClientKeyFile:  "/certs/client.key",
				RootCertFile:   "/certs/ca.crt",
			},
			expected: "host=pg.example.com port=5432 user=appuser password=secret dbname=production sslmode=verify-full sslcert=/certs/client.crt sslkey=/certs/client.key sslrootcert=/certs/ca.crt",
		},
		{
			name: "root certificate only",
			config: ConnectionConfig{
				Host:         "pg.example.com",
				Port:         5432,
				User:         "appuser",
				Password:     "secret",
				Database:     "production",
				SSLMode:      "verify-ca",
				RootCertFile: "/certs/ca.crt",
			},
			expected: "host=pg.example.com port=5432 user=appuser password=secret dbname=production sslmode=verify-ca sslrootcert=/certs/ca.crt",
		},
		{
			name: "custom DSN overrides all",
			config: ConnectionConfig{
				DSN:            "postgresql://custom:conn@string/db?sslmode=require",
				Host:           "ignored",
				Port:           5432,
				User:           "ignored",
				Password:       "ignored",
				Database:       "ignored",
				SSLMode:        "ignored",
				ClientCertFile: "ignored",
				ClientKeyFile:  "ignored",
				RootCertFile:   "ignored",
			},
			expected: "postgresql://custom:conn@string/db?sslmode=require",
		},
		{
			name: "different port",
			config: ConnectionConfig{
				Host:           "pg.example.com",
				Port:           5433,
				User:           "appuser",
				Password:       "secret",
				Database:       "production",
				SSLMode:        "require",
				ClientCertFile: "/certs/client.crt",
				ClientKeyFile:  "/certs/client.key",
			},
			expected: "host=pg.example.com port=5433 user=appuser password=secret dbname=production sslmode=require sslcert=/certs/client.crt sslkey=/certs/client.key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.config.dsn()
			assert.Equal(t, tt.expected, actual, "DSN mismatch")
		})
	}
}

func TestConnectionConfig_DSN_EmptyValues(t *testing.T) {
	// Test that empty certificate paths don't add parameters
	config := ConnectionConfig{
		Host:           "localhost",
		Port:           5432,
		User:           "user",
		Password:       "pass",
		Database:       "db",
		SSLMode:        "disable",
		ClientCertFile: "",
		ClientKeyFile:  "",
		RootCertFile:   "",
	}

	expected := "host=localhost port=5432 user=user password=pass dbname=db sslmode=disable"
	actual := config.dsn()

	assert.Equal(t, expected, actual)
	assert.NotContains(t, actual, "sslcert")
	assert.NotContains(t, actual, "sslkey")
	assert.NotContains(t, actual, "sslrootcert")
}

func TestConnectionConfig_DSN_PartialCertificates(t *testing.T) {
	tests := []struct {
		name        string
		config      ConnectionConfig
		contains    []string
		notContains []string
	}{
		{
			name: "only client cert",
			config: ConnectionConfig{
				Host:           "localhost",
				Port:           5432,
				User:           "user",
				Password:       "pass",
				Database:       "db",
				SSLMode:        "require",
				ClientCertFile: "/certs/client.crt",
			},
			contains:    []string{"sslcert=/certs/client.crt"},
			notContains: []string{"sslkey=", "sslrootcert="},
		},
		{
			name: "only client key (unusual but allowed)",
			config: ConnectionConfig{
				Host:          "localhost",
				Port:          5432,
				User:          "user",
				Password:      "pass",
				Database:      "db",
				SSLMode:       "require",
				ClientKeyFile: "/certs/client.key",
			},
			contains:    []string{"sslkey=/certs/client.key"},
			notContains: []string{"sslcert=", "sslrootcert="},
		},
		{
			name: "only root cert",
			config: ConnectionConfig{
				Host:         "localhost",
				Port:         5432,
				User:         "user",
				Password:     "pass",
				Database:     "db",
				SSLMode:      "verify-ca",
				RootCertFile: "/certs/ca.crt",
			},
			contains:    []string{"sslrootcert=/certs/ca.crt"},
			notContains: []string{"sslcert=", "sslkey="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.config.dsn()
			for _, s := range tt.contains {
				assert.Contains(t, actual, s)
			}
			for _, s := range tt.notContains {
				assert.NotContains(t, actual, s)
			}
		})
	}
}
