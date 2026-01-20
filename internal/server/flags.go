package server

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// AddServerFlags adds server configuration flags to a cobra command
func AddServerFlags(cmd *cobra.Command, config *Config) {
	cmd.Flags().StringVar(&config.GRPCAddr, "grpc-addr", config.GRPCAddr, "gRPC server address")
	cmd.Flags().StringVar(&config.HTTPAddr, "http-addr", config.HTTPAddr, "HTTP gateway address")
	cmd.Flags().StringVar(&config.StorageType, "storage-type", config.StorageType, "Storage backend type (redis, sqlite, postgres)")
	cmd.Flags().StringVar(&config.RedisAddr, "redis-addr", config.RedisAddr, "Redis server address")
	cmd.Flags().StringVar(&config.RedisPassword, "redis-password", config.RedisPassword, "Redis password")
	cmd.Flags().StringVar(&config.RedisUsername, "redis-username", config.RedisUsername, "Redis username (ACL)")
	cmd.Flags().IntVar(&config.RedisDB, "redis-db", config.RedisDB, "Redis database number")
	cmd.Flags().BoolVar(&config.RedisTLS, "redis-tls", config.RedisTLS, "Enable TLS for Redis")
	cmd.Flags().StringVar(&config.SQLiteDBPath, "sqlite-db-path", config.SQLiteDBPath, "Path to SQLite database file")
	cmd.Flags().StringVar(&config.PostgresDSN, "postgres-dsn", config.PostgresDSN, "PostgreSQL DSN (overrides discrete flags)")
	cmd.Flags().StringVar(&config.PostgresHost, "postgres-host", config.PostgresHost, "PostgreSQL host")
	cmd.Flags().IntVar(&config.PostgresPort, "postgres-port", config.PostgresPort, "PostgreSQL port")
	cmd.Flags().StringVar(&config.PostgresUser, "postgres-user", config.PostgresUser, "PostgreSQL user")
	cmd.Flags().StringVar(&config.PostgresPassword, "postgres-password", config.PostgresPassword, "PostgreSQL password")
	cmd.Flags().StringVar(&config.PostgresDBName, "postgres-db", config.PostgresDBName, "PostgreSQL database name")
	cmd.Flags().StringVar(&config.PostgresSSLMode, "postgres-sslmode", config.PostgresSSLMode, "PostgreSQL sslmode (disable, require, verify-full, etc.)")

	// Convenience flags for SQLite (shortcuts)
	cmd.Flags().StringP("database", "d", "", "SQLite database file path (shortcut for --storage-type sqlite --sqlite-db-path)")
	cmd.Flags().String("db", "", "SQLite database file path (shortcut for --storage-type sqlite --sqlite-db-path)")

	cmd.Flags().StringVar(&config.LogLevel, "log-level", config.LogLevel, "Log level (debug, info, warn, error)")
	cmd.Flags().StringVar(&config.LogFormat, "log-format", config.LogFormat, "Log format (text, json)")
	cmd.Flags().BoolVar(&config.EnableTLS, "enable-tls", config.EnableTLS, "Enable TLS")
	cmd.Flags().StringVar(&config.CertFile, "cert-file", config.CertFile, "TLS certificate file")
	cmd.Flags().StringVar(&config.KeyFile, "key-file", config.KeyFile, "TLS key file")
	cmd.Flags().StringVar(&config.CACertFile, "ca-cert-file", config.CACertFile, "CA certificate file for mutual TLS (optional)")
	cmd.Flags().BoolVar(&config.EnableCORS, "enable-cors", config.EnableCORS, "Enable CORS for HTTP gateway")
	cmd.Flags().StringSliceVar(&config.AllowOrigins, "cors-origins", config.AllowOrigins, "Allowed CORS origins")
}

// AddServerFlagsLegacy adds server configuration flags using pflag (for backward compatibility)
func AddServerFlagsLegacy(config *Config) {
	pflag.StringVar(&config.GRPCAddr, "grpc-addr", config.GRPCAddr, "gRPC server address")
	pflag.StringVar(&config.HTTPAddr, "http-addr", config.HTTPAddr, "HTTP gateway address")
	pflag.StringVar(&config.StorageType, "storage-type", config.StorageType, "Storage backend type (redis, sqlite, postgres)")
	pflag.StringVar(&config.RedisAddr, "redis-addr", config.RedisAddr, "Redis server address")
	pflag.StringVar(&config.RedisPassword, "redis-password", config.RedisPassword, "Redis password")
	pflag.StringVar(&config.RedisUsername, "redis-username", config.RedisUsername, "Redis username (ACL)")
	pflag.IntVar(&config.RedisDB, "redis-db", config.RedisDB, "Redis database number")
	pflag.BoolVar(&config.RedisTLS, "redis-tls", config.RedisTLS, "Enable TLS for Redis")
	pflag.StringVar(&config.SQLiteDBPath, "sqlite-db-path", config.SQLiteDBPath, "Path to SQLite database file")
	pflag.StringVar(&config.PostgresDSN, "postgres-dsn", config.PostgresDSN, "PostgreSQL DSN (overrides discrete flags)")
	pflag.StringVar(&config.PostgresHost, "postgres-host", config.PostgresHost, "PostgreSQL host")
	pflag.IntVar(&config.PostgresPort, "postgres-port", config.PostgresPort, "PostgreSQL port")
	pflag.StringVar(&config.PostgresUser, "postgres-user", config.PostgresUser, "PostgreSQL user")
	pflag.StringVar(&config.PostgresPassword, "postgres-password", config.PostgresPassword, "PostgreSQL password")
	pflag.StringVar(&config.PostgresDBName, "postgres-db", config.PostgresDBName, "PostgreSQL database name")
	pflag.StringVar(&config.PostgresSSLMode, "postgres-sslmode", config.PostgresSSLMode, "PostgreSQL sslmode (disable, require, verify-full, etc.)")
	pflag.StringVar(&config.LogLevel, "log-level", config.LogLevel, "Log level (debug, info, warn, error)")
	pflag.StringVar(&config.LogFormat, "log-format", config.LogFormat, "Log format (text, json)")
	pflag.BoolVar(&config.EnableTLS, "enable-tls", config.EnableTLS, "Enable TLS")
	pflag.StringVar(&config.CertFile, "cert-file", config.CertFile, "TLS certificate file")
	pflag.StringVar(&config.KeyFile, "key-file", config.KeyFile, "TLS key file")
	pflag.StringVar(&config.CACertFile, "ca-cert-file", config.CACertFile, "CA certificate file for mutual TLS (optional)")
	pflag.BoolVar(&config.EnableCORS, "enable-cors", config.EnableCORS, "Enable CORS for HTTP gateway")
	pflag.StringSliceVar(&config.AllowOrigins, "cors-origins", config.AllowOrigins, "Allowed CORS origins")
}

// ParseConfigFromFlags parses configuration from cobra command flags
func ParseConfigFromFlags(cmd *cobra.Command) (*Config, error) {
	config := DefaultConfig()

	if cmd.Flags().Changed("grpc-addr") {
		config.GRPCAddr, _ = cmd.Flags().GetString("grpc-addr")
	}
	if cmd.Flags().Changed("http-addr") {
		config.HTTPAddr, _ = cmd.Flags().GetString("http-addr")
	}
	if cmd.Flags().Changed("storage-type") {
		config.StorageType, _ = cmd.Flags().GetString("storage-type")
	}

	// Handle convenience flags for SQLite (--database, --db, -d)
	// These automatically set storage-type to sqlite and set the db path
	database, _ := cmd.Flags().GetString("database")
	db, _ := cmd.Flags().GetString("db")

	if database != "" {
		config.StorageType = "sqlite"
		config.SQLiteDBPath = database
	} else if db != "" {
		config.StorageType = "sqlite"
		config.SQLiteDBPath = db
	}

	if cmd.Flags().Changed("redis-addr") {
		config.RedisAddr, _ = cmd.Flags().GetString("redis-addr")
	}
	if cmd.Flags().Changed("redis-password") {
		config.RedisPassword, _ = cmd.Flags().GetString("redis-password")
	}
	if cmd.Flags().Changed("redis-username") {
		config.RedisUsername, _ = cmd.Flags().GetString("redis-username")
	}
	if cmd.Flags().Changed("redis-db") {
		config.RedisDB, _ = cmd.Flags().GetInt("redis-db")
	}
	if cmd.Flags().Changed("redis-tls") {
		config.RedisTLS, _ = cmd.Flags().GetBool("redis-tls")
	}
	if cmd.Flags().Changed("sqlite-db-path") {
		config.SQLiteDBPath, _ = cmd.Flags().GetString("sqlite-db-path")
	}
	if cmd.Flags().Changed("postgres-dsn") {
		config.PostgresDSN, _ = cmd.Flags().GetString("postgres-dsn")
	}
	if cmd.Flags().Changed("postgres-host") {
		config.PostgresHost, _ = cmd.Flags().GetString("postgres-host")
	}
	if cmd.Flags().Changed("postgres-port") {
		config.PostgresPort, _ = cmd.Flags().GetInt("postgres-port")
	}
	if cmd.Flags().Changed("postgres-user") {
		config.PostgresUser, _ = cmd.Flags().GetString("postgres-user")
	}
	if cmd.Flags().Changed("postgres-password") {
		config.PostgresPassword, _ = cmd.Flags().GetString("postgres-password")
	}
	if cmd.Flags().Changed("postgres-db") {
		config.PostgresDBName, _ = cmd.Flags().GetString("postgres-db")
	}
	if cmd.Flags().Changed("postgres-sslmode") {
		config.PostgresSSLMode, _ = cmd.Flags().GetString("postgres-sslmode")
	}
	if cmd.Flags().Changed("log-level") {
		config.LogLevel, _ = cmd.Flags().GetString("log-level")
	}
	if cmd.Flags().Changed("log-format") {
		config.LogFormat, _ = cmd.Flags().GetString("log-format")
	}
	if cmd.Flags().Changed("enable-tls") {
		config.EnableTLS, _ = cmd.Flags().GetBool("enable-tls")
	}
	if cmd.Flags().Changed("cert-file") {
		config.CertFile, _ = cmd.Flags().GetString("cert-file")
	}
	if cmd.Flags().Changed("key-file") {
		config.KeyFile, _ = cmd.Flags().GetString("key-file")
	}
	if cmd.Flags().Changed("ca-cert-file") {
		config.CACertFile, _ = cmd.Flags().GetString("ca-cert-file")
	}
	if cmd.Flags().Changed("enable-cors") {
		config.EnableCORS, _ = cmd.Flags().GetBool("enable-cors")
	}
	if cmd.Flags().Changed("cors-origins") {
		config.AllowOrigins, _ = cmd.Flags().GetStringSlice("cors-origins")
	}

	return config, config.Validate()
}
