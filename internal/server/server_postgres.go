package server

import (
	"context"
	"fmt"

	"github.com/adrien19/chronoqueue/pkg/repository"
	postgresrepository "github.com/adrien19/chronoqueue/pkg/repository/postgres"
	"github.com/adrien19/chronoqueue/pkg/schema"
)

// initializePostgresStorage initializes PostgreSQL storage and schema registry.
func (s *Server) initializePostgresStorage(ctx context.Context) error {
	connConfig := postgresrepository.ConnectionConfig{
		DSN:      s.config.PostgresDSN,
		Host:     s.config.PostgresHost,
		Port:     s.config.PostgresPort,
		User:     s.config.PostgresUser,
		Password: s.config.PostgresPassword,
		Database: s.config.PostgresDBName,
		SSLMode:  s.config.PostgresSSLMode,
	}

	storage, err := repository.NewPostgresStorage(ctx, &postgresrepository.Config{
		Conn:       connConfig,
		Logger:     s.logger,
		KeyManager: s.encryptionKeyManager,
	})
	if err != nil {
		return fmt.Errorf("failed to create Postgres repository: %w", err)
	}

	s.database = storage
	s.logger.InfoWithFields("Postgres repository initialized",
		"host", s.config.PostgresHost,
		"port", s.config.PostgresPort,
		"database", s.config.PostgresDBName,
		"user", s.config.PostgresUser,
		"sslmode", s.config.PostgresSSLMode,
	)

	db, err := postgresrepository.OpenConnection(ctx, &connConfig)
	if err != nil {
		return fmt.Errorf("failed to open connection for schema registry: %w", err)
	}

	registry, err := schema.NewPostgresRegistry(db, s.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize Postgres schema registry: %w", err)
	}

	s.schemaRegistry = registry
	s.logger.Info("Postgres schema registry initialized")

	return nil
}
