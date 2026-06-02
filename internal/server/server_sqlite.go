//go:build sqlite
// +build sqlite

package server

import (
	"context"
	"fmt"

	_ "github.com/mattn/go-sqlite3"

	"github.com/adrien19/chronoqueue/pkg/repository"
	sqliterepository "github.com/adrien19/chronoqueue/pkg/repository/sqlite"
	"github.com/adrien19/chronoqueue/pkg/schema"
)

// initializeSQLiteStorage initializes SQLite storage and schema registry
func (s *Server) initializeSQLiteStorage(ctx context.Context) error {
	// Create repository using SQLite backend
	storage, err := repository.NewSQLiteStorage(ctx, &sqliterepository.Config{
		Path:       s.config.SQLiteDBPath,
		Logger:     s.logger,
		KeyManager: s.encryptionKeyManager,
	})
	if err != nil {
		return fmt.Errorf("failed to create SQLite repository: %w", err)
	}

	s.logger.InfoWithFields(
		"SQLite repository initialized",
		"path", s.config.SQLiteDBPath,
	)

	// Initialize SQLite schema registry
	connConfig := sqliterepository.DefaultConnectionConfig(s.config.SQLiteDBPath)
	db, err := sqliterepository.OpenConnection(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("failed to open connection for schema registry: %w", err)
	}

	sqliteRegistry, err := schema.NewSQLiteRegistry(db, s.logger)
	if err != nil {
		return fmt.Errorf("failed to initialize SQLite schema registry: %w", err)
	}

	// Store schema registry for use by ChronoQueueServer
	s.schemaRegistry = sqliteRegistry
	s.logger.Info("SQLite schema registry initialized")

	// Set the repository as the database
	s.database = storage
	s.logger.Info("SQLite storage backend ready")

	return nil
}
