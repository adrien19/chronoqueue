//go:build !sqlite
// +build !sqlite

package server

import (
	"context"
	"fmt"
)

// initializeSQLiteStorage returns an error when SQLite support is not compiled in
func (s *Server) initializeSQLiteStorage(ctx context.Context) error {
	return fmt.Errorf(`SQLite storage requested but not available.

This binary was built without SQLite support. To use SQLite:

  Option 1 - Rebuild with SQLite support:
    CGO_ENABLED=1 go build -tags sqlite -o chronoqueue .
  
  Option 2 - Use the Makefile:
    make build-full

  Option 3 - Use Redis instead (default):
    chronoqueue server --dev
    chronoqueue server --storage-type redis

Note: SQLite requires CGO and is not available by default to keep the
      binary portable and easy to cross-compile. Most production deployments
      use Redis, PostgreSQL, or Cassandra (all pure Go, no CGO needed)`)
}
