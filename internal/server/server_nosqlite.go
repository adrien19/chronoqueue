//go:build !sqlite
// +build !sqlite

package server

import (
	"context"
	"fmt"
)

// initializeSQLiteStorage returns an error when SQLite support is not compiled in
func (s *Server) initializeSQLiteStorage(ctx context.Context) error {
	return fmt.Errorf("SQLite storage requested but not available.\n\n" +
		"This binary was built without SQLite support. To use SQLite:\n\n" +
		"  Option 1 - Rebuild with SQLite support:\n" +
		"    CGO_ENABLED=1 go build -tags sqlite -o chronoqueue .\n\n" +
		"  Option 2 - Use the Makefile:\n" +
		"    make build-full\n\n" +
		"  Option 3 - Use PostgreSQL instead (recommended):\n" +
		"    chronoqueue server --storage-type postgres\n\n" +
		"Note: SQLite requires CGO and is not available by default to keep the\n" +
		"      binary portable and easy to cross-compile. For production deployments,\n" +
		"      PostgreSQL is recommended as it's pure Go with no CGO dependencies")
}
