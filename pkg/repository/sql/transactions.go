package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// TxOptions holds transaction configuration
type TxOptions struct {
	Isolation sql.IsolationLevel
	Timeout   time.Duration
	ReadOnly  bool
}

// DefaultTxOptions returns default transaction options
func DefaultTxOptions() *TxOptions {
	return &TxOptions{
		Isolation: sql.LevelDefault,
		Timeout:   5 * time.Second,
		ReadOnly:  false,
	}
}

// WithTransaction executes fn within a transaction with proper error handling and rollback.
// This provides a consistent transaction pattern across all SQL backends.
func (b *BaseSQL) WithTransaction(
	ctx context.Context,
	opts *TxOptions,
	fn func(tx *sql.Tx) error,
) error {
	if opts == nil {
		opts = DefaultTxOptions()
	}

	// Create context with timeout
	txCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	// Begin transaction
	tx, err := b.DB.BeginTx(txCtx, &sql.TxOptions{
		Isolation: opts.Isolation,
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Handle panics
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // Re-panic after rollback
		}
	}()

	// Execute function
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			b.Logger.ErrorWithFields("Failed to rollback transaction", "error", rbErr)
		}
		return err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// WithReadOnlyTransaction executes fn within a read-only transaction
func (b *BaseSQL) WithReadOnlyTransaction(
	ctx context.Context,
	fn func(tx *sql.Tx) error,
) error {
	opts := &TxOptions{
		Isolation: sql.LevelReadCommitted,
		Timeout:   5 * time.Second,
		ReadOnly:  true,
	}
	return b.WithTransaction(ctx, opts, fn)
}

// WithSerializableTransaction executes fn within a serializable transaction.
// Used for operations requiring strict isolation (e.g., message claiming in SQLite).
func (b *BaseSQL) WithSerializableTransaction(
	ctx context.Context,
	fn func(tx *sql.Tx) error,
) error {
	opts := &TxOptions{
		Isolation: sql.LevelSerializable,
		Timeout:   10 * time.Second,
		ReadOnly:  false,
	}
	return b.WithTransaction(ctx, opts, fn)
}
