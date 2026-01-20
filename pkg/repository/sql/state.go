package sql

import (
	"context"
	"database/sql"
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

// StateManager handles message state counter updates for queue statistics.
// This provides O(1) queue state lookups by maintaining pre-computed counters.
type StateManager struct {
	dialect SQLDialect
}

// NewStateManager creates a new StateManager instance
func NewStateManager(dialect SQLDialect) *StateManager {
	return &StateManager{dialect: dialect}
}

// UpdateCounters atomically updates state counters when a message transitions.
// This is called within a transaction to ensure consistency.
func (sm *StateManager) UpdateCounters(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	oldState, newState messagepb.Message_Metadata_State,
) error {
	if oldState == newState {
		return nil // No state change, no update needed
	}

	// Decrement old state counter
	if err := sm.decrementCounter(ctx, tx, queueName, oldState); err != nil {
		return fmt.Errorf("decrement %s counter: %w", oldState.String(), err)
	}

	// Increment new state counter
	if err := sm.incrementCounter(ctx, tx, queueName, newState); err != nil {
		return fmt.Errorf("increment %s counter: %w", newState.String(), err)
	}

	return nil
}

// decrementCounter decrements the counter for a specific state
func (sm *StateManager) decrementCounter(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	state messagepb.Message_Metadata_State,
) error {
	stateKey := sm.stateToKey(state)
	jsonSet := sm.dialect.JSONSet()
	jsonSetPath := sm.dialect.JSONSetPath(stateKey)
	jsonExtract := sm.dialect.JSONExtract()
	jsonExtractPath := sm.dialect.JSONExtractPath(stateKey)

	query := fmt.Sprintf(`
		UPDATE cq_queues 
		SET state_counts = %s(
			COALESCE(state_counts, '{}'),
			%s,
			%s
		) 
		WHERE name = %s
	`, jsonSet, jsonSetPath,
		sm.dialect.ToJSON(fmt.Sprintf("COALESCE(CAST(%s(state_counts, %s) AS INTEGER), 0) - 1", jsonExtract, jsonExtractPath)),
		sm.dialect.Placeholder(1))

	_, err := tx.ExecContext(ctx, query, queueName)
	return err
}

// incrementCounter increments the counter for a specific state
func (sm *StateManager) incrementCounter(
	ctx context.Context,
	tx *sql.Tx,
	queueName string,
	state messagepb.Message_Metadata_State,
) error {
	stateKey := sm.stateToKey(state)
	jsonSet := sm.dialect.JSONSet()
	jsonSetPath := sm.dialect.JSONSetPath(stateKey)
	jsonExtract := sm.dialect.JSONExtract()
	jsonExtractPath := sm.dialect.JSONExtractPath(stateKey)

	query := fmt.Sprintf(`
		UPDATE cq_queues 
		SET state_counts = %s(
			COALESCE(state_counts, '{}'),
			%s,
			%s
		) 
		WHERE name = %s
	`, jsonSet, jsonSetPath,
		sm.dialect.ToJSON(fmt.Sprintf("COALESCE(CAST(%s(state_counts, %s) AS INTEGER), 0) + 1", jsonExtract, jsonExtractPath)),
		sm.dialect.Placeholder(1))

	_, err := tx.ExecContext(ctx, query, queueName)
	return err
}

// stateToKey converts a message state enum to a JSON key
func (sm *StateManager) stateToKey(state messagepb.Message_Metadata_State) string {
	switch state {
	case messagepb.Message_Metadata_PENDING:
		return "pending"
	case messagepb.Message_Metadata_RUNNING:
		return "running"
	case messagepb.Message_Metadata_INVISIBLE:
		return "invisible"
	case messagepb.Message_Metadata_ERRORED:
		return "errored"
	case messagepb.Message_Metadata_COMPLETED:
		return "completed"
	default:
		return "unknown"
	}
}

// GetStateCounts retrieves the current state counts for a queue
func (sm *StateManager) GetStateCounts(
	ctx context.Context,
	db *sql.DB,
	queueName string,
) (map[string]int64, error) {
	jsonExtract := sm.dialect.JSONExtract()

	query := fmt.Sprintf(`
		SELECT 
			COALESCE(CAST(%s(state_counts, '$.pending') AS INTEGER), 0) as pending,
			COALESCE(CAST(%s(state_counts, '$.running') AS INTEGER), 0) as running,
			COALESCE(CAST(%s(state_counts, '$.invisible') AS INTEGER), 0) as invisible,
			COALESCE(CAST(%s(state_counts, '$.errored') AS INTEGER), 0) as errored,
			COALESCE(CAST(%s(state_counts, '$.completed') AS INTEGER), 0) as completed
		FROM cq_queues
		WHERE name = %s
	`, jsonExtract, jsonExtract, jsonExtract, jsonExtract, jsonExtract, sm.dialect.Placeholder(1))

	counts := make(map[string]int64)
	var pending, running, invisible, errored, completed int64

	err := db.QueryRowContext(ctx, query, queueName).Scan(
		&pending, &running, &invisible, &errored, &completed,
	)
	if err != nil {
		return nil, err
	}

	counts["pending"] = pending
	counts["running"] = running
	counts["invisible"] = invisible
	counts["errored"] = errored
	counts["completed"] = completed

	return counts, nil
}
