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

// InsertCounter records a newly inserted message without decrementing a prior state.
func (sm *StateManager) InsertCounter(ctx context.Context, tx *sql.Tx, queueName string, state messagepb.Message_Metadata_State) error {
	if err := sm.incrementCounter(ctx, tx, queueName, state); err != nil {
		return fmt.Errorf("increment %s counter: %w", state.String(), err)
	}
	return nil
}

// MoveCounter transfers one message between queues without inventing an intermediate state.
func (sm *StateManager) MoveCounter(ctx context.Context, tx *sql.Tx, sourceQueue string, sourceState messagepb.Message_Metadata_State, targetQueue string, targetState messagepb.Message_Metadata_State) error {
	if err := sm.decrementCounter(ctx, tx, sourceQueue, sourceState); err != nil {
		return fmt.Errorf("decrement %s counter: %w", sourceState.String(), err)
	}
	if err := sm.incrementCounter(ctx, tx, targetQueue, targetState); err != nil {
		return fmt.Errorf("increment %s counter: %w", targetState.String(), err)
	}
	return nil
}

func (sm *StateManager) RemoveCounter(ctx context.Context, tx *sql.Tx, queueName string, state messagepb.Message_Metadata_State) error {
	if err := sm.decrementCounter(ctx, tx, queueName, state); err != nil {
		return fmt.Errorf("decrement %s counter: %w", state.String(), err)
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
	case messagepb.Message_Metadata_CANCELED:
		return "canceled"
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
			COALESCE(CAST(%s(state_counts, '$.completed') AS INTEGER), 0) as completed,
			COALESCE(CAST(%s(state_counts, '$.canceled') AS INTEGER), 0) as canceled
		FROM cq_queues
		WHERE name = %s
	`, jsonExtract, jsonExtract, jsonExtract, jsonExtract, jsonExtract, jsonExtract, sm.dialect.Placeholder(1))

	counts := make(map[string]int64)
	var pending, running, invisible, errored, completed, canceled int64

	err := db.QueryRowContext(ctx, query, queueName).Scan(
		&pending, &running, &invisible, &errored, &completed, &canceled,
	)
	if err != nil {
		return nil, err
	}

	counts["pending"] = pending
	counts["running"] = running
	counts["invisible"] = invisible
	counts["errored"] = errored
	counts["completed"] = completed
	counts["canceled"] = canceled

	return counts, nil
}
