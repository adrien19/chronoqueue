package sql

import (
	"fmt"
	"strings"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

// QueryBuilder helps construct SQL queries with dialect-specific syntax.
// This allows the same logical query to work across different SQL databases.
type QueryBuilder struct {
	dialect SQLDialect
}

// NewQueryBuilder creates a new QueryBuilder for the given dialect
func NewQueryBuilder(dialect SQLDialect) *QueryBuilder {
	return &QueryBuilder{dialect: dialect}
}

// BuildFindExpiredMessagesQuery builds a query to find messages with expired leases
func (qb *QueryBuilder) BuildFindExpiredMessagesQuery() string {
	return fmt.Sprintf(`
		SELECT id, message_id, queue_name, metadata_pb, state, attempts_left
		FROM cq_messages
		WHERE queue_name = %s
		  AND state = 2
		  AND (lease_expiry < %s OR (heartbeat_expiry IS NOT NULL AND heartbeat_expiry < %s))
		ORDER BY priority DESC, id ASC
		LIMIT %s
	`, qb.dialect.Placeholder(1), qb.dialect.Placeholder(2),
		qb.dialect.Placeholder(3), qb.dialect.Placeholder(4))
}

// BuildClaimMessageQuery builds a query to claim a PENDING message.
// Returns different queries based on dialect capabilities.
func (qb *QueryBuilder) BuildClaimMessageQuery() string {
	nowPlaceholder := qb.dialect.Placeholder(2)

	if qb.dialect.SupportsSkipLocked() {
		// Postgres: Use SELECT FOR UPDATE SKIP LOCKED for non-blocking claims
		return fmt.Sprintf(`
			SELECT id, message_id, queue_name, metadata_pb, priority, attempts_left, max_attempts
			FROM cq_messages
			WHERE queue_name = %s
			  AND state = 1
			  AND (scheduled_at IS NULL OR scheduled_at <= %s)
			ORDER BY priority DESC, id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		`, qb.dialect.Placeholder(1), nowPlaceholder)
	}

	// SQLite: Simple SELECT (relies on transaction isolation)
	return fmt.Sprintf(`
		SELECT id, message_id, queue_name, metadata_pb, priority, attempts_left, max_attempts
		FROM cq_messages
		WHERE queue_name = %s
		  AND state = 1
		  AND (scheduled_at IS NULL OR scheduled_at <= %s)
		ORDER BY priority DESC, id ASC
		LIMIT 1
	`, qb.dialect.Placeholder(1), nowPlaceholder)
}

// BuildOldestMessageAgeQuery returns the minimum created_at for pending messages in a priority range.
// Used for aging-based priority weighting.
func (qb *QueryBuilder) BuildOldestMessageAgeQuery(_ string) string {
	return fmt.Sprintf(
		`
		SELECT MIN(created_at)
		FROM cq_messages
		WHERE queue_name = %s
		  AND state = %d
		  AND (scheduled_at IS NULL OR scheduled_at <= %s)
		  AND priority BETWEEN %s AND %s
	`,
		qb.dialect.Placeholder(1),
		messagepb.Message_Metadata_PENDING,
		qb.dialect.Placeholder(2),
		qb.dialect.Placeholder(3),
		qb.dialect.Placeholder(4),
	)
}

// BuildUpdateMessageStateQuery builds a query to update message state
func (qb *QueryBuilder) BuildUpdateMessageStateQuery() string {
	return fmt.Sprintf(`
		UPDATE cq_messages
		SET state = %s,
		    updated_at = %s
		WHERE message_id = %s
	`, qb.dialect.Placeholder(1), qb.dialect.CurrentTimestamp(), qb.dialect.Placeholder(2))
}

// BuildGetScheduledMessagesQuery builds a query to find messages ready for activation
func (qb *QueryBuilder) BuildGetScheduledMessagesQuery() string {
	nowPlaceholder := qb.dialect.Placeholder(1)
	return fmt.Sprintf(`
		SELECT id, message_id, queue_name, metadata_pb
		FROM cq_messages
		WHERE state = 3
		  AND scheduled_at IS NOT NULL
		  AND scheduled_at <= %s
		ORDER BY scheduled_at ASC, priority DESC
		LIMIT %s
	`, nowPlaceholder, qb.dialect.Placeholder(2))
}

// BuildUpdateStateCountersQuery builds a query to update queue state counters.
// Uses JSON functions to increment/decrement state counts.
func (qb *QueryBuilder) BuildUpdateStateCountersQuery(increment bool, stateKey string) string {
	operation := "+"
	if !increment {
		operation = "-"
	}

	jsonSet := qb.dialect.JSONSet()
	jsonSetPath := qb.dialect.JSONSetPath(stateKey)
	jsonExtract := qb.dialect.JSONExtract()
	jsonExtractPath := qb.dialect.JSONExtractPath(stateKey)

	return fmt.Sprintf(`
		UPDATE cq_queues
		SET state_counts = %s(
			COALESCE(state_counts, '{}'),
			%s,
			%s
		)
		WHERE name = %s
	`, jsonSet, jsonSetPath,
		qb.dialect.ToJSON(fmt.Sprintf("COALESCE(CAST(%s(state_counts, %s) AS INTEGER), 0) %s 1", jsonExtract, jsonExtractPath, operation)),
		qb.dialect.Placeholder(1))
}

// BuildCountByStateQuery builds a query to count messages by state
func (qb *QueryBuilder) BuildCountByStateQuery() string {
	return fmt.Sprintf(`
		SELECT state, COUNT(*) as count
		FROM cq_messages
		WHERE queue_name = %s
		GROUP BY state
	`, qb.dialect.Placeholder(1))
}

// BuildFindEarliestDeadlineQuery builds a query to find the earliest lease expiry
func (qb *QueryBuilder) BuildFindEarliestDeadlineQuery() string {
	return fmt.Sprintf(`
		SELECT MIN(lease_expiry) as earliest
		FROM cq_messages
		WHERE queue_name = %s
		  AND state = 2
		  AND lease_expiry IS NOT NULL
	`, qb.dialect.Placeholder(1))
}

// BuildInsertQueueQuery builds a query to insert a new queue
func (qb *QueryBuilder) BuildInsertQueueQuery() string {
	if qb.dialect.SupportsReturning() {
		return fmt.Sprintf(`
			INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at)
			VALUES (%s, %s, %s, %s)
			RETURNING name
		`, qb.dialect.Placeholder(1), qb.dialect.Placeholder(2),
			qb.dialect.CurrentTimestamp(), qb.dialect.CurrentTimestamp())
	}
	return fmt.Sprintf(`
		INSERT INTO cq_queues (name, metadata_pb, created_at, updated_at)
		VALUES (%s, %s, %s, %s)
	`, qb.dialect.Placeholder(1), qb.dialect.Placeholder(2),
		qb.dialect.CurrentTimestamp(), qb.dialect.CurrentTimestamp())
}

// BuildInsertMessageQuery builds a query to insert a new message with idempotency support
func (qb *QueryBuilder) BuildInsertMessageQuery() string {
	// 12 columns total
	placeholders := make([]string, 12)
	for i := range 12 {
		placeholders[i] = qb.dialect.Placeholder(i + 1)
	}

	baseQuery := fmt.Sprintf(`
		INSERT INTO cq_messages (
			message_id, queue_name, state, priority, scheduled_at,
			lease_expiry, heartbeat_expiry, attempts_left, max_attempts,
			metadata_pb, created_at, updated_at
		) VALUES (%s)`, strings.Join(placeholders, ", "))

	// Add conflict clause - both SQLite and PostgreSQL support ON CONFLICT
	return baseQuery + " ON CONFLICT(message_id) DO NOTHING"
}

// BuildDeleteMessageQuery builds a query to delete a message
func (qb *QueryBuilder) BuildDeleteMessageQuery() string {
	return fmt.Sprintf(`
		DELETE FROM cq_messages
		WHERE message_id = %s
	`, qb.dialect.Placeholder(1))
}

// BuildGetQueueMetadataQuery builds a query to retrieve queue metadata
func (qb *QueryBuilder) BuildGetQueueMetadataQuery() string {
	return fmt.Sprintf(`
		SELECT name, metadata_pb, created_at, updated_at
		FROM cq_queues
		WHERE name = %s
	`, qb.dialect.Placeholder(1))
}
