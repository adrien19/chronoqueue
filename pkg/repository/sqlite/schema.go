package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/schema"
)

const latestVersion = uint(10)

type SchemaManager struct {
	baseManager *schema.BaseManager
}

func NewSchemaManager() *SchemaManager {
	return &SchemaManager{
		baseManager: &schema.BaseManager{},
	}
}

func (m *SchemaManager) EnsureVersionTable(ctx context.Context, db *sql.DB) error {
	return m.baseManager.EnsureVersionTable(ctx, db)
}

func (m *SchemaManager) GetVersion(ctx context.Context, db *sql.DB) (uint, bool, error) {
	return m.baseManager.GetVersion(ctx, db)
}

func (m *SchemaManager) SetVersion(ctx context.Context, db *sql.DB, version uint, description string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, version, description)
	return err
}

func (m *SchemaManager) Initialize(ctx context.Context, db *sql.DB) error {
	version, exists, err := m.GetVersion(ctx, db)
	if err != nil {
		return fmt.Errorf("check schema version: %w", err)
	}
	if exists && version > latestVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, latestVersion)
	}
	if exists && version > 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := m.createQueuesTable(ctx, tx); err != nil {
		return fmt.Errorf("create queues table: %w", err)
	}
	if err := m.createMessagesTable(ctx, tx); err != nil {
		return fmt.Errorf("create messages table: %w", err)
	}
	if err := m.createDLQTable(ctx, tx); err != nil {
		return fmt.Errorf("create DLQ table: %w", err)
	}
	if err := m.createSchedulesTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedules table: %w", err)
	}
	if err := m.createScheduleHistoryTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedule history table: %w", err)
	}
	if err := m.createScheduleArchiveTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedule archive table: %w", err)
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, latestVersion, "Initial schema")
	if err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}

	return tx.Commit()
}

func (m *SchemaManager) Migrate(ctx context.Context, db *sql.DB, targetVersion uint) error {
	if targetVersion > latestVersion {
		return fmt.Errorf("target schema version %d is newer than supported version %d", targetVersion, latestVersion)
	}
	current, exists, err := m.GetVersion(ctx, db)
	if err != nil {
		return fmt.Errorf("get schema version: %w", err)
	}
	if exists && current > latestVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", current, latestVersion)
	}

	if !exists {
		return m.Initialize(ctx, db)
	}

	if current >= targetVersion {
		return nil
	}

	for v := current + 1; v <= targetVersion; v++ {
		switch v {
		case 2:
			if err := m.migrateToV2(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 2: %w", err)
			}
		case 3:
			if err := m.migrateToV3(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 3: %w", err)
			}
		case 4:
			if err := m.migrateToV4_AddRetentionFields(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 4: %w", err)
			}
		case 5:
			if err := m.migrateToV5_AddCancellationReason(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 5: %w", err)
			}
		case 6:
			if err := m.migrateToV6_QueueScopedMessageIDs(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 6: %w", err)
			}
		case 7:
			if err := m.migrateToV7_FixSchedulerIndex(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 7: %w", err)
			}
		case 8:
			if err := m.migrateToV8_DurableScheduleHistory(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 8: %w", err)
			}
		case 9:
			if err := m.migrateToV9_AddDLQRelationshipIndex(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 9: %w", err)
			}
		case 10:
			if err := m.migrateToV10_NormalizeMessageRuntime(ctx, db); err != nil {
				return fmt.Errorf("migrate to version 10: %w", err)
			}
		default:
			return fmt.Errorf("unsupported target version %d", v)
		}
	}

	return nil
}

func (m *SchemaManager) migrateToV10_NormalizeMessageRuntime(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var messagesExist bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cq_messages')`).Scan(&messagesExist); err != nil {
		return fmt.Errorf("inspect messages table: %w", err)
	}
	if messagesExist {
		dialect := NewDialect()
		for _, column := range []string{"created_at", "updated_at"} {
			query := fmt.Sprintf("UPDATE cq_messages SET %s = %s WHERE typeof(%s) = 'text'", column, dialect.UnixMillis(column), column)
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("normalize message %s: %w", column, err)
			}
		}
	}
	var stateCountsColumn bool
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(cq_queues)`)
	if err != nil {
		return fmt.Errorf("inspect queues table: %w", err)
	}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan queues table: %w", err)
		}
		stateCountsColumn = stateCountsColumn || name == "state_counts"
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate queues table info: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close queues table info: %w", err)
	}
	if messagesExist && stateCountsColumn {
		if _, err := tx.ExecContext(ctx, `
		UPDATE cq_queues SET state_counts = json_object(
			'invisible', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 0 AND deleted_at IS NULL),
			'pending', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 1 AND deleted_at IS NULL),
			'running', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 2 AND deleted_at IS NULL),
			'completed', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 3 AND deleted_at IS NULL),
			'errored', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 4 AND deleted_at IS NULL),
			'canceled', (SELECT COUNT(*) FROM cq_messages WHERE queue_name = cq_queues.name AND state = 5 AND deleted_at IS NULL))`); err != nil {
			return fmt.Errorf("rebuild queue state counters: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 10, "Normalize message timestamps and rebuild state counters"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func (m *SchemaManager) migrateToV9_AddDLQRelationshipIndex(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `ALTER TABLE cq_queues ADD COLUMN dead_letter_queue_name TEXT`); err != nil {
		return fmt.Errorf("add dead letter queue column: %w", err)
	}
	type relationship struct{ source, target string }
	var relationships []relationship
	rows, err := tx.QueryContext(ctx, `SELECT name, metadata_pb FROM cq_queues`)
	if err != nil {
		return fmt.Errorf("query queues for DLQ backfill: %w", err)
	}
	closeRowsWith := func(baseErr error) error {
		if closeErr := rows.Close(); closeErr != nil {
			return errors.Join(baseErr, fmt.Errorf("close queues for DLQ backfill: %w", closeErr))
		}
		return baseErr
	}
	for rows.Next() {
		var name string
		var data []byte
		if err := rows.Scan(&name, &data); err != nil {
			return closeRowsWith(fmt.Errorf("scan queue for DLQ backfill: %w", err))
		}
		queue := &queuepb.Queue{}
		if err := proto.Unmarshal(data, queue); err != nil {
			return closeRowsWith(fmt.Errorf("unmarshal queue %q for DLQ backfill: %w", name, err))
		}
		if target := queue.GetMetadata().GetDeadLetterQueueName(); target != "" {
			relationships = append(relationships, relationship{source: name, target: target})
		}
	}
	if err := rows.Err(); err != nil {
		return closeRowsWith(fmt.Errorf("iterate queues for DLQ backfill: %w", err))
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close queues for DLQ backfill: %w", err)
	}
	for _, relationship := range relationships {
		if _, err := tx.ExecContext(ctx, `UPDATE cq_queues SET dead_letter_queue_name = ? WHERE name = ?`, relationship.target, relationship.source); err != nil {
			return fmt.Errorf("backfill dead letter queue name: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX idx_queues_dead_letter_queue_name ON cq_queues(dead_letter_queue_name) WHERE dead_letter_queue_name IS NOT NULL`); err != nil {
		return fmt.Errorf("create dead letter queue index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 9, "Index queue dead letter relationships"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func nullableDLQName(name string) any {
	if name == "" {
		return nil
	}
	return name
}

func (m *SchemaManager) migrateToV2(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	statements := []string{
		`ALTER TABLE cq_schedules ADD COLUMN next_run INTEGER`,
		`ALTER TABLE cq_schedules ADD COLUMN last_run INTEGER`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_state_next_run ON cq_schedules(state, next_run)`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 2, "Add schedule next_run/last_run"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	return tx.Commit()
}

func (m *SchemaManager) migrateToV3(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	statements := []string{
		`ALTER TABLE cq_schedules ADD COLUMN cron_schedule TEXT`,
		`ALTER TABLE cq_schedules ADD COLUMN execution_count INTEGER DEFAULT 0`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 3, "Add cron schedule columns"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	return tx.Commit()
}

func (m *SchemaManager) migrateToV4_AddRetentionFields(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	statements := []string{
		`ALTER TABLE cq_messages ADD COLUMN completed_at INTEGER`,
		`ALTER TABLE cq_messages ADD COLUMN deleted_at INTEGER`,
		`CREATE INDEX IF NOT EXISTS idx_messages_cleanup ON cq_messages(deleted_at) WHERE deleted_at IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_messages_queue_state_active ON cq_messages(queue_name, state) WHERE deleted_at IS NULL`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 4, "Add message retention fields"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	return tx.Commit()
}

func (m *SchemaManager) migrateToV5_AddCancellationReason(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	statements := []string{
		`ALTER TABLE cq_messages ADD COLUMN cancellation_reason TEXT`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 5, "Add cancellation_reason field"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	return tx.Commit()
}

func (m *SchemaManager) migrateToV6_QueueScopedMessageIDs(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `ALTER TABLE cq_messages RENAME TO cq_messages_v5`); err != nil {
		return fmt.Errorf("rename messages table: %w", err)
	}
	for _, indexName := range []string{
		"idx_messages_scheduler",
		"idx_messages_reclaim",
		"idx_messages_heartbeat",
		"idx_messages_message_id",
		"idx_messages_queue_state",
		"idx_messages_cleanup",
		"idx_messages_queue_state_active",
	} {
		if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS `+indexName); err != nil {
			return fmt.Errorf("drop old messages index %s: %w", indexName, err)
		}
	}
	if err := m.createMessagesTable(ctx, tx); err != nil {
		return fmt.Errorf("create queue-scoped messages table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO cq_messages (
			id, queue_name, message_id, metadata_pb, state, priority, scheduled_at,
			lease_expiry, heartbeat_expiry, attempts_left, max_attempts,
			current_attempt_id, current_worker_id, lease_started_at,
			lease_extension_used, lease_renewal_count, last_heartbeat_at,
			created_at, updated_at, completed_at, deleted_at, cancellation_reason
		)
		SELECT id, queue_name, message_id, metadata_pb, state, priority, scheduled_at,
			lease_expiry, heartbeat_expiry, attempts_left, max_attempts,
			current_attempt_id, current_worker_id, lease_started_at,
			lease_extension_used, lease_renewal_count, last_heartbeat_at,
			created_at, updated_at, completed_at, deleted_at, cancellation_reason
		FROM cq_messages_v5
	`); err != nil {
		return fmt.Errorf("copy messages: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE cq_messages_v5`); err != nil {
		return fmt.Errorf("drop old messages table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 6, "Scope message IDs to queues"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func (m *SchemaManager) migrateToV7_FixSchedulerIndex(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS idx_messages_scheduler`); err != nil {
		return fmt.Errorf("drop scheduler index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX idx_messages_scheduler ON cq_messages(state, scheduled_at, priority DESC) WHERE state = 0`); err != nil {
		return fmt.Errorf("create scheduler index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 7, "Index invisible scheduled messages"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func (m *SchemaManager) migrateToV8_DurableScheduleHistory(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := m.createScheduleHistoryTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedule history table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE cq_schedule_history RENAME TO cq_schedule_history_v7`); err != nil {
		return fmt.Errorf("rename schedule history table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS idx_schedule_history_schedule`); err != nil {
		return fmt.Errorf("drop old schedule history index: %w", err)
	}
	if err := m.createScheduleHistoryTable(ctx, tx); err != nil {
		return fmt.Errorf("create durable schedule history table: %w", err)
	}
	copyStatement := `
		INSERT INTO cq_schedule_history (id, schedule_id, message_id, executed_at, success, error_message, message_pb)
		SELECT h.id, h.schedule_id, h.message_id, h.executed_at, h.success, h.error_message, m.metadata_pb
		FROM cq_schedule_history_v7 h
		LEFT JOIN cq_messages m ON m.id = (SELECT candidate.id FROM cq_messages candidate WHERE candidate.message_id = h.message_id ORDER BY candidate.id LIMIT 1)
	`
	hasQueueName, err := sqliteTableHasColumn(ctx, tx, "cq_schedules", "queue_name")
	if err != nil {
		return fmt.Errorf("inspect schedules table: %w", err)
	}
	if hasQueueName {
		copyStatement = `
			INSERT INTO cq_schedule_history (id, schedule_id, message_id, executed_at, success, error_message, message_pb)
			SELECT h.id, h.schedule_id, h.message_id, h.executed_at, h.success, h.error_message, m.metadata_pb
			FROM cq_schedule_history_v7 h
			JOIN cq_schedules s ON s.id = h.schedule_id
			LEFT JOIN cq_messages m ON m.message_id = h.message_id AND m.queue_name = s.queue_name
		`
	}
	if _, err := tx.ExecContext(ctx, copyStatement); err != nil {
		return fmt.Errorf("copy schedule history: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE cq_schedule_history_v7`); err != nil {
		return fmt.Errorf("drop old schedule history table: %w", err)
	}
	if err := m.createScheduleArchiveTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedule archive table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES (?, ?)`, 8, "Preserve schedule history and message snapshots"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func sqliteTableHasColumn(ctx context.Context, tx *sql.Tx, tableName, columnName string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+tableName+`)`)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == columnName {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (m *SchemaManager) Version(ctx context.Context, db *sql.DB) (uint, bool, error) {
	return m.GetVersion(ctx, db)
}

func (m *SchemaManager) createQueuesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_queues (name TEXT PRIMARY KEY, metadata_pb BLOB NOT NULL, dead_letter_queue_name TEXT, state_counts TEXT DEFAULT '{}', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_queues_dead_letter_queue_name ON cq_queues(dead_letter_queue_name) WHERE dead_letter_queue_name IS NOT NULL`)
	return err
}

func (m *SchemaManager) createMessagesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		queue_name TEXT NOT NULL,
		message_id TEXT NOT NULL,
		metadata_pb BLOB NOT NULL,
		state INTEGER NOT NULL,
		priority INTEGER NOT NULL DEFAULT 5,
		scheduled_at INTEGER,
		lease_expiry INTEGER,
		heartbeat_expiry INTEGER,
		attempts_left INTEGER,
		max_attempts INTEGER,
		current_attempt_id TEXT,
		current_worker_id TEXT,
		lease_started_at INTEGER,
		lease_extension_used INTEGER DEFAULT 0,
		lease_renewal_count INTEGER DEFAULT 0,
		last_heartbeat_at INTEGER,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		completed_at INTEGER,
		deleted_at INTEGER,
		cancellation_reason TEXT,
		FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE,
		UNIQUE (queue_name, message_id)
	)`)
	if err != nil {
		return err
	}
	indices := []string{
		`CREATE INDEX IF NOT EXISTS idx_messages_scheduler ON cq_messages(state, scheduled_at, priority DESC) WHERE state = 0`,
		`CREATE INDEX IF NOT EXISTS idx_messages_reclaim ON cq_messages(queue_name, state, lease_expiry) WHERE state = 2`,
		`CREATE INDEX IF NOT EXISTS idx_messages_heartbeat ON cq_messages(queue_name, state, heartbeat_expiry) WHERE state = 2 AND heartbeat_expiry IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_messages_queue_state ON cq_messages(queue_name, state)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_cleanup ON cq_messages(deleted_at) WHERE deleted_at IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_messages_queue_state_active ON cq_messages(queue_name, state) WHERE deleted_at IS NULL`,
	}
	for _, idx := range indices {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return err
		}
	}
	return nil
}

func (m *SchemaManager) createDLQTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_dlq (id INTEGER PRIMARY KEY AUTOINCREMENT, queue_name TEXT NOT NULL, message_id TEXT NOT NULL, reason TEXT NOT NULL, metadata_pb BLOB NOT NULL, created_at INTEGER NOT NULL, FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE)`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_dlq_queue_created ON cq_dlq(queue_name, created_at DESC)`)
	return err
}

func (m *SchemaManager) createSchedulesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_schedules (id TEXT PRIMARY KEY, queue_name TEXT NOT NULL, metadata_pb BLOB NOT NULL, state INTEGER NOT NULL, cron_schedule TEXT, next_run INTEGER, last_run INTEGER, execution_count INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE)`)
	if err != nil {
		return err
	}
	indices := []string{
		`CREATE INDEX IF NOT EXISTS idx_schedules_queue ON cq_schedules(queue_name, id)`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_state_next_run ON cq_schedules(state, next_run)`,
	}
	for _, idx := range indices {
		if _, err := tx.ExecContext(ctx, idx); err != nil {
			return err
		}
	}
	return nil
}

func (m *SchemaManager) createScheduleHistoryTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_schedule_history (id INTEGER PRIMARY KEY AUTOINCREMENT, schedule_id TEXT NOT NULL, message_id TEXT NOT NULL, executed_at INTEGER NOT NULL, success INTEGER NOT NULL, error_message TEXT, message_pb BLOB)`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_schedule_history_schedule ON cq_schedule_history(schedule_id, executed_at DESC)`)
	return err
}

func (m *SchemaManager) createScheduleArchiveTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cq_schedule_archive (schedule_id TEXT PRIMARY KEY, metadata_pb BLOB NOT NULL, deleted_at INTEGER NOT NULL)`)
	return err
}
