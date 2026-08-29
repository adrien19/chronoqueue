package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/schema"
)

const latestVersion = uint(9)

// SchemaManager handles PostgreSQL schema initialization and versioning.
type SchemaManager struct {
	baseManager *schema.BaseManager
}

// NewSchemaManager creates a new PostgreSQL schema manager.
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{baseManager: &schema.BaseManager{}}
}

func (m *SchemaManager) EnsureVersionTable(ctx context.Context, db *sql.DB) error {
	return m.baseManager.EnsureVersionTable(ctx, db)
}

func (m *SchemaManager) GetVersion(ctx context.Context, db *sql.DB) (uint, bool, error) {
	return m.baseManager.GetVersion(ctx, db)
}

func (m *SchemaManager) SetVersion(ctx context.Context, db *sql.DB, version uint, description string) error {
	_, err := db.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, version, description)
	return err
}

// Initialize creates the initial schema if no version is present.
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

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, latestVersion, "Initial schema"); err != nil {
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
		default:
			return fmt.Errorf("unsupported target version %d", v)
		}
	}

	return nil
}

func (m *SchemaManager) migrateToV9_AddDLQRelationshipIndex(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `ALTER TABLE cq_queues ADD COLUMN IF NOT EXISTS dead_letter_queue_name TEXT`); err != nil {
		return fmt.Errorf("add dead letter queue column: %w", err)
	}
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
	type relationship struct{ source, target string }
	var relationships []relationship
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
		if _, err := tx.ExecContext(ctx, `UPDATE cq_queues SET dead_letter_queue_name = $1 WHERE name = $2`, relationship.target, relationship.source); err != nil {
			return fmt.Errorf("backfill dead letter queue name: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_queues_dead_letter_queue_name ON cq_queues(dead_letter_queue_name) WHERE dead_letter_queue_name IS NOT NULL`); err != nil {
		return fmt.Errorf("create dead letter queue index: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 9, "Index queue dead letter relationships"); err != nil {
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
		`ALTER TABLE cq_schedules ADD COLUMN IF NOT EXISTS next_run BIGINT`,
		`ALTER TABLE cq_schedules ADD COLUMN IF NOT EXISTS last_run BIGINT`,
		`CREATE INDEX IF NOT EXISTS idx_schedules_state_next_run ON cq_schedules(state, next_run)`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 2, "Add schedule next_run/last_run"); err != nil {
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
		`ALTER TABLE cq_schedules ADD COLUMN IF NOT EXISTS cron_schedule TEXT`,
		`ALTER TABLE cq_schedules ADD COLUMN IF NOT EXISTS execution_count BIGINT DEFAULT 0`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 3, "Add cron schedule columns"); err != nil {
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
		`ALTER TABLE cq_messages ADD COLUMN IF NOT EXISTS completed_at BIGINT`,
		`ALTER TABLE cq_messages ADD COLUMN IF NOT EXISTS deleted_at BIGINT`,
		`CREATE INDEX IF NOT EXISTS idx_messages_cleanup ON cq_messages(deleted_at) WHERE deleted_at IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_messages_queue_state_active ON cq_messages(queue_name, state) WHERE deleted_at IS NULL`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 4, "Add message retention fields"); err != nil {
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
		`ALTER TABLE cq_messages ADD COLUMN IF NOT EXISTS cancellation_reason TEXT`,
	}

	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 5, "Add cancellation_reason field"); err != nil {
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

	statements := []string{
		`ALTER TABLE cq_messages DROP CONSTRAINT IF EXISTS cq_messages_message_id_key`,
		`DROP INDEX IF EXISTS idx_messages_message_id`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_queue_message_id ON cq_messages(queue_name, message_id)`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 6, "Scope message IDs to queues"); err != nil {
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 7, "Index invisible scheduled messages"); err != nil {
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
	statements := []string{
		`ALTER TABLE cq_schedule_history ADD COLUMN IF NOT EXISTS message_pb BYTEA`,
		`DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'cq_schedules' AND column_name = 'queue_name') THEN
				UPDATE cq_schedule_history h
				SET message_pb = m.metadata_pb
				FROM cq_messages m, cq_schedules s
				WHERE h.schedule_id = s.id AND m.queue_name = s.queue_name AND h.message_id = m.message_id AND h.message_pb IS NULL;
			ELSE
				UPDATE cq_schedule_history h
				SET message_pb = (SELECT m.metadata_pb FROM cq_messages m WHERE m.message_id = h.message_id ORDER BY m.id LIMIT 1)
				WHERE h.message_pb IS NULL;
			END IF;
		END $$`,
		`ALTER TABLE cq_schedule_history DROP CONSTRAINT IF EXISTS cq_schedule_history_schedule_id_fkey`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute migration statement: %w", err)
		}
	}
	if err := m.createScheduleArchiveTable(ctx, tx); err != nil {
		return fmt.Errorf("create schedule archive table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cq_schema_version (version, description) VALUES ($1, $2)`, 8, "Preserve schedule history and message snapshots"); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}
	return tx.Commit()
}

func (m *SchemaManager) Version(ctx context.Context, db *sql.DB) (uint, bool, error) {
	return m.GetVersion(ctx, db)
}

func (m *SchemaManager) createQueuesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS cq_queues (
            name TEXT PRIMARY KEY,
	            metadata_pb BYTEA NOT NULL,
	            dead_letter_queue_name TEXT,
            state_counts JSONB DEFAULT '{}'::jsonb,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
	        )`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_queues_dead_letter_queue_name ON cq_queues(dead_letter_queue_name) WHERE dead_letter_queue_name IS NOT NULL`)
	return err
}

func (m *SchemaManager) createMessagesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS cq_messages (
            id BIGSERIAL PRIMARY KEY,
            queue_name TEXT NOT NULL,
            message_id TEXT NOT NULL,
            metadata_pb BYTEA NOT NULL,
            state INTEGER NOT NULL,
            priority INTEGER NOT NULL DEFAULT 5,
            scheduled_at BIGINT,
            lease_expiry BIGINT,
            heartbeat_expiry BIGINT,
            attempts_left INTEGER,
            max_attempts INTEGER,
            current_attempt_id TEXT,
            current_worker_id TEXT,
            lease_started_at BIGINT,
            lease_extension_used BIGINT DEFAULT 0,
            lease_renewal_count BIGINT DEFAULT 0,
            last_heartbeat_at BIGINT,
            created_at BIGINT NOT NULL,
            updated_at BIGINT NOT NULL,
            completed_at BIGINT,
            deleted_at BIGINT,
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
	_, err := tx.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS cq_dlq (
            id BIGSERIAL PRIMARY KEY,
            queue_name TEXT NOT NULL,
            message_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            metadata_pb BYTEA NOT NULL,
            created_at BIGINT NOT NULL,
            FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE
        )`)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_dlq_queue_created ON cq_dlq(queue_name, created_at DESC)`)
	return err
}

func (m *SchemaManager) createSchedulesTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cq_schedules (
			id TEXT PRIMARY KEY,
			queue_name TEXT NOT NULL,
			metadata_pb BYTEA NOT NULL,
			state INTEGER NOT NULL,
			cron_schedule TEXT,
			next_run BIGINT,
			last_run BIGINT,
			execution_count BIGINT NOT NULL DEFAULT 0,
			created_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			FOREIGN KEY (queue_name) REFERENCES cq_queues(name) ON DELETE CASCADE
		)`)
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
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cq_schedule_history (
			id BIGSERIAL PRIMARY KEY,
			schedule_id TEXT NOT NULL,
			message_id TEXT NOT NULL,
			executed_at BIGINT NOT NULL,
			success INTEGER NOT NULL,
			error_message TEXT,
			message_pb BYTEA
		)`)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_schedule_history_schedule ON cq_schedule_history(schedule_id, executed_at DESC)`)
	return err
}

func (m *SchemaManager) createScheduleArchiveTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS cq_schedule_archive (
			schedule_id TEXT PRIMARY KEY,
			metadata_pb BYTEA NOT NULL,
			deleted_at BIGINT NOT NULL
		)`)
	return err
}
