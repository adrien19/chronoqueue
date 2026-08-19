package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// PostgresRegistry implements Registry using PostgreSQL as the storage backend.
type PostgresRegistry struct {
	db     *sql.DB
	logger *log.Logger
}

// NewPostgresRegistry creates a new PostgreSQL-based schema registry.
func NewPostgresRegistry(db *sql.DB, logger *log.Logger) (*PostgresRegistry, error) {
	registry := &PostgresRegistry{
		db:     db,
		logger: logger,
	}

	if err := registry.initSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize schema tables: %w", err)
	}

	return registry, nil
}

func (r *PostgresRegistry) initSchema(ctx context.Context) error {
	schema := `
    CREATE TABLE IF NOT EXISTS cq_schemas (
        schema_id    TEXT NOT NULL,
        version      INTEGER NOT NULL,
        name         TEXT NOT NULL,
        description  TEXT,
        content      TEXT NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'json-schema',
        is_active    BOOLEAN NOT NULL DEFAULT TRUE,
        created_at   BIGINT NOT NULL,
        updated_at   BIGINT NOT NULL,
        PRIMARY KEY (schema_id, version)
    );

    CREATE INDEX IF NOT EXISTS cq_schemas_active_idx
        ON cq_schemas (schema_id, is_active, version DESC);

    CREATE INDEX IF NOT EXISTS cq_schemas_latest_idx
        ON cq_schemas (schema_id, version DESC);
    `

	if _, err := r.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("failed to create schema tables: %w", err)
	}

	r.logger.Info("Schema registry tables initialized")
	return nil
}

// Register registers a new schema or creates a new version.
func (r *PostgresRegistry) Register(ctx context.Context, schema *schema_pb.Schema) (SchemaMetadata, error) {
	if err := validateSchemaContent(schema.Content); err != nil {
		return SchemaMetadata{}, fmt.Errorf("invalid schema content: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if schema.Version == 0 {
		latestVersion, err := r.getLatestVersionTx(ctx, tx, schema.SchemaId)
		if err != nil {
			return SchemaMetadata{}, fmt.Errorf("failed to get latest version: %w", err)
		}
		schema.Version = latestVersion + 1
	}

	now := time.Now().UnixMilli()
	if schema.CreatedAt == 0 {
		schema.CreatedAt = now
	}
	schema.UpdatedAt = now
	schema.IsActive = true

	if schema.ContentType == "" {
		schema.ContentType = "json-schema"
	}

	if _, err := tx.ExecContext(
		ctx, `
        INSERT INTO cq_schemas (
            schema_id, version, name, description, content,
            content_type, is_active, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(schema_id, version) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            content = EXCLUDED.content,
            content_type = EXCLUDED.content_type,
            is_active = EXCLUDED.is_active,
            updated_at = EXCLUDED.updated_at
    `,
		schema.SchemaId,
		schema.Version,
		schema.Name,
		schema.Description,
		schema.Content,
		schema.ContentType,
		true,
		schema.CreatedAt,
		schema.UpdatedAt,
	); err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to insert schema: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	metadata, err := r.getMetadata(ctx, schema.SchemaId)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	r.logger.InfoWithFields("Schema registered",
		"schemaId", schema.SchemaId,
		"version", schema.Version,
		"name", schema.Name)

	return metadata, nil
}

// Get retrieves a specific schema version.
func (r *PostgresRegistry) Get(ctx context.Context, schemaID string, version int32) (*schema_pb.Schema, error) {
	if version == 0 {
		return r.GetLatest(ctx, schemaID)
	}

	var schema schema_pb.Schema
	var isActive bool

	err := r.db.QueryRowContext(ctx, `
        SELECT schema_id, version, name, description, content,
               content_type, is_active, created_at, updated_at
        FROM cq_schemas
        WHERE schema_id = $1 AND version = $2
    `, schemaID, version).Scan(
		&schema.SchemaId,
		&schema.Version,
		&schema.Name,
		&schema.Description,
		&schema.Content,
		&schema.ContentType,
		&isActive,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schema not found: %s version %d", schemaID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	schema.IsActive = isActive

	return &schema, nil
}

// GetLatest retrieves the latest version of a schema.
func (r *PostgresRegistry) GetLatest(ctx context.Context, schemaID string) (*schema_pb.Schema, error) {
	var schema schema_pb.Schema
	var isActive bool

	err := r.db.QueryRowContext(ctx, `
        SELECT schema_id, version, name, description, content,
               content_type, is_active, created_at, updated_at
        FROM cq_schemas
        WHERE schema_id = $1
        ORDER BY version DESC
        LIMIT 1
    `, schemaID).Scan(
		&schema.SchemaId,
		&schema.Version,
		&schema.Name,
		&schema.Description,
		&schema.Content,
		&schema.ContentType,
		&isActive,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schema not found: %s", schemaID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest schema: %w", err)
	}

	schema.IsActive = isActive

	return &schema, nil
}

// List lists all active schemas (latest version of each).
func (r *PostgresRegistry) List(ctx context.Context) ([]*schema_pb.Schema, error) {
	result, err := r.ListWithOptions(ctx, ListOptions{Limit: math.MaxInt32, ActiveOnly: true})
	return result.Schemas, err
}

// ListWithOptions lists the latest schema matching the requested filters.
func (r *PostgresRegistry) ListWithOptions(ctx context.Context, options ListOptions) (ListResult, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT s.*, ROW_NUMBER() OVER (PARTITION BY schema_id ORDER BY version DESC) AS row_number
			FROM cq_schemas s
			WHERE STRPOS(schema_id, $2) = 1
		), latest AS (
			SELECT * FROM ranked WHERE row_number = 1
		)
		SELECT schema_id, version, name, description, content, content_type,
		       is_active, created_at, updated_at, COUNT(*) OVER ()
		FROM latest
		WHERE ($1 = FALSE OR is_active = TRUE)
		ORDER BY schema_id
		LIMIT $3
	`, options.ActiveOnly, options.Prefix, options.Limit)
	if err != nil {
		return ListResult{}, fmt.Errorf("failed to list schemas: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.DPanic("failed to close schema list rows: ", closeErr)
		}
	}()

	var schemas []*schema_pb.Schema
	var totalCount int32
	for rows.Next() {
		var schema schema_pb.Schema
		var isActive bool

		if err := rows.Scan(
			&schema.SchemaId,
			&schema.Version,
			&schema.Name,
			&schema.Description,
			&schema.Content,
			&schema.ContentType,
			&isActive,
			&schema.CreatedAt,
			&schema.UpdatedAt,
			&totalCount,
		); err != nil {
			r.logger.ErrorWithFields("Failed to scan schema", "error", err)
			continue
		}

		schema.IsActive = isActive
		schemas = append(schemas, &schema)
	}

	if err := rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("error iterating schemas: %w", err)
	}

	return ListResult{Schemas: schemas, TotalCount: totalCount}, nil
}

// Deactivate marks a schema version as inactive.
func (r *PostgresRegistry) Deactivate(ctx context.Context, schemaID string, version int32) error {
	result, err := r.db.ExecContext(ctx, `
        UPDATE cq_schemas
        SET is_active = FALSE, updated_at = $1
        WHERE schema_id = $2 AND version = $3
    `, time.Now().UnixMilli(), schemaID, version)
	if err != nil {
		return fmt.Errorf("failed to deactivate schema: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("schema not found: %s version %d", schemaID, version)
	}

	r.logger.InfoWithFields("Schema deactivated", "schemaId", schemaID, "version", version)
	return nil
}

// Validate validates a JSON payload against a schema.
func (r *PostgresRegistry) Validate(ctx context.Context, schemaID string, version int32, payload []byte) (*schema_pb.ValidationResult, error) {
	var schema *schema_pb.Schema
	var err error

	if version > 0 {
		schema, err = r.Get(ctx, schemaID, version)
	} else {
		schema, err = r.GetLatest(ctx, schemaID)
	}

	if err != nil {
		return &schema_pb.ValidationResult{
			Valid: false,
			Errors: []*schema_pb.ValidationError{
				{
					Field:     "schema",
					ErrorCode: schema_pb.ErrorCode_SCHEMA_NOT_FOUND.String(),
					Message:   fmt.Sprintf("Schema not found: %s", schemaID),
				},
			},
		}, nil
	}

	result, err := ValidateJSONSchema(schema.Content, payload)
	if err != nil {
		return &schema_pb.ValidationResult{
			Valid: false,
			Errors: []*schema_pb.ValidationError{
				{
					Field:     "payload",
					ErrorCode: schema_pb.ErrorCode_SCHEMA_VALIDATION_FAILED.String(),
					Message:   fmt.Sprintf("Validation failed: %s", err.Error()),
				},
			},
		}, nil
	}

	result.SchemaId = schemaID
	result.SchemaVersion = schema.Version
	result.ValidatedAt = time.Now().UnixMilli()

	return result, nil
}

// IsCompatible checks if a new schema content is compatible with existing versions.
func (r *PostgresRegistry) IsCompatible(ctx context.Context, schemaID string, newContent string) (bool, error) {
	latest, err := r.GetLatest(ctx, schemaID)
	if err != nil {
		return true, nil
	}

	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(latest.Content), &oldSchema); err != nil {
		return false, fmt.Errorf("failed to parse old schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newContent), &newSchema); err != nil {
		return false, fmt.Errorf("failed to parse new schema: %w", err)
	}

	oldRequired := getRequiredFields(oldSchema)
	newRequired := getRequiredFields(newSchema)

	for field := range oldRequired {
		if !newRequired[field] {
			r.logger.WarnWithFields("Schema incompatibility detected",
				"schemaId", schemaID,
				"missingField", field)
			return false, nil
		}
	}

	return true, nil
}

func (r *PostgresRegistry) getLatestVersionTx(ctx context.Context, tx *sql.Tx, schemaID string) (int32, error) {
	var version sql.NullInt32

	if err := tx.QueryRowContext(ctx, `
        SELECT COALESCE(MAX(version), 0)
        FROM cq_schemas
        WHERE schema_id = $1
    `, schemaID).Scan(&version); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	if version.Valid {
		return version.Int32, nil
	}
	return 0, nil
}

func (r *PostgresRegistry) getMetadata(ctx context.Context, schemaID string) (SchemaMetadata, error) {
	var metadata SchemaMetadata
	var totalVersions int32

	err := r.db.QueryRowContext(ctx, `
        SELECT MAX(version) AS latest_version, COUNT(*) AS total_versions
        FROM cq_schemas
        WHERE schema_id = $1
    `, schemaID).Scan(&metadata.LatestVersion, &totalVersions)
	if err != nil {
		return SchemaMetadata{}, err
	}

	metadata.SchemaID = schemaID
	metadata.TotalVersions = totalVersions

	var createdAt, updatedAt int64
	err = r.db.QueryRowContext(ctx, `
        SELECT created_at, updated_at
        FROM cq_schemas
        WHERE schema_id = $1 AND version = $2
    `, schemaID, metadata.LatestVersion).Scan(&createdAt, &updatedAt)
	if err != nil {
		return SchemaMetadata{}, err
	}

	metadata.CreatedAt = time.UnixMilli(createdAt)
	metadata.UpdatedAt = time.UnixMilli(updatedAt)

	return metadata, nil
}
