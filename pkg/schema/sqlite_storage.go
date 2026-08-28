//go:build sqlite
// +build sqlite

package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// SQLiteRegistry implements Registry using SQLite as the storage backend
type SQLiteRegistry struct {
	db     *sql.DB
	logger *log.Logger
}

// NewSQLiteRegistry creates a new SQLite-based schema registry
func NewSQLiteRegistry(db *sql.DB, logger *log.Logger) (*SQLiteRegistry, error) {
	registry := &SQLiteRegistry{
		db:     db,
		logger: logger,
	}

	// Initialize schema tables
	if err := registry.initSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize schema tables: %w", err)
	}

	return registry, nil
}

// initSchema creates the required tables for schema storage
func (s *SQLiteRegistry) initSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS cq_schemas (
		schema_id    TEXT NOT NULL,
		version      INTEGER NOT NULL,
		name         TEXT NOT NULL,
		description  TEXT,
		content      TEXT NOT NULL,
		content_type TEXT NOT NULL DEFAULT 'json-schema',
		metadata_json TEXT NOT NULL DEFAULT '{}',
		is_active    INTEGER NOT NULL DEFAULT 1,
		created_at   INTEGER NOT NULL,
		updated_at   INTEGER NOT NULL,
		PRIMARY KEY (schema_id, version)
	);

	CREATE INDEX IF NOT EXISTS cq_schemas_active_idx
		ON cq_schemas (schema_id, is_active, version DESC);

	CREATE INDEX IF NOT EXISTS cq_schemas_latest_idx
		ON cq_schemas (schema_id, version DESC);
	`

	_, err := s.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create schema tables: %w", err)
	}
	if err := s.ensureMetadataColumn(ctx); err != nil {
		return err
	}

	s.logger.Info("Schema registry tables initialized")
	return nil
}

func (s *SQLiteRegistry) ensureMetadataColumn(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(cq_schemas)`)
	if err != nil {
		return fmt.Errorf("inspect schema table: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("scan schema table column: %w", err)
		}
		if name == "metadata_json" {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema table columns: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE cq_schemas ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'`); err != nil {
		return fmt.Errorf("add schema metadata column: %w", err)
	}
	return nil
}

// Register registers a new schema or creates a new version
func (s *SQLiteRegistry) Register(ctx context.Context, schema *schema_pb.Schema) (SchemaMetadata, error) {
	if schema.ContentType != "" && schema.ContentType != "json-schema" {
		return SchemaMetadata{}, domainerror.InvalidWithFields("unsupported schema content type", []domainerror.FieldViolation{{Field: "content_type", Description: "must be json-schema"}}, nil)
	}
	// Validate schema content
	if err := validateSchemaContent(schema.ContentType, schema.Content); err != nil {
		return SchemaMetadata{}, domainerror.New(domainerror.InvalidArgument, "invalid schema content", err)
	}

	// Start transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Set timestamps
	now := time.Now().UnixMilli()
	if schema.CreatedAt == 0 {
		schema.CreatedAt = now
	}
	schema.UpdatedAt = now
	schema.IsActive = true

	// Set default content type
	if schema.ContentType == "" {
		schema.ContentType = "json-schema"
	}
	metadataJSON, err := encodeMetadata(schema.Metadata)
	if err != nil {
		return SchemaMetadata{}, err
	}

	var insertResult sql.Result
	if schema.Version == 0 {
		err = tx.QueryRowContext(ctx, `
		INSERT INTO cq_schemas (
			schema_id, version, name, description, content,
			content_type, metadata_json, is_active, created_at, updated_at
		) SELECT ?, COALESCE(MAX(version), 0) + 1, ?, ?, ?, ?, ?, ?, ?, ?
		FROM cq_schemas WHERE schema_id = ?
		RETURNING version
	`, schema.SchemaId, schema.Name, schema.Description, schema.Content, schema.ContentType, metadataJSON, 1,
			schema.CreatedAt, schema.UpdatedAt, schema.SchemaId).Scan(&schema.Version)
	} else {
		insertResult, err = tx.ExecContext(ctx, `
		INSERT INTO cq_schemas (
			schema_id, version, name, description, content, 
			content_type, metadata_json, is_active, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(schema_id, version) DO NOTHING
	`,
			schema.SchemaId,
			schema.Version,
			schema.Name,
			schema.Description,
			schema.Content,
			schema.ContentType,
			metadataJSON,
			1, // is_active = true
			schema.CreatedAt,
			schema.UpdatedAt,
		)
		if err == nil {
			rowsAffected, rowsErr := insertResult.RowsAffected()
			if rowsErr != nil {
				return SchemaMetadata{}, fmt.Errorf("failed to get inserted schema row count: %w", rowsErr)
			}
			if rowsAffected == 0 {
				return SchemaMetadata{}, domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("schema already exists: %s version %d", schema.SchemaId, schema.Version), nil)
			}
		}
	}
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to insert schema: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Get metadata
	metadata, err := s.getMetadata(ctx, schema.SchemaId)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	s.logger.InfoWithFields("Schema registered",
		"schemaId", schema.SchemaId,
		"version", schema.Version,
		"name", schema.Name)

	return metadata, nil
}

// Get retrieves a specific schema version
func (s *SQLiteRegistry) Get(ctx context.Context, schemaID string, version int32) (*schema_pb.Schema, error) {
	// If version is 0, get the latest version
	if version == 0 {
		return s.GetLatest(ctx, schemaID)
	}

	var schema schema_pb.Schema
	var isActive int
	var metadataJSON string

	err := s.db.QueryRowContext(ctx, `
		SELECT schema_id, version, name, description, content, 
		       content_type, metadata_json, is_active, created_at, updated_at
		FROM cq_schemas
		WHERE schema_id = ? AND version = ?
	`, schemaID, version).Scan(
		&schema.SchemaId,
		&schema.Version,
		&schema.Name,
		&schema.Description,
		&schema.Content,
		&schema.ContentType,
		&metadataJSON,
		&isActive,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, domainerror.New(domainerror.NotFound, fmt.Sprintf("schema not found: %s version %d", schemaID, version), err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	schema.IsActive = isActive == 1
	schema.Metadata, err = decodeMetadata(metadataJSON)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}

// GetLatest retrieves the latest version of a schema
func (s *SQLiteRegistry) GetLatest(ctx context.Context, schemaID string) (*schema_pb.Schema, error) {
	var schema schema_pb.Schema
	var isActive int
	var metadataJSON string

	err := s.db.QueryRowContext(ctx, `
		SELECT schema_id, version, name, description, content, 
		       content_type, metadata_json, is_active, created_at, updated_at
		FROM cq_schemas
		WHERE schema_id = ?
		ORDER BY version DESC
		LIMIT 1
	`, schemaID).Scan(
		&schema.SchemaId,
		&schema.Version,
		&schema.Name,
		&schema.Description,
		&schema.Content,
		&schema.ContentType,
		&metadataJSON,
		&isActive,
		&schema.CreatedAt,
		&schema.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, domainerror.New(domainerror.NotFound, fmt.Sprintf("schema %q not found", schemaID), err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get latest schema: %w", err)
	}

	schema.IsActive = isActive == 1
	schema.Metadata, err = decodeMetadata(metadataJSON)
	if err != nil {
		return nil, err
	}

	return &schema, nil
}

// List lists all active schemas (latest version of each)
func (s *SQLiteRegistry) List(ctx context.Context) ([]*schema_pb.Schema, error) {
	result, err := s.ListWithOptions(ctx, ListOptions{Limit: math.MaxInt32, ActiveOnly: true})
	return result.Schemas, err
}

// ListWithOptions lists the latest schema matching the requested filters.
func (s *SQLiteRegistry) ListWithOptions(ctx context.Context, options ListOptions) (ListResult, error) {
	if options.Limit <= 0 {
		options.Limit = math.MaxInt32
	}

	rows, err := s.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT s.*, ROW_NUMBER() OVER (PARTITION BY schema_id ORDER BY version DESC) AS row_number,
			       COUNT(*) OVER (PARTITION BY schema_id) AS version_count,
			       MIN(created_at) OVER (PARTITION BY schema_id) AS first_created_at,
			       MAX(updated_at) OVER (PARTITION BY schema_id) AS last_updated_at
			FROM cq_schemas s
			WHERE instr(schema_id, ?) = 1
		), latest AS (
			SELECT * FROM ranked WHERE row_number = 1
		)
		SELECT schema_id, version, name, description, content, content_type, metadata_json,
		       is_active, created_at, updated_at, version_count, first_created_at,
		       last_updated_at, COUNT(*) OVER ()
		FROM latest
		WHERE (? = 0 OR is_active = 1)
		ORDER BY schema_id
		LIMIT ?
	`, options.Prefix, options.ActiveOnly, options.Limit)
	if err != nil {
		return ListResult{}, fmt.Errorf("failed to list schemas: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.logger.DPanic("failed to close schema list rows: ", closeErr)
		}
	}()

	var schemas []*schema_pb.Schema
	var totalCount int32
	metadata := make(map[string]SchemaMetadata)
	for rows.Next() {
		var schema schema_pb.Schema
		var isActive int
		var versionCount int32
		var firstCreatedAt int64
		var lastUpdatedAt int64
		var metadataJSON string

		err := rows.Scan(
			&schema.SchemaId,
			&schema.Version,
			&schema.Name,
			&schema.Description,
			&schema.Content,
			&schema.ContentType,
			&metadataJSON,
			&isActive,
			&schema.CreatedAt,
			&schema.UpdatedAt,
			&versionCount,
			&firstCreatedAt,
			&lastUpdatedAt,
			&totalCount,
		)
		if err != nil {
			s.logger.ErrorWithFields("Failed to scan schema", "error", err)
			continue
		}

		schema.IsActive = isActive == 1
		schema.Metadata, err = decodeMetadata(metadataJSON)
		if err != nil {
			return ListResult{}, err
		}
		schemas = append(schemas, &schema)
		metadata[schema.SchemaId] = SchemaMetadata{SchemaID: schema.SchemaId, LatestVersion: schema.Version, TotalVersions: versionCount, CreatedAt: time.UnixMilli(firstCreatedAt), UpdatedAt: time.UnixMilli(lastUpdatedAt)}
	}

	if err := rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("error iterating schemas: %w", err)
	}

	return ListResult{Schemas: schemas, TotalCount: totalCount, Metadata: metadata}, nil
}

// Deactivate marks a schema version as inactive
func (s *SQLiteRegistry) Deactivate(ctx context.Context, schemaID string, version int32) (int32, error) {
	query := `
		UPDATE cq_schemas
		SET is_active = 0, updated_at = ?
		WHERE schema_id = ? AND is_active = 1`
	args := []interface{}{time.Now().UnixMilli(), schemaID}
	if version != 0 {
		query += ` AND version = ?`
		args = append(args, version)
	}
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to deactivate schema: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return 0, domainerror.New(domainerror.NotFound, fmt.Sprintf("active schema not found: %s version %d", schemaID, version), nil)
	}

	s.logger.InfoWithFields("Schema deactivated", "schemaId", schemaID, "version", version)
	return int32(rowsAffected), nil
}

// Validate validates a JSON payload against a schema
func (s *SQLiteRegistry) Validate(ctx context.Context, schemaID string, version int32, payload []byte) (*schema_pb.ValidationResult, error) {
	// Get schema
	var schema *schema_pb.Schema
	var err error

	if version > 0 {
		schema, err = s.Get(ctx, schemaID, version)
	} else {
		schema, err = s.GetLatest(ctx, schemaID)
	}

	if err != nil {
		return &schema_pb.ValidationResult{
			Valid:         false,
			SchemaId:      schemaID,
			SchemaVersion: version,
			Errors: []*schema_pb.ValidationError{
				{
					Field:     "schema",
					ErrorCode: schema_pb.ErrorCode_SCHEMA_NOT_FOUND.String(),
					Message:   fmt.Sprintf("Schema not found: %s", schemaID),
				},
			},
		}, nil
	}

	// Validate payload against schema
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

	result.SchemaId = schema.SchemaId
	result.SchemaVersion = schema.Version
	result.ValidatedAt = time.Now().UnixMilli()

	return result, nil
}

// IsCompatible checks if a new schema content is compatible with existing versions
func (s *SQLiteRegistry) IsCompatible(ctx context.Context, schemaID string, newContent string) (bool, error) {
	// Get latest schema
	latest, err := s.GetLatest(ctx, schemaID)
	if err != nil {
		// No existing schema, so new schema is compatible
		return true, nil
	}

	// Parse both schemas
	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(latest.Content), &oldSchema); err != nil {
		return false, fmt.Errorf("failed to parse old schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newContent), &newSchema); err != nil {
		return false, fmt.Errorf("failed to parse new schema: %w", err)
	}

	// Basic compatibility check: new schema should not remove required fields
	// This is a simplified check - full compatibility checking would be more complex
	oldRequired := getRequiredFields(oldSchema)
	newRequired := getRequiredFields(newSchema)

	for field := range oldRequired {
		if !newRequired[field] {
			s.logger.WarnWithFields("Schema incompatibility detected",
				"schemaId", schemaID,
				"missingField", field)
			return false, nil
		}
	}

	return true, nil
}

// Helper methods

func (s *SQLiteRegistry) getLatestVersionTx(ctx context.Context, tx *sql.Tx, schemaID string) (int32, error) {
	var version sql.NullInt32

	err := tx.QueryRowContext(ctx, `
		SELECT MAX(version)
		FROM cq_schemas
		WHERE schema_id = ?
	`, schemaID).Scan(&version)

	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	if !version.Valid {
		return 0, nil
	}

	return version.Int32, nil
}

func (s *SQLiteRegistry) getMetadata(ctx context.Context, schemaID string) (SchemaMetadata, error) {
	var metadata SchemaMetadata
	var totalVersions int32
	var latestVersion int32
	var createdAt, updatedAt int64

	// Get aggregate metadata
	err := s.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(*) as total_versions,
			MAX(version) as latest_version,
			MIN(created_at) as created_at,
			MAX(updated_at) as updated_at
		FROM cq_schemas
		WHERE schema_id = ?
	`, schemaID).Scan(&totalVersions, &latestVersion, &createdAt, &updatedAt)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	metadata.SchemaID = schemaID
	metadata.TotalVersions = totalVersions
	metadata.LatestVersion = latestVersion
	metadata.CreatedAt = time.UnixMilli(createdAt)
	metadata.UpdatedAt = time.UnixMilli(updatedAt)

	return metadata, nil
}
