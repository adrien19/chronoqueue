package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

// RedisRegistry implements Registry using Redis as the storage backend
type RedisRegistry struct {
	redisClient *redis.Client
	logger      *log.Logger
	cacheTTL    time.Duration
}

// NewRedisRegistry creates a new Redis-based schema registry
func NewRedisRegistry(redisClient *redis.Client, logger *log.Logger) *RedisRegistry {
	return &RedisRegistry{
		redisClient: redisClient,
		logger:      logger,
		cacheTTL:    1 * time.Hour, // Cache schemas for 1 hour
	}
}

// Register registers a new schema or creates a new version
func (r *RedisRegistry) Register(ctx context.Context, schema *schema_pb.Schema) (SchemaMetadata, error) {
	// Validate schema content
	if err := validateSchemaContent(schema.Content); err != nil {
		return SchemaMetadata{}, fmt.Errorf("invalid schema content: %w", err)
	}

	// Get or create version number
	if schema.Version == 0 {
		// Auto-increment version
		latestVersion, err := r.getLatestVersion(ctx, schema.SchemaId)
		if err != nil && err != redis.Nil {
			return SchemaMetadata{}, fmt.Errorf("failed to get latest version: %w", err)
		}
		schema.Version = latestVersion + 1
	}

	// Set timestamps
	now := time.Now().UnixMilli()
	if schema.CreatedAt == 0 {
		schema.CreatedAt = now
	}
	schema.UpdatedAt = now
	schema.IsActive = true

	// Serialize schema
	marshaller := protojson.MarshalOptions{EmitUnpopulated: true}
	schemaBytes, err := marshaller.Marshal(schema)
	if err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to serialize schema: %w", err)
	}

	// Store in Redis
	schemaKey := r.schemaKey(schema.SchemaId, schema.Version)

	// Use pipeline for atomic operations
	pipe := r.redisClient.TxPipeline()

	// Store schema
	pipe.Set(ctx, schemaKey, schemaBytes, 0)

	// Add version to sorted set
	pipe.ZAdd(ctx, r.versionsKey(schema.SchemaId), redis.Z{
		Score:  float64(schema.Version),
		Member: schema.Version,
	})

	// Update latest version
	pipe.Set(ctx, r.latestKey(schema.SchemaId), schema.Version, 0)

	// Add to active schemas set
	pipe.SAdd(ctx, r.activeSchemasKey(), schema.SchemaId)

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return SchemaMetadata{}, fmt.Errorf("failed to store schema: %w", err)
	}

	r.logger.InfoWithFields("Schema registered",
		"schemaId", schema.SchemaId,
		"version", schema.Version,
		"name", schema.Name)

	return SchemaMetadata{
		SchemaID:      schema.SchemaId,
		LatestVersion: schema.Version,
	}, nil
}

// Get retrieves a specific schema version
func (r *RedisRegistry) Get(ctx context.Context, schemaID string, version int32) (*schema_pb.Schema, error) {
	// If version is 0, get the latest version
	if version == 0 {
		return r.GetLatest(ctx, schemaID)
	}

	schemaKey := r.schemaKey(schemaID, version)

	schemaBytes, err := r.redisClient.Get(ctx, schemaKey).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("schema not found: %s version %d", schemaID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	var schema schema_pb.Schema
	if err := protojson.Unmarshal(schemaBytes, &schema); err != nil {
		return nil, fmt.Errorf("failed to deserialize schema: %w", err)
	}

	return &schema, nil
}

// GetLatest retrieves the latest version of a schema
func (r *RedisRegistry) GetLatest(ctx context.Context, schemaID string) (*schema_pb.Schema, error) {
	version, err := r.getLatestVersion(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version: %w", err)
	}

	return r.Get(ctx, schemaID, version)
}

// List lists all active schemas
func (r *RedisRegistry) List(ctx context.Context) ([]*schema_pb.Schema, error) {
	// Get all active schema IDs
	schemaIDs, err := r.redisClient.SMembers(ctx, r.activeSchemasKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}

	schemas := make([]*schema_pb.Schema, 0, len(schemaIDs))
	for _, schemaID := range schemaIDs {
		schema, err := r.GetLatest(ctx, schemaID)
		if err != nil {
			r.logger.ErrorWithFields("Failed to get schema", "schemaId", schemaID, "error", err)
			continue
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// Deactivate marks a schema version as inactive
func (r *RedisRegistry) Deactivate(ctx context.Context, schemaID string, version int32) error {
	schema, err := r.Get(ctx, schemaID, version)
	if err != nil {
		return err
	}

	schema.IsActive = false
	schema.UpdatedAt = time.Now().UnixMilli()

	// Serialize and store
	marshaller := protojson.MarshalOptions{EmitUnpopulated: true}
	schemaBytes, err := marshaller.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to serialize schema: %w", err)
	}

	schemaKey := r.schemaKey(schemaID, version)
	if err := r.redisClient.Set(ctx, schemaKey, schemaBytes, 0).Err(); err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	r.logger.InfoWithFields("Schema deactivated", "schemaId", schemaID, "version", version)
	return nil
}

// Validate validates a JSON payload against a schema
func (r *RedisRegistry) Validate(ctx context.Context, schemaID string, version int32, payload []byte) (*schema_pb.ValidationResult, error) {
	// Get schema
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

	result.SchemaId = schemaID
	result.SchemaVersion = schema.Version
	result.ValidatedAt = time.Now().UnixMilli()

	return result, nil
}

// IsCompatible checks if a new schema content is compatible with existing versions
func (r *RedisRegistry) IsCompatible(ctx context.Context, schemaID string, newContent string) (bool, error) {
	// Get latest schema
	latest, err := r.GetLatest(ctx, schemaID)
	if err == redis.Nil {
		// No existing schema, so new schema is compatible
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get latest schema: %w", err)
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
			r.logger.WarnWithFields("Schema incompatibility detected",
				"schemaId", schemaID,
				"missingField", field)
			return false, nil
		}
	}

	return true, nil
}

// Helper methods

func (r *RedisRegistry) schemaKey(schemaID string, version int32) string {
	return fmt.Sprintf("schema:%s:%d", schemaID, version)
}

func (r *RedisRegistry) versionsKey(schemaID string) string {
	return fmt.Sprintf("schema:%s:versions", schemaID)
}

func (r *RedisRegistry) latestKey(schemaID string) string {
	return fmt.Sprintf("schema:%s:latest", schemaID)
}

func (r *RedisRegistry) activeSchemasKey() string {
	return "schemas:active"
}

func (r *RedisRegistry) getLatestVersion(ctx context.Context, schemaID string) (int32, error) {
	versionStr, err := r.redisClient.Get(ctx, r.latestKey(schemaID)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	version, err := strconv.ParseInt(versionStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid version number: %w", err)
	}

	return int32(version), nil
}

func validateSchemaContent(content string) error {
	// Validate that content is valid JSON
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(content), &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Check for required JSON Schema fields
	if _, ok := schema["type"]; !ok {
		return fmt.Errorf("schema missing required 'type' field")
	}

	return nil
}

func getRequiredFields(schema map[string]interface{}) map[string]bool {
	required := make(map[string]bool)

	if requiredArray, ok := schema["required"].([]interface{}); ok {
		for _, field := range requiredArray {
			if fieldStr, ok := field.(string); ok {
				required[fieldStr] = true
			}
		}
	}

	return required
}
