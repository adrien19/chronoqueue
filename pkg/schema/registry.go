package schema

import (
	"context"
	"time"

	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

// Registry manages message schemas
type Registry interface {
	// Register registers a new schema or updates an existing one
	Register(ctx context.Context, schema *schema_pb.Schema) (SchemaMetadata, error)

	// Get retrieves a specific schema version
	Get(ctx context.Context, schemaID string, version int32) (*schema_pb.Schema, error)

	// GetLatest retrieves the latest version of a schema
	GetLatest(ctx context.Context, schemaID string) (*schema_pb.Schema, error)

	// List lists all active schemas
	List(ctx context.Context) ([]*schema_pb.Schema, error)

	// Deactivate marks a schema version as inactive
	Deactivate(ctx context.Context, schemaID string, version int32) error

	// Validate validates a JSON payload against a schema
	Validate(ctx context.Context, schemaID string, version int32, payload []byte) (*schema_pb.ValidationResult, error)

	// IsCompatible checks if a new schema content is compatible with existing versions
	IsCompatible(ctx context.Context, schemaID string, newContent string) (bool, error)
}

// SchemaMetadata contains metadata about a schema
type SchemaMetadata struct {
	SchemaID      string
	LatestVersion int32
	TotalVersions int32
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
