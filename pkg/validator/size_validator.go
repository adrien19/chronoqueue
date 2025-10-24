package validator

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
)

const (
	// Default size limits
	DefaultMaxMessageSize       = 150 * 1024 // 150 KB
	DefaultMaxPayloadSize       = 100 * 1024 // 100 KB
	DefaultMaxMetadataKeySize   = 256        // 256 bytes
	DefaultMaxMetadataValueSize = 4096       // 4 KB
	DefaultMaxMessageIDSize     = 512        // 512 bytes
)

// SizeValidator validates message sizes
type SizeValidator struct {
	maxMessageSize       int
	maxPayloadSize       int
	maxMetadataKeySize   int
	maxMetadataValueSize int
	maxMessageIDSize     int
	queueMeta            *queue_pb.QueueMetadata
}

// NewSizeValidator creates a new size validator
func NewSizeValidator(queueMeta *queue_pb.QueueMetadata) *SizeValidator {
	validator := &SizeValidator{
		maxMessageSize:       getEnvInt("CHRONOQUEUE_MAX_MESSAGE_SIZE", DefaultMaxMessageSize),
		maxPayloadSize:       getEnvInt("CHRONOQUEUE_MAX_PAYLOAD_SIZE", DefaultMaxPayloadSize),
		maxMetadataKeySize:   getEnvInt("CHRONOQUEUE_MAX_METADATA_KEY_SIZE", DefaultMaxMetadataKeySize),
		maxMetadataValueSize: getEnvInt("CHRONOQUEUE_MAX_METADATA_VALUE_SIZE", DefaultMaxMetadataValueSize),
		maxMessageIDSize:     getEnvInt("CHRONOQUEUE_MAX_MESSAGE_ID_SIZE", DefaultMaxMessageIDSize),
		queueMeta:            queueMeta,
	}

	// Override payload size if queue has custom limit
	if queueMeta != nil && queueMeta.MaxPayloadSize > 0 {
		validator.maxPayloadSize = int(queueMeta.MaxPayloadSize)
	}

	return validator
}

// Validate validates the size of a message
func (v *SizeValidator) Validate(ctx context.Context, msg *message_pb.Message) *ValidationResult {
	result := NewValidationResult()

	// Validate message ID size
	if len(msg.MessageId) > v.maxMessageIDSize {
		result.AddError(
			NewValidationError(
				"message_id",
				schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
				"Message ID exceeds maximum size",
			).WithDetail("size", fmt.Sprintf("%d", len(msg.MessageId))).
				WithDetail("max_size", fmt.Sprintf("%d", v.maxMessageIDSize)),
		)
	}

	if msg.Metadata == nil || msg.Metadata.Payload == nil {
		return result
	}

	payload := msg.Metadata.Payload

	// Validate metadata key/value sizes
	for key, value := range payload.Metadata {
		if len(key) > v.maxMetadataKeySize {
			result.AddError(
				NewValidationError(
					fmt.Sprintf("payload.metadata[%s]", key),
					schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
					"Metadata key exceeds maximum size",
				).WithDetail("key", key).
					WithDetail("size", fmt.Sprintf("%d", len(key))).
					WithDetail("max_size", fmt.Sprintf("%d", v.maxMetadataKeySize)),
			)
		}

		valueStr := value.String()
		if len(valueStr) > v.maxMetadataValueSize {
			result.AddError(
				NewValidationError(
					fmt.Sprintf("payload.metadata[%s]", key),
					schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
					"Metadata value exceeds maximum size",
				).WithDetail("key", key).
					WithDetail("size", fmt.Sprintf("%d", len(valueStr))).
					WithDetail("max_size", fmt.Sprintf("%d", v.maxMetadataValueSize)),
			)
		}
	}

	// Validate payload data size
	if payload.Data != nil {
		dataSize := calculateDataSize(payload.Data)
		if dataSize > v.maxPayloadSize {
			result.AddError(
				NewValidationError(
					"payload.data",
					schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
					"Payload data exceeds maximum size",
				).WithDetail("size", fmt.Sprintf("%d", dataSize)).
					WithDetail("max_size", fmt.Sprintf("%d", v.maxPayloadSize)),
			)
		}
	}

	// Validate total message size
	totalSize := calculateTotalMessageSize(msg)
	if totalSize > v.maxMessageSize {
		result.AddError(
			NewValidationError(
				"message",
				schema_pb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
				"Total message size exceeds maximum",
			).WithDetail("size", fmt.Sprintf("%d", totalSize)).
				WithDetail("max_size", fmt.Sprintf("%d", v.maxMessageSize)),
		)
	}

	return result
}

// calculateDataSize calculates the size of the data field
func calculateDataSize(data interface{}) int {
	// Serialize to JSON to get accurate size
	marshaller := protojson.MarshalOptions{}
	bytes, err := marshaller.Marshal(data.(proto.Message))
	if err != nil {
		// Fallback to string representation
		return len(fmt.Sprintf("%v", data))
	}
	return len(bytes)
}

// calculateTotalMessageSize calculates the total size of the message
func calculateTotalMessageSize(msg *message_pb.Message) int {
	marshaller := protojson.MarshalOptions{EmitUnpopulated: true}
	bytes, err := marshaller.Marshal(msg)
	if err != nil {
		// Fallback to approximate calculation
		size := len(msg.MessageId)
		if msg.Metadata != nil && msg.Metadata.Payload != nil {
			size += calculateDataSize(msg.Metadata.Payload.Data)
			for k, v := range msg.Metadata.Payload.Metadata {
				size += len(k) + len(v.String())
			}
		}
		return size
	}
	return len(bytes)
}

// getEnvInt gets an integer from environment variable with fallback
func getEnvInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return fallback
}
