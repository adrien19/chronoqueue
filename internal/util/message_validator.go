package util

import (
	"errors"
	"os"
	"strconv"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
)

const MaxMessageSize = 150 * 1024 // 150 KB

// Estimations for variable-sized fields
const (
	averageMetadataKeySize   = 50  // Assumption
	averageMetadataValueSize = 100 // Assumption
)

// Fixed sizes
const (
	sizeInt64 = 8
	sizeInt32 = 4
	sizeEnum  = 2
)

func ValidateMessageSize(msg *message_pb.Message) error {
	// Compute total size for fixed fields
	fixedSize := sizeInt64*4 + // Four int64 fields: priority, invisibility_duration, lease_duration, lease_expiry
		sizeInt32 + // One int32 field: attempts_left
		sizeEnum // One enum field: state

	// Estimate size for variable-sized fields
	variableSize := len(msg.MessageId) +
		averageMetadataKeySize*len(msg.Metadata.Payload.Metadata) +
		averageMetadataValueSize*len(msg.Metadata.Payload.Metadata) // Assume each metadata value is a string for simplicity

	// Compute the size for the data field
	dataSize := len(msg.Metadata.Payload.Data.String()) // This might need to be adjusted based on how the data is serialized

	totalSize := fixedSize + variableSize + dataSize
	allowedMessageSize, err := strconv.Atoi(envString("CHRONOQUEUE_MAX_MESSAGE_SIZE", strconv.Itoa(MaxMessageSize)))
	if err != nil {
		return err
	}

	if totalSize > allowedMessageSize {
		return errors.New("message size exceeds the maximum allowed size")
	}
	return nil
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
