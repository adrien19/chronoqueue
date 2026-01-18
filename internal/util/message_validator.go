package util

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
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

// ValidateLeasePolicy validates LeasePolicy fields for correctness
// Implements validation rules committed in POST_REVIEW_DOCUMENTATION.md section 2.3
func ValidateLeasePolicy(policy *common_pb.LeasePolicy) error {
	if policy == nil {
		return nil // No policy to validate
	}

	// Extract durations
	baseLease := policy.GetBaseLease().AsDuration()
	maxExtension := policy.GetMaxExtension().AsDuration()
	heartbeatTimeout := policy.GetHeartbeatTimeout().AsDuration()
	extendStep := policy.GetExtendStep().AsDuration()

	// Rule 1: Base lease must be positive
	if baseLease <= 0 {
		return fmt.Errorf("lease_policy.base_lease must be > 0, got %v", baseLease)
	}

	// Rule 2: Base lease must be reasonable (not too long)
	if baseLease > 1*time.Hour {
		return fmt.Errorf("lease_policy.base_lease must be <= 1 hour, got %v", baseLease)
	}

	// Rule 3: ExtendStep must be positive if extensions are allowed
	if maxExtension > 0 && extendStep <= 0 {
		return fmt.Errorf("lease_policy.extend_step must be > 0 when max_extension is set, got %v", extendStep)
	}

	// Rule 4: MaxExtension must allow at least one extension
	if maxExtension > 0 && maxExtension < extendStep {
		return fmt.Errorf("lease_policy.max_extension (%v) must be >= extend_step (%v)", maxExtension, extendStep)
	}

	// Rule 5: Heartbeat timeout must allow heartbeats to arrive before total lease expires
	totalLeaseTime := baseLease + maxExtension
	if heartbeatTimeout > 0 && heartbeatTimeout >= totalLeaseTime {
		return fmt.Errorf("lease_policy.heartbeat_timeout (%v) must be < total lease time (%v = base_lease + max_extension)", heartbeatTimeout, totalLeaseTime)
	}

	// Rule 6: Total lease time sanity check (< 1 hour)
	if totalLeaseTime > 1*time.Hour {
		return fmt.Errorf("total lease time (base_lease + max_extension) must be < 1 hour, got %v", totalLeaseTime)
	}

	// Rule 7: ExtendStep should be reasonable relative to base lease
	if extendStep > baseLease {
		return fmt.Errorf("lease_policy.extend_step (%v) should not exceed base_lease (%v)", extendStep, baseLease)
	}

	return nil
}
