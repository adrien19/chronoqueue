package validator

import (
	"fmt"
	"mime"
	"regexp"
	"strings"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

const MaxQueueNameLength = 255

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type ConfigurationError struct {
	Field   string
	Message string
}

func (e *ConfigurationError) Error() string {
	return e.Message
}

func configurationError(field string, format string, args ...any) error {
	return &ConfigurationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

func ValidateQueue(name string, metadata *queuepb.QueueMetadata) error {
	if name == "" {
		return configurationError("name", "queue name is required")
	}
	if len(name) > MaxQueueNameLength {
		return configurationError("name", "queue name must not exceed %d characters", MaxQueueNameLength)
	}
	if !queueNamePattern.MatchString(name) {
		return configurationError("name", "queue name must start with an alphanumeric character and contain only alphanumeric characters, dots, underscores, or hyphens")
	}
	if metadata == nil {
		return nil
	}
	if _, ok := queuepb.QueueType_name[int32(metadata.GetType())]; !ok {
		return configurationError("metadata.type", "queue type is invalid: %d", metadata.GetType())
	}
	if metadata.GetType() == queuepb.QueueType_EXCLUSIVE && strings.TrimSpace(metadata.GetExclusivityKey()) == "" {
		return configurationError("metadata.exclusivity_key", "exclusivity key is required for an EXCLUSIVE queue")
	}
	if metadata.GetDefaultMaxAttempts() < -1 {
		return configurationError("metadata.default_max_attempts", "default_max_attempts must be -1, 0, or a positive integer")
	}
	if metadata.GetMaxPayloadSize() < 0 {
		return configurationError("metadata.max_payload_size", "max_payload_size must be >= 0")
	}
	if metadata.GetSchemaRequired() && strings.TrimSpace(metadata.GetSchemaId()) == "" {
		return configurationError("metadata.schema_id", "schema_id is required when schema_required is true")
	}
	if err := validateLegacyLease(metadata); err != nil {
		return err
	}
	if err := util.ValidateLeasePolicy(metadata.GetLeasePolicy()); err != nil {
		return configurationError("metadata.lease_policy", "%s", err)
	}
	if err := validateRetentionPolicy(metadata.GetMessageRetentionPolicy()); err != nil {
		return err
	}
	if err := validatePriorityConfig(metadata.GetPriorityConfig()); err != nil {
		return err
	}
	for _, contentType := range metadata.GetAllowedContentTypes() {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil || !SupportedContentTypes[strings.ToLower(mediaType)] {
			return configurationError("metadata.allowed_content_types", "allowed_content_types contains unsupported content type %q", contentType)
		}
	}
	return nil
}

func validateLegacyLease(metadata *queuepb.QueueMetadata) error {
	leaseDuration := metadata.GetLeaseDuration()
	if leaseDuration == nil {
		return nil
	}
	if err := leaseDuration.CheckValid(); err != nil {
		return configurationError("metadata.lease_duration", "lease_duration is invalid: %v", err)
	}
	if leaseDuration.AsDuration() <= 0 {
		return configurationError("metadata.lease_duration", "lease_duration must be > 0")
	}
	if policyBase := metadata.GetLeasePolicy().GetBaseLease(); policyBase != nil && policyBase.AsDuration() != leaseDuration.AsDuration() {
		return configurationError("metadata.lease_duration", "lease_duration and lease_policy.base_lease must match when both are set")
	}
	return nil
}

func validateRetentionPolicy(policy *queuepb.MessageRetentionPolicy) error {
	if policy == nil {
		return nil
	}
	if _, ok := queuepb.MessageRetentionPolicy_Mode_name[int32(policy.GetMode())]; !ok {
		return configurationError("metadata.message_retention_policy.mode", "message_retention_policy.mode is invalid: %d", policy.GetMode())
	}
	if policy.GetMode() == queuepb.MessageRetentionPolicy_RETAIN_DURATION {
		if policy.GetRetentionSeconds() <= 0 {
			return configurationError("metadata.message_retention_policy.retention_seconds", "message_retention_policy.retention_seconds must be > 0 for RETAIN_DURATION")
		}
		return nil
	}
	if policy.GetRetentionSeconds() != 0 {
		return configurationError("metadata.message_retention_policy.retention_seconds", "message_retention_policy.retention_seconds must be 0 unless mode is RETAIN_DURATION")
	}
	return nil
}

func validatePriorityConfig(config *queuepb.PriorityConfig) error {
	if config == nil {
		return nil
	}
	if _, ok := queuepb.FairnessPolicy_name[int32(config.GetPolicy())]; !ok {
		return configurationError("metadata.priority_config.policy", "priority_config.policy is invalid: %d", config.GetPolicy())
	}
	hasAgeConfig := config.GetAgeBoostThreshold() != nil || config.GetAgeBoostMultiplier() != 0
	switch config.GetPolicy() {
	case queuepb.FairnessPolicy_STRICT:
		if len(config.GetPriorityWeights()) > 0 || hasAgeConfig {
			return configurationError("metadata.priority_config", "STRICT priority policy does not accept weights or age-boost settings")
		}
	case queuepb.FairnessPolicy_WEIGHTED:
		if hasAgeConfig {
			return configurationError("metadata.priority_config", "WEIGHTED priority policy does not accept age-boost settings")
		}
	case queuepb.FairnessPolicy_AGING:
		if len(config.GetPriorityWeights()) > 0 {
			return configurationError("metadata.priority_config.priority_weights", "AGING priority policy does not accept priority weights")
		}
	}
	for priority, weight := range config.GetPriorityWeights() {
		if priority != 0 && priority != 2 && priority != 4 {
			return configurationError("metadata.priority_config.priority_weights", "priority_config.priority_weights keys must be 0, 2, or 4, got %d", priority)
		}
		if weight <= 0 {
			return configurationError("metadata.priority_config.priority_weights", "priority_config.priority_weights values must be > 0")
		}
	}
	if threshold := config.GetAgeBoostThreshold(); threshold != nil {
		if err := threshold.CheckValid(); err != nil {
			return configurationError("metadata.priority_config.age_boost_threshold", "priority_config.age_boost_threshold is invalid: %v", err)
		}
		if threshold.AsDuration() <= 0 {
			return configurationError("metadata.priority_config.age_boost_threshold", "priority_config.age_boost_threshold must be > 0")
		}
	}
	if config.GetAgeBoostMultiplier() < 0 {
		return configurationError("metadata.priority_config.age_boost_multiplier", "priority_config.age_boost_multiplier must be >= 0")
	}
	return nil
}
