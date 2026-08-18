package common

import (
	"fmt"
	"strings"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func ValidateQueueExclusivity(metadata *queuepb.QueueMetadata) error {
	if metadata.GetType() == queuepb.QueueType_EXCLUSIVE && strings.TrimSpace(metadata.GetExclusivityKey()) == "" {
		return fmt.Errorf("exclusivity key is required for an EXCLUSIVE queue")
	}
	return nil
}

func ValidateClaimExclusivity(metadata *queuepb.QueueMetadata, exclusivityKey string) error {
	if metadata.GetType() != queuepb.QueueType_EXCLUSIVE {
		return nil
	}
	if err := ValidateQueueExclusivity(metadata); err != nil {
		return err
	}
	if exclusivityKey != metadata.GetExclusivityKey() {
		return fmt.Errorf("exclusivity key does not match the queue")
	}
	return nil
}
