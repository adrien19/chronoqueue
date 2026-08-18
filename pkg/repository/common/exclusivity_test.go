package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestValidateQueueExclusivity(t *testing.T) {
	require.NoError(t, ValidateQueueExclusivity(&queuepb.QueueMetadata{Type: queuepb.QueueType_SIMPLE}))
	require.Error(t, ValidateQueueExclusivity(&queuepb.QueueMetadata{Type: queuepb.QueueType_EXCLUSIVE}))
	require.NoError(t, ValidateQueueExclusivity(&queuepb.QueueMetadata{Type: queuepb.QueueType_EXCLUSIVE, ExclusivityKey: "orders"}))
}

func TestValidateClaimExclusivity(t *testing.T) {
	metadata := &queuepb.QueueMetadata{Type: queuepb.QueueType_EXCLUSIVE, ExclusivityKey: "orders"}
	require.Error(t, ValidateClaimExclusivity(metadata, ""))
	require.Error(t, ValidateClaimExclusivity(metadata, "other"))
	require.NoError(t, ValidateClaimExclusivity(metadata, "orders"))
	require.NoError(t, ValidateClaimExclusivity(&queuepb.QueueMetadata{Type: queuepb.QueueType_SIMPLE}, "ignored"))
}
