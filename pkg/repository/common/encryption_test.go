package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestEncryptDecryptMessagePayload_RoundTrip(t *testing.T) {
	keyManager := newTestKeyManager(t)

	original := &messagepb.Message{
		MessageId: "msg-enc-1",
		Metadata: &messagepb.Message_Metadata{
			Payload:      buildTestPayload(t, "user-123", "demo-task"),
			State:        messagepb.Message_Metadata_PENDING,
			AttemptsLeft: 1,
			MaxAttempts:  1,
			Priority:     0,
			LeasePolicy: &commonpb.LeasePolicy{
				BaseLease: durationpb.New(30 * time.Second),
			},
		},
	}

	encrypted := proto.Clone(original).(*messagepb.Message)
	require.NoError(t, EncryptMessagePayload(encrypted, keyManager))

	// Payload should be replaced by encrypted metadata only
	assert.Nil(t, encrypted.Metadata.Payload.GetData())
	assert.Contains(t, encrypted.Metadata.Payload.GetMetadata(), "encryptedPayload")
	assert.Contains(t, encrypted.Metadata.Payload.GetMetadata(), "nonce")

	require.NoError(t, DecryptMessagePayload(encrypted, keyManager))

	decryptedPayload := encrypted.GetMetadata().GetPayload()
	require.NotNil(t, decryptedPayload)
	assert.Equal(t, "user-123", decryptedPayload.Metadata["user_id"].GetStringValue())
	assert.Equal(t, "demo-task", decryptedPayload.Metadata["task"].GetStringValue())
	require.NotNil(t, decryptedPayload.Data)
	assert.Equal(t, "order-456", decryptedPayload.Data.Fields["order_id"].GetStringValue())
}

func TestEncryptMessagePayload_KeyManagerDisabled(t *testing.T) {
	keyManager := &keymanager.EncryptionKeyManager{Enabled: false}

	message := &messagepb.Message{
		MessageId: "msg-noenc",
		Metadata: &messagepb.Message_Metadata{
			Payload: buildTestPayload(t, "user-abc", "noop"),
		},
	}

	require.NoError(t, EncryptMessagePayload(message, keyManager))

	// Should remain unchanged when encryption is disabled
	payload := message.GetMetadata().GetPayload()
	require.NotNil(t, payload)
	assert.NotContains(t, payload.GetMetadata(), "encryptedPayload")
	assert.NotNil(t, payload.GetData())
	assert.Equal(t, "user-abc", payload.Metadata["user_id"].GetStringValue())
}

func TestDecryptMessagePayload_Unencrypted_NoChange(t *testing.T) {
	keyManager := newTestKeyManager(t)

	message := &messagepb.Message{
		MessageId: "msg-plain",
		Metadata: &messagepb.Message_Metadata{
			Payload: buildTestPayload(t, "user-plain", "noop"),
		},
	}

	require.NoError(t, DecryptMessagePayload(message, keyManager))

	payload := message.GetMetadata().GetPayload()
	require.NotNil(t, payload)
	assert.Equal(t, "user-plain", payload.Metadata["user_id"].GetStringValue())
	assert.NotNil(t, payload.GetData())
}

func TestEncryptDecryptSchedulePayload_RoundTrip(t *testing.T) {
	keyManager := newTestKeyManager(t)

	schedule := &schedulepb.Schedule{
		ScheduleId: "sched-enc-1",
		Metadata: &schedulepb.Schedule_Metadata{
			Payload: buildTestPayload(t, "tenant-1", "schedule-task"),
			State:   schedulepb.Schedule_Metadata_SCHEDULED,
		},
	}

	require.NoError(t, EncryptSchedulePayload(schedule, keyManager))
	assert.Contains(t, schedule.Metadata.Payload.GetMetadata(), "encryptedPayload")
	assert.Nil(t, schedule.Metadata.Payload.GetData())

	require.NoError(t, DecryptSchedulePayload(schedule, keyManager))
	payload := schedule.GetMetadata().GetPayload()
	require.NotNil(t, payload)
	assert.Equal(t, "tenant-1", payload.Metadata["user_id"].GetStringValue())
	assert.Equal(t, "schedule-task", payload.Metadata["task"].GetStringValue())
	assert.Equal(t, "order-456", payload.Data.Fields["order_id"].GetStringValue())
}

func newTestKeyManager(t *testing.T) *keymanager.EncryptionKeyManager {
	t.Helper()

	t.Setenv("ENABLE_ENCRYPTION", "true")
	t.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef")
	t.Setenv("KEY_REFRESH_DURATION_IN_MINUTES", "60")

	logger := log.NewLogger()

	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	require.NoError(t, err)
	require.True(t, keyManager.Enabled)

	return keyManager
}

func buildTestPayload(t *testing.T, userID string, task string) *commonpb.Payload {
	t.Helper()

	metadata := map[string]*structpb.Value{
		"user_id": structpb.NewStringValue(userID),
		"task":    structpb.NewStringValue(task),
	}

	data := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"order_id": structpb.NewStringValue("order-456"),
		},
	}

	return &commonpb.Payload{
		Metadata: metadata,
		Data:     data,
	}
}

// Ensure helpers are referenced to avoid lint complaints when unused in certain builds
var _ = context.Background
