package common

import (
	"context"
	"encoding/json"
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
			Headers: []*messagepb.Message_Metadata_Header{
				{Key: "trace-id", Value: []byte{0x00, 0xff}},
				{Key: "trace-id", Value: []byte("second")},
			},
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
	assert.Equal(t, original.Metadata.Headers, encrypted.Metadata.Headers)

	// Payload should be replaced by encrypted metadata only
	assert.Nil(t, encrypted.Metadata.Payload.GetData())
	assert.Contains(t, encrypted.Metadata.Payload.GetMetadata(), "encryptedPayload")
	assert.Contains(t, encrypted.Metadata.Payload.GetMetadata(), "nonce")
	assert.Contains(t, encrypted.Metadata.Payload.GetMetadata(), "encryptionKeyId")

	require.NoError(t, DecryptMessagePayload(encrypted, keyManager))
	assert.Equal(t, original.Metadata.Headers, encrypted.Metadata.Headers)

	decryptedPayload := encrypted.GetMetadata().GetPayload()
	require.NotNil(t, decryptedPayload)
	assert.Equal(t, "user-123", decryptedPayload.Metadata["user_id"].GetStringValue())
	assert.Equal(t, "demo-task", decryptedPayload.Metadata["task"].GetStringValue())
	require.NotNil(t, decryptedPayload.Data)
	assert.Equal(t, "order-456", decryptedPayload.Data.Fields["order_id"].GetStringValue())
}

func TestEncryptDecryptMessagePayload_AcrossKeyRotation(t *testing.T) {
	oldManager := newTestKeyManagerWithKeys(t, "0123456789abcdef", nil)
	oldMessage := &messagepb.Message{
		MessageId: "before-rotation",
		Metadata: &messagepb.Message_Metadata{
			Payload: buildTestPayload(t, "old-key", "demo-task"),
		},
	}
	require.NoError(t, EncryptMessagePayload(oldMessage, oldManager))
	oldKeyID := oldMessage.GetMetadata().GetPayload().GetMetadata()["encryptionKeyId"].GetStringValue()
	require.NotEmpty(t, oldKeyID)

	newManager := newTestKeyManagerWithKeys(t, "abcdef0123456789", []string{"0123456789abcdef"})
	require.NoError(t, DecryptMessagePayload(oldMessage, newManager))
	assert.Equal(t, "old-key", oldMessage.GetMetadata().GetPayload().GetMetadata()["user_id"].GetStringValue())

	newMessage := &messagepb.Message{
		MessageId: "after-rotation",
		Metadata: &messagepb.Message_Metadata{
			Payload: buildTestPayload(t, "new-key", "demo-task"),
		},
	}
	require.NoError(t, EncryptMessagePayload(newMessage, newManager))
	newKeyID := newMessage.GetMetadata().GetPayload().GetMetadata()["encryptionKeyId"].GetStringValue()
	assert.NotEqual(t, oldKeyID, newKeyID)
	require.NoError(t, DecryptMessagePayload(newMessage, oldManager))
	assert.Equal(t, "new-key", newMessage.GetMetadata().GetPayload().GetMetadata()["user_id"].GetStringValue())
}

func TestDecryptMessagePayload_LegacyEnvelopeUsesHistoricalKeys(t *testing.T) {
	oldManager := newTestKeyManagerWithKeys(t, "0123456789abcdef", nil)
	message := &messagepb.Message{
		Metadata: &messagepb.Message_Metadata{Payload: buildTestPayload(t, "legacy", "demo-task")},
	}
	require.NoError(t, EncryptMessagePayload(message, oldManager))
	delete(message.GetMetadata().GetPayload().GetMetadata(), "encryptionKeyId")

	newManager := newTestKeyManagerWithKeys(t, "abcdef0123456789", []string{"0123456789abcdef"})
	require.NoError(t, DecryptMessagePayload(message, newManager))
	assert.Equal(t, "legacy", message.GetMetadata().GetPayload().GetMetadata()["user_id"].GetStringValue())
}

func TestDecryptMessagePayload_ReportsMissingHistoricalKey(t *testing.T) {
	oldManager := newTestKeyManagerWithKeys(t, "0123456789abcdef", nil)
	message := &messagepb.Message{
		Metadata: &messagepb.Message_Metadata{Payload: buildTestPayload(t, "missing", "demo-task")},
	}
	require.NoError(t, EncryptMessagePayload(message, oldManager))
	keyID := message.GetMetadata().GetPayload().GetMetadata()["encryptionKeyId"].GetStringValue()

	newManager := newTestKeyManagerWithKeys(t, "abcdef0123456789", nil)
	err := DecryptMessagePayload(message, newManager)
	require.Error(t, err)
	assert.Contains(t, err.Error(), keyID)
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
	keyManager := newTestKeyManagerWithKeys(t, "0123456789abcdef", nil)

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

	keyManager = newTestKeyManagerWithKeys(t, "abcdef0123456789", []string{"0123456789abcdef"})
	require.NoError(t, DecryptSchedulePayload(schedule, keyManager))
	payload := schedule.GetMetadata().GetPayload()
	require.NotNil(t, payload)
	assert.Equal(t, "tenant-1", payload.Metadata["user_id"].GetStringValue())
	assert.Equal(t, "schedule-task", payload.Metadata["task"].GetStringValue())
	assert.Equal(t, "order-456", payload.Data.Fields["order_id"].GetStringValue())
}

func newTestKeyManager(t *testing.T) *keymanager.EncryptionKeyManager {
	t.Helper()
	return newTestKeyManagerWithKeys(t, "0123456789abcdef", nil)
}

func newTestKeyManagerWithKeys(t *testing.T, current string, previous []string) *keymanager.EncryptionKeyManager {
	t.Helper()

	t.Setenv("ENABLE_ENCRYPTION", "true")
	t.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	t.Setenv("ENCRYPTION_KEY", current)
	if previous == nil {
		t.Setenv("ENCRYPTION_PREVIOUS_KEYS", "")
	} else {
		previousJSON, err := json.Marshal(previous)
		require.NoError(t, err)
		t.Setenv("ENCRYPTION_PREVIOUS_KEYS", string(previousJSON))
	}
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
