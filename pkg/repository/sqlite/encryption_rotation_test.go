//go:build sqlite && cgo

package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestEncryptionKeyRotation_PreservesSQLiteMessagesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "encryption-rotation.db")
	logger := log.NewLogger()

	oldManager := newSQLiteRotationKeyManager(t, logger, "0123456789abcdef", nil)
	oldStorage, err := NewStorage(ctx, &Config{Path: path, Logger: logger, KeyManager: oldManager})
	require.NoError(t, err)
	queueName := "encrypted-rotation"
	require.NoError(t, oldStorage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, oldStorage.EnqueueMessage(ctx, queueName, sqliteRotationMessage("old", "before")))
	require.NoError(t, oldStorage.Close())

	newManager := newSQLiteRotationKeyManager(t, logger, "abcdef0123456789", []string{"0123456789abcdef"})
	newStorage, err := NewStorage(ctx, &Config{Path: path, Logger: logger, KeyManager: newManager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, newStorage.Close()) })
	require.NoError(t, newStorage.EnqueueMessage(ctx, queueName, sqliteRotationMessage("new", "after")))

	oldMessage, err := newStorage.ClaimMessage(ctx, queueName, "worker-old", "attempt-old", "")
	require.NoError(t, err)
	require.NotNil(t, oldMessage)
	assert.Equal(t, "before", oldMessage.GetMetadata().GetPayload().GetMetadata()["version"].GetStringValue())
	require.NoError(t, newStorage.AcknowledgeMessage(ctx, queueName, oldMessage.GetMessageId(), "attempt-old", "worker-old"))

	newMessage, err := newStorage.ClaimMessage(ctx, queueName, "worker-new", "attempt-new", "")
	require.NoError(t, err)
	require.NotNil(t, newMessage)
	assert.Equal(t, "after", newMessage.GetMetadata().GetPayload().GetMetadata()["version"].GetStringValue())
}

func TestEncryptionKeyRotation_SQLiteRestartWithoutHistoricalKeyFailsClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "encryption-missing-key.db")
	logger := log.NewLogger()

	oldManager := newSQLiteRotationKeyManager(t, logger, "0123456789abcdef", nil)
	oldStorage, err := NewStorage(ctx, &Config{Path: path, Logger: logger, KeyManager: oldManager})
	require.NoError(t, err)
	queueName := "encrypted-missing-key"
	require.NoError(t, oldStorage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{}}))
	require.NoError(t, oldStorage.EnqueueMessage(ctx, queueName, sqliteRotationMessage("old", "before")))
	require.NoError(t, oldStorage.Close())

	currentManager := newSQLiteRotationKeyManager(t, logger, "abcdef0123456789", nil)
	currentStorage, err := NewStorage(ctx, &Config{Path: path, Logger: logger, KeyManager: currentManager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, currentStorage.Close()) })

	message, err := currentStorage.ClaimMessage(ctx, queueName, "worker", "attempt", "")
	require.ErrorContains(t, err, "decrypt message payload")
	require.ErrorContains(t, err, "is not available")
	assert.Nil(t, message)
}

func TestGetDLQMessages_DecryptsPayload(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	manager := newSQLiteRotationKeyManager(t, logger, "0123456789abcdef", nil)
	storage, err := NewStorage(ctx, &Config{Path: filepath.Join(t.TempDir(), "encrypted-dlq.db"), Logger: logger, KeyManager: manager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queueName := "encrypted-dlq"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
		MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_FOREVER},
	}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, sqliteRotationMessage("failed", "plaintext")))
	_, err = storage.ClaimMessage(ctx, queueName, "worker", "attempt", "")
	require.NoError(t, err)
	require.NoError(t, storage.NackMessage(ctx, queueName, "failed", "attempt", "worker"))

	messages, err := storage.GetDLQMessages(ctx, queueName, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "plaintext", messages[0].GetMetadata().GetPayload().GetMetadata()["version"].GetStringValue())
}

func TestGetDLQMessages_ReturnsPayloadDecryptionError(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()
	manager := newSQLiteRotationKeyManager(t, logger, "0123456789abcdef", nil)
	storage, err := NewStorage(ctx, &Config{Path: filepath.Join(t.TempDir(), "corrupt-dlq.db"), Logger: logger, KeyManager: manager})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	queueName := "corrupt-dlq"
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: queueName, Metadata: &queuepb.QueueMetadata{
		MessageRetentionPolicy: &queuepb.MessageRetentionPolicy{Mode: queuepb.MessageRetentionPolicy_RETAIN_FOREVER},
	}}))
	require.NoError(t, storage.EnqueueMessage(ctx, queueName, sqliteRotationMessage("failed", "plaintext")))
	_, err = storage.ClaimMessage(ctx, queueName, "worker", "attempt", "")
	require.NoError(t, err)
	require.NoError(t, storage.NackMessage(ctx, queueName, "failed", "attempt", "worker"))

	var messageBytes []byte
	require.NoError(t, storage.DB.QueryRowContext(ctx, `SELECT metadata_pb FROM cq_messages WHERE queue_name = ? AND message_id = ?`, queueName, "failed").Scan(&messageBytes))
	message, err := storage.Serializer.UnmarshalMessage(messageBytes)
	require.NoError(t, err)
	message.Metadata.Payload.Metadata["encryptedPayload"] = structpb.NewStringValue("not-base64")
	messageBytes, err = storage.Serializer.MarshalMessage(message)
	require.NoError(t, err)
	_, err = storage.DB.ExecContext(ctx, `UPDATE cq_messages SET metadata_pb = ? WHERE queue_name = ? AND message_id = ?`, messageBytes, queueName, "failed")
	require.NoError(t, err)

	messages, err := storage.GetDLQMessages(ctx, queueName, 1)
	require.ErrorContains(t, err, "decrypt message payload")
	require.Nil(t, messages)
}

func newSQLiteRotationKeyManager(t *testing.T, logger *log.Logger, current string, previous []string) *keymanager.EncryptionKeyManager {
	t.Helper()
	t.Setenv("ENCRYPTION_KEY", current)
	t.Setenv("ENCRYPTION_PREVIOUS_KEYS", "")
	if previous != nil {
		previousJSON, err := json.Marshal(previous)
		require.NoError(t, err)
		t.Setenv("ENCRYPTION_PREVIOUS_KEYS", string(previousJSON))
	}
	manager, err := keymanager.NewEncryptionKeyManagerWithConfig(logger, keymanager.Config{Enabled: true, SourceType: "LOCAL"})
	require.NoError(t, err)
	return manager
}

func sqliteRotationMessage(id, version string) *messagepb.Message {
	return &messagepb.Message{
		MessageId: id,
		Metadata: &messagepb.Message_Metadata{
			State:        messagepb.Message_Metadata_PENDING,
			AttemptsLeft: 1,
			MaxAttempts:  1,
			LeasePolicy:  &commonpb.LeasePolicy{BaseLease: durationpb.New(time.Minute)},
			Payload: &commonpb.Payload{Metadata: map[string]*structpb.Value{
				"version": structpb.NewStringValue(version),
			}},
		},
	}
}
