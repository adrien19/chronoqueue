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

func newSQLiteRotationKeyManager(t *testing.T, logger *log.Logger, current string, previous []string) *keymanager.EncryptionKeyManager {
	t.Helper()
	t.Setenv("ENCRYPTION_KEY", current)
	previousJSON, err := json.Marshal(previous)
	require.NoError(t, err)
	t.Setenv("ENCRYPTION_PREVIOUS_KEYS", string(previousJSON))
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
