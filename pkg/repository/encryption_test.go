//go:build test_dep

package repository

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tc_redis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// TestEncryptDecryptMetadataPayload tests the encryption and decryption of message metadata payloads
func TestEncryptDecryptMetadataPayload(t *testing.T) {
	// Enable encryption for this test
	os.Setenv("ENABLE_ENCRYPTION", "true")
	os.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	os.Setenv("ENCRYPTION_KEY", "12345678901234567890123456789012")
	defer os.Unsetenv("ENABLE_ENCRYPTION")
	defer os.Unsetenv("ENCRYPTION_KEY_SOURCE_TYPE")
	defer os.Unsetenv("ENCRYPTION_KEY")

	logger := log.NewLogger()
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		t.Fatalf("Failed to create encryption key manager: %v", err)
	}

	// Create test metadata with payload
	payloadMetadata, _ := structpb.NewStruct(map[string]interface{}{
		"user_id":   "user-123",
		"action":    "process",
		"sensitive": "secret-data",
	})
	payloadData, _ := structpb.NewStruct(map[string]interface{}{
		"order_id": "order-456",
		"amount":   100.50,
	})

	originalMetadata := &message_pb.Message_Metadata{
		State:         message_pb.Message_Metadata_PENDING,
		Priority:      5,
		LeaseDuration: durationpb.New(300000000000), // 5 minutes
		MaxAttempts:   3,
		Payload: &common_pb.Payload{
			Metadata: payloadMetadata.Fields,
			Data:     payloadData,
		},
	}

	storage := &storage{
		logger:               logger,
		encryptionKeyManager: keyManager,
	}

	// Test encryption
	err = storage.encryptMetadataPayload(originalMetadata)
	if err != nil {
		t.Fatalf("encryptMetadataPayload() failed: %v", err)
	}

	// Verify payload was encrypted (original fields replaced with encryptedPayload/nonce in Metadata map)
	if originalMetadata.Payload.Metadata["encryptedPayload"] == nil {
		t.Error("After encryption, Payload.Metadata should contain 'encryptedPayload'")
	}
	if originalMetadata.Payload.Metadata["nonce"] == nil {
		t.Error("After encryption, Payload.Metadata should contain 'nonce'")
	}
	if originalMetadata.Payload.Metadata["encryptedPayload"].GetStringValue() == "" {
		t.Error("encryptedPayload should not be empty")
	}
	if originalMetadata.Payload.Metadata["nonce"].GetStringValue() == "" {
		t.Error("nonce should not be empty")
	}

	// Verify only encryptedPayload and nonce remain in Metadata
	if len(originalMetadata.Payload.Metadata) != 2 {
		t.Errorf("After encryption, Payload.Metadata should only have 2 keys (encryptedPayload, nonce), got: %d", len(originalMetadata.Payload.Metadata))
	}

	// Test decryption
	err = storage.decryptMessageMetadataPayload(originalMetadata)
	if err != nil {
		t.Fatalf("decryptMessageMetadataPayload() failed: %v", err)
	}

	// Verify payload was decrypted correctly
	if originalMetadata.Payload.Metadata == nil {
		t.Fatal("After decryption, Payload.Metadata should not be nil")
	}
	if originalMetadata.Payload.Metadata["user_id"].GetStringValue() != "user-123" {
		t.Errorf("After decryption, expected user_id=user-123, got: %v", originalMetadata.Payload.Metadata["user_id"].GetStringValue())
	}
	if originalMetadata.Payload.Metadata["sensitive"].GetStringValue() != "secret-data" {
		t.Errorf("After decryption, expected sensitive=secret-data, got: %v", originalMetadata.Payload.Metadata["sensitive"].GetStringValue())
	}
	if originalMetadata.Payload.Data == nil {
		t.Error("After decryption, Payload.Data should not be nil")
	}
	if originalMetadata.Payload.Data.Fields["order_id"].GetStringValue() != "order-456" {
		t.Errorf("After decryption, expected order_id=order-456, got: %v", originalMetadata.Payload.Data.Fields["order_id"].GetStringValue())
	}
}

// TestEncryptionDisabled tests that encryption/decryption is skipped when disabled
func TestEncryptionDisabled(t *testing.T) {
	// Ensure encryption is disabled
	os.Setenv("ENABLE_ENCRYPTION", "false")
	defer os.Unsetenv("ENABLE_ENCRYPTION")

	logger := log.NewLogger()
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		t.Fatalf("Failed to create encryption key manager: %v", err)
	}

	if keyManager.Enabled {
		t.Fatal("Expected encryption to be disabled")
	}

	payloadMetadata, _ := structpb.NewStruct(map[string]interface{}{
		"key": "value",
	})
	payloadData, _ := structpb.NewStruct(map[string]interface{}{
		"test": "data",
	})

	originalMetadata := &message_pb.Message_Metadata{
		State: message_pb.Message_Metadata_PENDING,
		Payload: &common_pb.Payload{
			Metadata: payloadMetadata.Fields,
			Data:     payloadData,
		},
	}

	storage := &storage{
		logger:               logger,
		encryptionKeyManager: keyManager,
	}

	// Test encryption does nothing when disabled
	err = storage.encryptMetadataPayload(originalMetadata)
	if err != nil {
		t.Fatalf("encryptMetadataPayload() failed: %v", err)
	}

	// Verify payload was NOT encrypted
	if originalMetadata.Payload.Metadata["key"].GetStringValue() != "value" {
		t.Error("When encryption disabled, Payload.Metadata should remain unchanged")
	}
	if originalMetadata.Payload.Data.Fields["test"].GetStringValue() != "data" {
		t.Error("When encryption disabled, Payload.Data should remain unchanged")
	}
	if originalMetadata.Payload.Metadata["encryptedPayload"] != nil {
		t.Error("When encryption disabled, encryptedPayload should not be set")
	}
}

// TestSaveMessageMetadataWithEncryption tests that saveMessageMetadata properly encrypts before saving
// and doesn't modify the original metadata object (via proto.Clone)
func TestSaveMessageMetadataWithEncryption(t *testing.T) {
	// Enable encryption
	os.Setenv("ENABLE_ENCRYPTION", "true")
	os.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	os.Setenv("ENCRYPTION_KEY", "12345678901234567890123456789012")
	defer os.Unsetenv("ENABLE_ENCRYPTION")
	defer os.Unsetenv("ENCRYPTION_KEY_SOURCE_TYPE")
	defer os.Unsetenv("ENCRYPTION_KEY")

	// Setup Redis container
	ctx := context.Background()
	redisContainer, err := tc_redis.Run(ctx, "redis:7.2.4")
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Redis container: %v", err)
		}
	}()

	endpoint, err := redisContainer.Endpoint(ctx, "redis")
	if err != nil {
		t.Fatalf("Failed to get Redis endpoint: %v", err)
	}
	endpoint = strings.TrimPrefix(endpoint, "redis://")
	redisClient := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})
	defer redisClient.Close()

	logger := log.NewLogger()
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		t.Fatalf("Failed to create encryption key manager: %v", err)
	}

	// Use full storage with scheduler and reclaim service (100ms intervals for testing)
	stor := NewQueueStorageWithIntervals(ctx, redisClient, keyManager, logger, 100*time.Millisecond, 5*time.Second)

	// Create test metadata with decrypted payload
	payloadMetadata, _ := structpb.NewStruct(map[string]interface{}{
		"user_id": "user-789",
		"action":  "execute",
	})
	payloadData, _ := structpb.NewStruct(map[string]interface{}{
		"task":  "important",
		"value": 42.0,
	})

	metadata := &message_pb.Message_Metadata{
		State:         message_pb.Message_Metadata_RUNNING,
		Priority:      10,
		LeaseDuration: durationpb.New(300000000000),
		MaxAttempts:   5,
		Payload: &common_pb.Payload{
			Metadata: payloadMetadata.Fields,
			Data:     payloadData,
		},
	}

	// Save metadata (should encrypt before saving)
	storageImpl := stor.(*storage)
	err = storageImpl.saveMessageMetadata(ctx, "test-queue", "test-msg-456", metadata)
	if err != nil {
		t.Fatalf("saveMessageMetadata() failed: %v", err)
	}

	// CRITICAL TEST: Verify original metadata is still decrypted (not modified in-place)
	if metadata.Payload.Metadata["user_id"] == nil {
		t.Error("Original metadata should remain decrypted after save (proto.Clone should protect it)")
	}
	if metadata.Payload.Metadata["user_id"].GetStringValue() != "user-789" {
		t.Errorf("Original metadata fields should be intact, got: %v", metadata.Payload.Metadata["user_id"].GetStringValue())
	}
	if metadata.Payload.Data == nil || metadata.Payload.Data.Fields["task"].GetStringValue() != "important" {
		t.Error("Original Payload.Data should remain intact")
	}
	if metadata.Payload.Metadata["encryptedPayload"] != nil {
		t.Error("Original metadata should not have encryptedPayload set (should be on clone only)")
	}

	// Verify Redis storage contains ENCRYPTED data
	key := "chronoqueue:test-queue:msg:test-msg-456:meta"
	storedData, err := redisClient.HGet(ctx, key, "metadata").Result()
	if err != nil {
		t.Fatalf("Failed to get stored metadata from Redis: %v", err)
	}

	var storedMetadata message_pb.Message_Metadata
	if err := protojson.Unmarshal([]byte(storedData), &storedMetadata); err != nil {
		t.Fatalf("Failed to unmarshal stored metadata: %v", err)
	}

	// Verify stored version is encrypted
	if storedMetadata.Payload.Metadata["encryptedPayload"] == nil {
		t.Error("Stored metadata should have encryptedPayload in Metadata map")
	}
	if storedMetadata.Payload.Metadata["nonce"] == nil {
		t.Error("Stored metadata should have nonce in Metadata map")
	}
	if storedMetadata.Payload.Metadata["user_id"] != nil {
		t.Error("Stored metadata should NOT have decrypted fields like user_id")
	}
	if len(storedMetadata.Payload.Metadata) != 2 {
		t.Errorf("Stored metadata should only have 2 fields (encryptedPayload, nonce), got: %d", len(storedMetadata.Payload.Metadata))
	}
}

// TestMessageLifecycleWithEncryption tests the full message lifecycle with encryption enabled
func TestMessageLifecycleWithEncryption(t *testing.T) {
	// Enable encryption
	os.Setenv("ENABLE_ENCRYPTION", "true")
	os.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	os.Setenv("ENCRYPTION_KEY", "12345678901234567890123456789012")
	defer os.Unsetenv("ENABLE_ENCRYPTION")
	defer os.Unsetenv("ENCRYPTION_KEY_SOURCE_TYPE")
	defer os.Unsetenv("ENCRYPTION_KEY")

	// Setup Redis container
	ctx := context.Background()
	redisContainer, err := tc_redis.Run(ctx, "redis:7.2.4")
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Errorf("Failed to terminate Redis container: %v", err)
		}
	}()

	endpoint, err := redisContainer.Endpoint(ctx, "redis")
	if err != nil {
		t.Fatalf("Failed to get Redis endpoint: %v", err)
	}
	endpoint = strings.TrimPrefix(endpoint, "redis://")
	redisClient := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})
	defer redisClient.Close()

	logger := log.NewLogger()
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		t.Fatalf("Failed to create encryption key manager: %v", err)
	}

	// Use full storage with scheduler enabled (100ms interval for fast testing)
	storage := NewQueueStorageWithIntervals(ctx, redisClient, keyManager, logger, 100*time.Millisecond, 5*time.Second)

	// Step 1: Create a queue
	createQueueReq := &queueservice_pb.CreateQueueRequest{
		Name: "encrypted-queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}
	_, err = storage.CreateQueue(ctx, createQueueReq)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// Step 2: Post a message with sensitive data
	msgMetadata, _ := structpb.NewStruct(map[string]interface{}{
		"ssn":            "123-45-6789",
		"credit_card":    "4111111111111111",
		"user_email":     "user@example.com",
		"internal_notes": "sensitive info",
	})
	msgData, _ := structpb.NewStruct(map[string]interface{}{
		"account_number": "ACC-999",
		"balance":        50000.00,
	})

	postMsgReq := &queueservice_pb.PostMessageRequest{
		QueueName: "encrypted-queue",
		Message: &message_pb.Message{
			MessageId: "encrypted-msg-001",
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Metadata: msgMetadata.Fields,
					Data:     msgData,
				},
			},
		},
	}
	_, err = storage.CreateQueueMessage(ctx, postMsgReq, nil)
	if err != nil {
		t.Fatalf("Failed to create message: %v", err)
	}

	// Give the scheduler time to move message from schedule to PENDING stream
	// Scheduler runs every 100ms in this test
	time.Sleep(300 * time.Millisecond)

	// Step 3: Verify message is encrypted in Redis
	metaKey := "chronoqueue:encrypted-queue:msg:encrypted-msg-001:meta"
	storedData, err := redisClient.HGet(ctx, metaKey, "metadata").Result()
	if err != nil {
		t.Fatalf("Failed to get metadata from Redis: %v", err)
	}

	var storedMetadata message_pb.Message_Metadata
	if err := protojson.Unmarshal([]byte(storedData), &storedMetadata); err != nil {
		t.Fatalf("Failed to unmarshal stored metadata: %v", err)
	}

	// Verify stored data is encrypted
	if storedMetadata.Payload.Metadata["encryptedPayload"] == nil {
		t.Error("Stored message should have encryptedPayload")
	}
	if storedMetadata.Payload.Metadata["nonce"] == nil {
		t.Error("Stored message should have nonce")
	}
	if storedMetadata.Payload.Metadata["ssn"] != nil {
		t.Error("Stored message should NOT have decrypted ssn field")
	}

	// Step 4: Get next message (PENDING -> RUNNING transition)
	getNextReq := &queueservice_pb.GetNextMessageRequest{
		QueueName:     "encrypted-queue",
		LeaseDuration: durationpb.New(60000000000), // 60 seconds
	}
	getNextResp, err := storage.GetQueueMessage(ctx, getNextReq)
	if err != nil {
		t.Fatalf("Failed to get next message: %v", err)
	}

	// Verify returned message is decrypted
	if getNextResp.Message == nil {
		t.Fatal("Expected to receive a message")
	}
	if getNextResp.Message.Metadata == nil || getNextResp.Message.Metadata.Payload == nil {
		t.Fatal("Expected message metadata to be decrypted")
	}
	if getNextResp.Message.Metadata.Payload.Metadata["ssn"].GetStringValue() != "123-45-6789" {
		t.Errorf("Expected decrypted ssn, got: %v", getNextResp.Message.Metadata.Payload.Metadata["ssn"].GetStringValue())
	}
	if getNextResp.Message.Metadata.Payload.Metadata["credit_card"].GetStringValue() != "4111111111111111" {
		t.Errorf("Expected decrypted credit_card, got: %v", getNextResp.Message.Metadata.Payload.Metadata["credit_card"].GetStringValue())
	}

	// Verify payload data is decrypted
	if getNextResp.Message.Metadata.Payload.Data == nil {
		t.Fatal("Expected payload data to be decrypted")
	}
	if getNextResp.Message.Metadata.Payload.Data.Fields["account_number"].GetStringValue() != "ACC-999" {
		t.Errorf("Expected decrypted account_number, got: %v", getNextResp.Message.Metadata.Payload.Data.Fields["account_number"].GetStringValue())
	}

	// Step 5: Verify message STILL encrypted in Redis after state transition
	storedDataAfter, err := redisClient.HGet(ctx, metaKey, "metadata").Result()
	if err != nil {
		t.Fatalf("Failed to get metadata from Redis after GetNext: %v", err)
	}

	var storedMetadataAfter message_pb.Message_Metadata
	if err := protojson.Unmarshal([]byte(storedDataAfter), &storedMetadataAfter); err != nil {
		t.Fatalf("Failed to unmarshal stored metadata after GetNext: %v", err)
	}

	// CRITICAL: Verify encryption persists after state change
	if storedMetadataAfter.Payload.Metadata["encryptedPayload"] == nil {
		t.Error("After state transition, stored message should still have encryptedPayload")
	}
	if storedMetadataAfter.Payload.Metadata["nonce"] == nil {
		t.Error("After state transition, stored message should still have nonce")
	}
	if storedMetadataAfter.Payload.Metadata["ssn"] != nil {
		t.Error("After state transition, stored message should NOT have decrypted ssn")
	}
	if storedMetadataAfter.State != message_pb.Message_Metadata_RUNNING {
		t.Errorf("Expected state to be RUNNING, got: %v", storedMetadataAfter.State)
	}

	// Step 6: Renew message lease (another state modification)
	renewReq := &queueservice_pb.RenewMessageLeaseRequest{
		QueueName:     "encrypted-queue",
		MessageId:     "encrypted-msg-001",
		LeaseDuration: durationpb.New(120000000000), // 120 seconds
	}
	_, err = storage.RenewMessageLease(ctx, renewReq)
	if err != nil {
		t.Fatalf("Failed to renew message lease: %v", err)
	}

	// Step 7: Verify STILL encrypted after lease renewal
	storedDataAfterRenew, err := redisClient.HGet(ctx, metaKey, "metadata").Result()
	if err != nil {
		t.Fatalf("Failed to get metadata from Redis after renew: %v", err)
	}

	var storedMetadataAfterRenew message_pb.Message_Metadata
	if err := protojson.Unmarshal([]byte(storedDataAfterRenew), &storedMetadataAfterRenew); err != nil {
		t.Fatalf("Failed to unmarshal stored metadata after renew: %v", err)
	}

	if storedMetadataAfterRenew.Payload.Metadata["encryptedPayload"] == nil {
		t.Error("After lease renewal, message should still have encryptedPayload")
	}
	if storedMetadataAfterRenew.Payload.Metadata["ssn"] != nil {
		t.Error("After lease renewal, sensitive metadata should still be encrypted")
	}
}

// TestDecryptionErrorHandling tests behavior when decryption fails
func TestDecryptionErrorHandling(t *testing.T) {
	// Enable encryption
	os.Setenv("ENABLE_ENCRYPTION", "true")
	os.Setenv("ENCRYPTION_KEY_SOURCE_TYPE", "LOCAL")
	os.Setenv("ENCRYPTION_KEY", "12345678901234567890123456789012")
	defer os.Unsetenv("ENABLE_ENCRYPTION")
	defer os.Unsetenv("ENCRYPTION_KEY_SOURCE_TYPE")
	defer os.Unsetenv("ENCRYPTION_KEY")

	logger := log.NewLogger()
	keyManager, err := keymanager.NewEncryptionKeyManager(logger)
	if err != nil {
		t.Fatalf("Failed to create encryption key manager: %v", err)
	}

	storage := &storage{
		logger:               logger,
		encryptionKeyManager: keyManager,
	}

	// Create metadata with invalid encrypted data
	metadata := &message_pb.Message_Metadata{
		State: message_pb.Message_Metadata_PENDING,
		Payload: &common_pb.Payload{
			Metadata: map[string]*structpb.Value{
				"encryptedPayload": structpb.NewStringValue("invalid-encrypted-data"),
				"nonce":            structpb.NewStringValue("invalid-nonce"),
			},
		},
	}

	// Attempt decryption - should fail gracefully
	err = storage.decryptMessageMetadataPayload(metadata)
	if err == nil {
		t.Error("Expected decryption to fail with invalid data")
	}
}
