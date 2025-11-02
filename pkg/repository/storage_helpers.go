package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
)

type ChronoHandlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

func ErrorHandler(defaultResp interface{}, msg string) ChronoHandlerFunc {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		resp, err := defaultResp, errors.New(msg)
		log.Println(msg, err)
		return resp, err
	}
}

func (as *storage) decryptMessageMetadataPayload(metadata *message_pb.Message_Metadata) error {
	// Fetch the base64-encoded values from metadata
	base64EncryptedPayload := metadata.Payload.Metadata["encryptedPayload"].GetStringValue()
	base64Nonce := metadata.Payload.Metadata["nonce"].GetStringValue()

	// Handle nil encryption key manager (used in testing)
	if as.encryptionKeyManager == nil {
		if base64EncryptedPayload != "" || base64Nonce != "" {
			return errors.New("encrypted payload found but encryption not configured")
		}
		return nil
	}

	if !as.encryptionKeyManager.Enabled {
		if base64EncryptedPayload != "" || base64Nonce != "" {
			return errors.New("encrypted payload found but encryption not enabled")
		}
		return nil
	}
	if base64EncryptedPayload == "" || base64Nonce == "" {
		return errors.New("encryptedPayload or nonce not found in metadata")
	}

	decryptedPayloadBytes, err := encryption.DecryptPayload(base64EncryptedPayload, base64Nonce, as.encryptionKeyManager)
	if err != nil {
		return err
	}

	err = protojson.Unmarshal([]byte(decryptedPayloadBytes), metadata.Payload)
	if err != nil {
		return err
	}
	return nil
}

// Fetches and deserializes the message metadata from Redis.
func (as *storage) fetchMessageMetadata(ctx context.Context, queueName string, messageID string) (*message_pb.Message_Metadata, error) {
	messageMutex := as.rs.NewMutex("mutex:" + messageID)
	// Try to acquire the lock
	if err := messageMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return nil, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := messageMutex.Unlock(); !ok || err != nil {
			as.logger.Error("Failed to release message lock", err)
		}
	}()

	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)
	result, err := as.redisClient.HGet(ctx, key, "metadata").Result()
	if err != nil {
		return nil, err
	}

	as.logger.DebugWithFields(
		"Fetched metadata for message:",
		"message Id", messageID,
		"metadata", result,
	)

	var meta message_pb.Message_Metadata
	err = protojson.Unmarshal([]byte(result), &meta)
	if err != nil {
		return nil, err
	}

	as.logger.DebugWithFields(
		"Successfully unmarshaled metadata for message:",
		"message Id", messageID,
		"metadata", meta.Payload,
	)

	err = as.decryptMessageMetadataPayload(&meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

func (as *storage) getMetadata(ctx context.Context, key string, metadata interface{}) error {
	fmt.Printf("Fetching metadata for key: %s\n", key)
	metaResult, err := as.redisClient.HGet(ctx, key, "metadata").Result()
	if err != nil {
		return err
	}
	metaReflect, ok := metadata.(protoreflect.ProtoMessage)
	if !ok {
		return errors.New("metadata does not implement ProtoMessage")
	}
	if err = protojson.Unmarshal([]byte(metaResult), metaReflect); err != nil {
		return err
	}
	return nil
}

func (as *storage) GetQueueMetadata(ctx context.Context, queueName string) (*queue_pb.QueueMetadata, error) {
	queueMetaKey := fmt.Sprintf("queue:%s:meta", queueName)
	var queueMeta queue_pb.QueueMetadata
	if err := as.getMetadata(ctx, queueMetaKey, &queueMeta); err != nil {
		return nil, err
	}
	return &queueMeta, nil
}

func (as *storage) getScheduleMetadata(ctx context.Context, scheduleId string) (*schedule_pb.Schedule_Metadata, error) {
	scheduleMetaKey := scheduleId
	if !strings.HasPrefix(scheduleId, "schedule:") || !strings.HasSuffix(scheduleId, ":meta") {
		scheduleMetaKey = fmt.Sprintf("schedule:%s:meta", scheduleId)
	}
	var scheduleMeta schedule_pb.Schedule_Metadata
	if err := as.getMetadata(ctx, scheduleMetaKey, &scheduleMeta); err != nil {
		return nil, err
	}
	return &scheduleMeta, nil
}

func (as *storage) updateMessageStateAndLease(message *message_pb.Message, request *queueservice_pb.GetNextMessageRequest, queueMeta *queue_pb.QueueMetadata) {
	message.Metadata.State = message_pb.Message_Metadata_RUNNING
	if message.Metadata.GetLeaseDuration().AsDuration() <= 0 {
		if request.LeaseDuration.AsDuration() > 0 {
			message.Metadata.LeaseDuration = request.GetLeaseDuration()
		} else {
			message.Metadata.LeaseDuration = queueMeta.LeaseDuration
		}
	} else if request.LeaseDuration.AsDuration() > 0 && request.LeaseDuration.AsDuration() > message.Metadata.GetLeaseDuration().AsDuration() {
		message.Metadata.LeaseDuration = request.GetLeaseDuration()
	}

	// Initialize retry counts if not set
	if message.Metadata.AttemptsLeft == 0 && message.Metadata.MaxAttempts == 0 {
		// Use queue default if message doesn't specify max attempts
		message.Metadata.MaxAttempts = queueMeta.GetDefaultMaxAttempts()
		message.Metadata.AttemptsLeft = message.Metadata.MaxAttempts
	} else if message.Metadata.AttemptsLeft == 0 && message.Metadata.MaxAttempts > 0 {
		// Message specified max_attempts but attempts_left wasn't initialized
		as.logger.InfoWithFields(
			"Initializing attempts left for message:",
			"message Id", message.GetMessageId(),
			"old attempts left", message.Metadata.AttemptsLeft,
			"max attempts", message.Metadata.MaxAttempts,
		)
		message.Metadata.AttemptsLeft = message.Metadata.MaxAttempts
	} else if message.Metadata.AttemptsLeft == 0 && message.Metadata.MaxAttempts == -1 {
		// Infinite retries: set attempts_left to -1 to indicate infinite
		message.Metadata.AttemptsLeft = -1
	}

	// Add lease expiry data to the message metadata
	expireDate := time.Now().Add(time.Duration(message.Metadata.GetLeaseDuration().AsDuration()))
	message.Metadata.LeaseExpiry = expireDate.UnixNano() / int64(time.Millisecond)
}

func (as *storage) listMetadataIDs(ctx context.Context, keyType string, prefix string, limit int64) ([]string, error) {
	var metadataIDs []string
	var cursor uint64
	var err error
	queryStr := ""
	switch keyType {
	case "queue":
		fmt.Println("Listing queue metadata IDs with prefix:", prefix)
		queryStr = "queue:" + prefix + "*" + ":meta"
	case "schedule":
		queryStr = "schedule:" + prefix + "*" + ":meta"
	default:
		return nil, errors.New("invalid key type: " + queryStr)
	}
	for {
		var keys []string
		keys, cursor, err = as.redisClient.Scan(ctx, cursor, queryStr, limit).Result()
		if err != nil {
			return nil, err
		}
		metadataIDs = append(metadataIDs, keys...)
		if cursor == 0 {
			break
		}
	}
	return metadataIDs, nil
}

// State index management helpers for sorted sets optimization

// addToStateIndex adds a message to the appropriate state-based sorted set
func (as *storage) addToStateIndex(ctx context.Context, pipeline redis.Pipeliner, messageKey string, state message_pb.Message_Metadata_State, score float64) error {
	var indexKey string
	switch state {
	case message_pb.Message_Metadata_INVISIBLE:
		indexKey = "invisible_messages"
	case message_pb.Message_Metadata_RUNNING:
		indexKey = "running_messages"
	case message_pb.Message_Metadata_PENDING:
		// PENDING messages don't need indexing as they're handled by queue priority
		return nil
	case message_pb.Message_Metadata_COMPLETED, message_pb.Message_Metadata_CANCELED, message_pb.Message_Metadata_ERRORED:
		// Terminal states don't need indexing
		return nil
	default:
		return nil
	}

	if pipeline != nil {
		_, err := pipeline.ZAdd(ctx, indexKey, redis.Z{
			Score:  score,
			Member: messageKey,
		}).Result()
		return err
	} else {
		_, err := as.redisClient.ZAdd(ctx, indexKey, redis.Z{
			Score:  score,
			Member: messageKey,
		}).Result()
		return err
	}
}

// removeFromStateIndex removes a message from the appropriate state-based sorted set
func (as *storage) removeFromStateIndex(ctx context.Context, pipeline redis.Pipeliner, messageKey string, state message_pb.Message_Metadata_State) error {
	var indexKey string
	switch state {
	case message_pb.Message_Metadata_INVISIBLE:
		indexKey = "invisible_messages"
	case message_pb.Message_Metadata_RUNNING:
		indexKey = "running_messages"
	case message_pb.Message_Metadata_PENDING:
		// PENDING messages don't need indexing
		return nil
	case message_pb.Message_Metadata_COMPLETED, message_pb.Message_Metadata_CANCELED, message_pb.Message_Metadata_ERRORED:
		// Terminal states don't need indexing
		return nil
	default:
		return nil
	}

	if pipeline != nil {
		_, err := pipeline.ZRem(ctx, indexKey, messageKey).Result()
		return err
	} else {
		_, err := as.redisClient.ZRem(ctx, indexKey, messageKey).Result()
		return err
	}
}

// transitionStateIndex handles state transitions by removing from old index and adding to new index
func (as *storage) transitionStateIndex(ctx context.Context, pipeline redis.Pipeliner, messageKey string, oldState, newState message_pb.Message_Metadata_State, newScore float64) error {
	// Remove from old state index (skip if oldState is -1, indicating no previous state)
	if oldState != message_pb.Message_Metadata_State(-1) {
		if err := as.removeFromStateIndex(ctx, pipeline, messageKey, oldState); err != nil {
			return fmt.Errorf("failed to remove from old state index: %w", err)
		}
	}

	// Add to new state index
	if err := as.addToStateIndex(ctx, pipeline, messageKey, newState, newScore); err != nil {
		return fmt.Errorf("failed to add to new state index: %w", err)
	}

	return nil
}

func (as *storage) saveMessageMetadata(ctx context.Context, queueName, messageID string, meta *message_pb.Message_Metadata) error {
	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)
	data, err := protojson.Marshal(meta)
	if err != nil {
		return err
	}

	return as.redisClient.HSet(ctx, key, "metadata", data).Err()
}

func (as *storage) generateConsumerName() string {
	return fmt.Sprintf("worker-%d", time.Now().UnixNano())
}

func (as *storage) listQueueNames(ctx context.Context) ([]string, error) {
	var queues []string
	seen := make(map[string]bool)

	iter := as.redisClient.Scan(ctx, 0, "queue:*:meta", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		parts := strings.Split(key, ":")
		if len(parts) >= 2 {
			queueName := parts[1]
			if !seen[queueName] {
				queues = append(queues, queueName)
				seen[queueName] = true
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return queues, nil
}
