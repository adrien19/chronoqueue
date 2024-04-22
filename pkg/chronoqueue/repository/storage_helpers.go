package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ChronoHandlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

func ErrorHandler(defaultResp interface{}, msg string) ChronoHandlerFunc {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		resp, err := defaultResp, errors.New(msg)
		log.Println(msg, err)
		return resp, err
	}
}

func (as *storage) decryptMessageMetadataPayload(metadata *chronoqueue.Message_Metadata) error {
	// Fetch the base64-encoded values from metadata
	base64EncryptedPayload := metadata.Payload.Metadata["encryptedPayload"].GetStringValue()
	base64Nonce := metadata.Payload.Metadata["nonce"].GetStringValue()

	if !as.encryptionKeyManager.Enabled {
		if base64EncryptedPayload != "" || base64Nonce != "" {
			util.WarnWithFields("Metadata with no fields: ", map[string]interface{}{
				"base64EncryptedPayload": base64EncryptedPayload,
				"base64Nonce":            base64Nonce,
				"meta":                   metadata.Payload,
			})
			return errors.New("encrypted payload found but encryption not enabled")
		}
		return nil
	}
	if base64EncryptedPayload == "" || base64Nonce == "" {
		util.WarnWithFields("Metadata with no encrypted payload: ", map[string]interface{}{
			"base64EncryptedPayload": base64EncryptedPayload,
			"base64Nonce":            base64Nonce,
			"meta":                   metadata.Payload,
		})
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
func (as *storage) fetchMessageMetadata(ctx context.Context, queueName string, messageID string) (*chronoqueue.Message_Metadata, error) {

	messageMutex := as.rs.NewMutex("mutex:" + messageID)
	// Try to acquire the lock
	if err := messageMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return nil, chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := messageMutex.Unlock(); !ok || err != nil {
			util.Error("Failed to release message lock", err)
		}
	}()

	key := fmt.Sprintf("%s:%s:meta", queueName, messageID)
	result, err := as.redisClient.HGet(ctx, key, "metadata").Result()
	if err != nil {
		return nil, err
	}

	var meta chronoqueue.Message_Metadata
	err = protojson.Unmarshal([]byte(result), &meta)
	if err != nil {
		return nil, err
	}

	err = as.decryptMessageMetadataPayload(&meta)
	if err != nil {
		return nil, err
	}

	return &meta, nil
}

func (as *storage) getMetadata(ctx context.Context, key string, metadata interface{}) error {
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

func (as *storage) getQueueMetadata(ctx context.Context, queueName string) (*chronoqueue.Queue_Options, error) {
	queueMetaKey := fmt.Sprintf("%s:meta", queueName)
	var queueMeta chronoqueue.Queue_Options
	if err := as.getMetadata(ctx, queueMetaKey, &queueMeta); err != nil {
		return nil, err
	}
	return &queueMeta, nil
}

func (as *storage) getScheduleMetadata(ctx context.Context, scheduleId string) (*chronoqueue.Schedule_Metadata, error) {
	scheduleMetaKey := scheduleId
	if !strings.HasPrefix(scheduleId, "schedule:") || !strings.HasSuffix(scheduleId, ":meta") {
		scheduleMetaKey = fmt.Sprintf("schedule:%s:meta", scheduleId)
	}
	var scheduleMeta chronoqueue.Schedule_Metadata
	if err := as.getMetadata(ctx, scheduleMetaKey, &scheduleMeta); err != nil {
		return nil, err
	}
	return &scheduleMeta, nil
}

func (as *storage) updateMessageStateAndLease(message *chronoqueue.Message, request *chronoqueue.GetNextMessageRequest, queueMeta *chronoqueue.Queue_Options) {
	message.Metadata.State = chronoqueue.Message_Metadata_RUNNING
	if message.Metadata.GetLeaseDuration().AsDuration() <= 0 {
		if request.LeaseDuration.AsDuration() > 0 {
			message.Metadata.LeaseDuration = request.GetLeaseDuration()
		} else {
			message.Metadata.LeaseDuration = queueMeta.LeaseDuration
		}
	}

	// Add lease expiry data to the message metadata
	expireDate := time.Now().Add(time.Duration(message.Metadata.GetLeaseDuration().AsDuration()))
	message.Metadata.LeaseExpiry = expireDate.UnixNano() / int64(time.Millisecond)
}

func (as *storage) saveMessageWithMetadata(ctx context.Context, queueName string, message *chronoqueue.Message) error {

	messageMutex := as.rs.NewMutex("mutex:" + message.MessageId)
	// Try to acquire the lock
	if err := messageMutex.Lock(); err != nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Unexpected while acquiring lock")
		return chronoErr.GRPCStatus()
	}

	defer func() {
		// Release the message lock
		if ok, err := messageMutex.Unlock(); !ok || err != nil {
			util.Error("Failed to release message lock", err)
		}
	}()

	// Create a proto message's metadata marshaller
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}

	err := as.encryptMetadataPayload(message.Metadata)
	if err != nil {
		return err
	}

	messageMetadataByte, err := m.Marshal(message.Metadata)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s:meta", queueName, message.MessageId)
	return as.redisClient.HSet(ctx, key, "metadata", string(messageMetadataByte)).Err()
}

func (as *storage) fetchQueueMembersBeforeNow(ctx context.Context, queueName string) ([]string, error) {
	max := strconv.FormatInt(time.Now().UnixMilli(), 10)
	return as.redisClient.ZRangeByScore(ctx, queueName, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    max,
		Offset: 0,
		Count:  10,
	}).Result()
}

func (as *storage) listMetadataIDs(ctx context.Context, keyType string, prefix string, limit int64) ([]string, error) {
	var metadataIDs []string
	var cursor uint64
	var err error
	queryStr := ""
	if keyType == "queue" {
		queryStr = prefix + "*" + ":meta"
	} else if keyType == "schedule" {
		queryStr = "schedule:" + prefix + "*" + ":meta"
	} else {
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
