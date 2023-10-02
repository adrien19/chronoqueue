package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
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

func (as *storage) getQueueMetadata(ctx context.Context, queueName string) (*chronoqueue.Queue_Options, error) {
	queueMetaKey := fmt.Sprintf("%s:meta", queueName)
	queueMetaResult, err := as.redisClient.HGet(ctx, queueMetaKey, "metadata").Result()
	if err != nil {
		return nil, err
	}
	var queueMeta chronoqueue.Queue_Options
	if err = protojson.Unmarshal([]byte(queueMetaResult), &queueMeta); err != nil {
		return nil, err
	}
	return &queueMeta, nil
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
	expireDate := time.Now().Add(time.Duration(message.Metadata.GetLeaseDuration().AsDuration())).Unix()
	message.Metadata.LeaseExpiry = &expireDate
}

func (as *storage) saveMessageWithMetadata(ctx context.Context, queueName string, message *chronoqueue.Message) error {
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
