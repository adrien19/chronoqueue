package common

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
)

var (
	marshalOptions   = protojson.MarshalOptions{EmitUnpopulated: true}
	unmarshalOptions = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// EncryptMessagePayload encrypts the payload on a message metadata in place.
func EncryptMessagePayload(message *messagepb.Message, keyManager *keymanager.EncryptionKeyManager) error {
	if message == nil || message.Metadata == nil {
		return nil
	}

	encryptedPayload, err := encryptPayload(message.Metadata.Payload, keyManager)
	if err != nil {
		return err
	}

	message.Metadata.Payload = encryptedPayload
	return nil
}

// DecryptMessagePayload decrypts the payload on a message metadata in place.
func DecryptMessagePayload(message *messagepb.Message, keyManager *keymanager.EncryptionKeyManager) error {
	if message == nil || message.Metadata == nil {
		return nil
	}

	decryptedPayload, err := decryptPayload(message.Metadata.Payload, keyManager)
	if err != nil {
		return err
	}

	message.Metadata.Payload = decryptedPayload
	return nil
}

// EncryptSchedulePayload encrypts the payload on a schedule metadata in place.
func EncryptSchedulePayload(schedule *schedulepb.Schedule, keyManager *keymanager.EncryptionKeyManager) error {
	if schedule == nil || schedule.Metadata == nil {
		return nil
	}

	encryptedPayload, err := encryptPayload(schedule.Metadata.Payload, keyManager)
	if err != nil {
		return err
	}

	schedule.Metadata.Payload = encryptedPayload
	return nil
}

// DecryptSchedulePayload decrypts the payload on a schedule metadata in place.
func DecryptSchedulePayload(schedule *schedulepb.Schedule, keyManager *keymanager.EncryptionKeyManager) error {
	if schedule == nil || schedule.Metadata == nil {
		return nil
	}

	decryptedPayload, err := decryptPayload(schedule.Metadata.Payload, keyManager)
	if err != nil {
		return err
	}

	schedule.Metadata.Payload = decryptedPayload
	return nil
}

func encryptPayload(payload *commonpb.Payload, keyManager *keymanager.EncryptionKeyManager) (*commonpb.Payload, error) {
	if payload == nil {
		return payload, nil
	}

	if keyManager == nil || !keyManager.Enabled {
		return payload, nil
	}

	payloadBytes, err := marshalOptions.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload for encryption: %w", err)
	}

	encryptedPayload, nonce, err := encryption.EncryptPayload(payloadBytes, keyManager)
	if err != nil {
		return nil, fmt.Errorf("encrypt payload: %w", err)
	}

	if encryptedPayload == "" || nonce == "" {
		return nil, fmt.Errorf("encrypt payload: empty ciphertext or nonce")
	}

	return &commonpb.Payload{
		Metadata: map[string]*structpb.Value{
			"encryptedPayload": structpb.NewStringValue(encryptedPayload),
			"nonce":            structpb.NewStringValue(nonce),
		},
	}, nil
}

func decryptPayload(payload *commonpb.Payload, keyManager *keymanager.EncryptionKeyManager) (*commonpb.Payload, error) {
	if payload == nil {
		return payload, nil
	}

	metadata := payload.GetMetadata()
	encryptedValue, hasEncrypted := metadata["encryptedPayload"]
	nonceValue, hasNonce := metadata["nonce"]

	if !hasEncrypted || !hasNonce {
		return payload, nil
	}

	encryptedPayload := encryptedValue.GetStringValue()
	nonce := nonceValue.GetStringValue()

	if encryptedPayload == "" || nonce == "" {
		return nil, fmt.Errorf("decrypt payload: missing ciphertext or nonce")
	}

	if keyManager == nil || !keyManager.Enabled {
		return nil, fmt.Errorf("decrypt payload: encryption key manager disabled")
	}

	decryptedBytes, err := encryption.DecryptPayload(encryptedPayload, nonce, keyManager)
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}

	var decrypted commonpb.Payload
	if err := unmarshalOptions.Unmarshal(decryptedBytes, &decrypted); err != nil {
		return nil, fmt.Errorf("unmarshal decrypted payload: %w", err)
	}

	return &decrypted, nil
}
