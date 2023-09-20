package client

import (
	"context"
	"encoding/json"
	"log"
	"time"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	State int32

	QueueOptions struct {
		DequeueAttempts      int32                `json:"dequeueAttempts,omitempty"`
		ExclusivityKey       string               `json:"exclusivityKey,omitempty"`
		InvisibilityDuration *durationpb.Duration `json:"invisibilityDuration,omitempty"`
		LeaseDuration        *durationpb.Duration `json:"leaseDuration,omitempty"`
		Type                 int32                `json:"type,omitempty"`
	}
	MessageOptions struct {
		// Payload              map[string]interface{} `json:"payload,omitempty"`
		Payload              Payload              `json:"payload,omitempty"`
		AttemptsLeft         int32                `json:"attemptsLeft,omitempty"`
		InvisibilityDuration *durationpb.Duration `json:"invisibilityDuration,omitempty"`
		LeaseDuration        *durationpb.Duration `json:"leaseDuration,omitempty"`
		LeaseExpiry          int64                `json:"leaseExpiry,omitempty"`
		State                State                `json:"state,omitempty"`
	}
	Payload struct {
		Metadata map[string]*structpb.Value `json:"metadata,omitempty"`
		Data     *structpb.Struct           `json:"data,omitempty"`
	}
	TimeRangeOption struct {
		Min int64 `json:"min,omitempty"`
		Max int64 `json:"max,omitempty"`
	}
)

const (
	STATE_UNDEFINED State = iota
	MESSAGE_INVISIBLE
	MESSAGE_PENDING
	MESSAGE_RUNNING
	MESSAGE_COMPLETED
	MESSAGE_CANCELED
	MESSAGE_ERRORED
)

// ChronoQueueClient is a client to call ChronoQueue RPC
type ChronoQueueClient struct {
	service pb_chronoqueue.ChronoQueueClient
}

// NewChronoQueueClient returns a new ChronoQueue client
func NewChronoQueueClient(conn *grpc.ClientConn) *ChronoQueueClient {
	service := pb_chronoqueue.NewChronoQueueClient(conn)
	return &ChronoQueueClient{service: service}
}

// CreateQueue create a queue and returns empty response
func (client *ChronoQueueClient) CreateQueue(name string, queueOptions QueueOptions) (*pb_chronoqueue.CreateQueueResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.CreateQueueRequest{
		Queue: &pb_chronoqueue.Queue{
			Name: name,
			Metadata: &pb_chronoqueue.Queue_Options{
				Type:                 pb_chronoqueue.Queue_Options_Type(queueOptions.Type),
				DequeueAttempts:      int32(queueOptions.DequeueAttempts),
				LeaseDuration:        queueOptions.LeaseDuration,
				ExclusivityKey:       queueOptions.ExclusivityKey,
				InvisibilityDuration: queueOptions.InvisibilityDuration,
			},
		},
	}
	res, err := client.service.CreateQueue(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// DeleteQueue deletes a queue and returns empty response
func (client *ChronoQueueClient) DeleteQueue(name string) (*pb_chronoqueue.DeleteQueueResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.DeleteQueueRequest{Name: name}
	res, err := client.service.DeleteQueue(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// PostMessage create adds a message to the queue and returns empty response
// Note: Payload is an opaque struct containing "metadata" and "data" fields.
//
//	The "data" field can be anything coverted in []byte.
//	The "metadata" field is a map[string][]byte that can be used to describe the data.
func (client *ChronoQueueClient) PostMessage(queue string, messageId string, messageOptions MessageOptions) (*pb_chronoqueue.PostMessageResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.PostMessageRequest{
		QueueName: queue,
		Message: &pb_chronoqueue.Message{
			MessageId: messageId,
			Metadata: &pb_chronoqueue.Message_Metadata{
				Payload: &pb_chronoqueue.Payload{
					Metadata: messageOptions.Payload.Metadata,
					Data:     messageOptions.Payload.Data,
				},
				AttemptsLeft:         messageOptions.AttemptsLeft,
				LeaseDuration:        messageOptions.LeaseDuration,
				LeaseExpiry:          &messageOptions.LeaseExpiry,
				InvisibilityDuration: messageOptions.InvisibilityDuration,
				State:                pb_chronoqueue.Message_Metadata_State(messageOptions.State),
			},
		},
	}
	res, err := client.service.PostMessage(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// GetNextMessage returns next message on a queue
func (client *ChronoQueueClient) GetNextMessage(queue string, leaseDuration *durationpb.Duration) (*pb_chronoqueue.GetNextMessageResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.GetNextMessageRequest{QueueName: queue, LeaseDuration: leaseDuration}
	res, err := client.service.GetNextMessage(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// PeekQueueMessages returns messages on a queue that are in pending state
func (client *ChronoQueueClient) PeekQueueMessages(queue string, limit int32, timeRange TimeRangeOption) (*pb_chronoqueue.PeekQueueMessagesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var priorityRange pb_chronoqueue.PeekQueueMessagesRequest_PriorityRange
	priorityRangeBytes, err := json.Marshal(timeRange)
	if err != nil {
		return &pb_chronoqueue.PeekQueueMessagesResponse{}, err
	}
	err = protojson.Unmarshal(priorityRangeBytes, &priorityRange)
	if err != nil {
		log.Println("Failed to deserialize priorityRange - err: ", err)
		return &pb_chronoqueue.PeekQueueMessagesResponse{}, err
	}

	req := &pb_chronoqueue.PeekQueueMessagesRequest{QueueName: queue, PriorityRange: &priorityRange}
	res, err := client.service.PeekQueueMessages(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// GetQueueState returns state of a queue
func (client *ChronoQueueClient) GetQueueState(queue string) (*pb_chronoqueue.GetQueueStateResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.GetQueueStateRequest{QueueName: queue}
	res, err := client.service.GetQueueState(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// RenewMessageLease updates a message's lease duration and returns empty response
func (client *ChronoQueueClient) RenewMessageLease(queue string, messageId string, leaseDuration *durationpb.Duration) (*pb_chronoqueue.RenewMessageLeaseResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.RenewMessageLeaseRequest{QueueName: queue, MessageId: messageId, LeaseDuration: leaseDuration}
	res, err := client.service.RenewMessageLease(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// AcknowledgeMessage updates state of a message and empty response
func (client *ChronoQueueClient) AcknowledgeMessage(queue string, messageId string, state State) (*pb_chronoqueue.AcknowledgeMessageResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb_chronoqueue.AcknowledgeMessageRequest{QueueName: queue, MessageId: messageId, State: pb_chronoqueue.Message_Metadata_State(state)}
	res, err := client.service.AcknowledgeMessage(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}
