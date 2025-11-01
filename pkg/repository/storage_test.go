package repository

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tc_redis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

var (
	redisContainer *tc_redis.RedisContainer
	redisClient    *redis.Client
)

func setup(t *testing.T) {
	ctx := context.Background()
	var err error
	redisContainer, err = tc_redis.RunContainer(ctx, testcontainers.WithImage("redis:7.2.4"))
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}

	endpoint, err := redisContainer.Endpoint(ctx, "redis")
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		t.Fatalf("Failed to get Redis endpoint: %v", err)
	}
	// Remove redis:// prefix if present
	endpoint = strings.TrimPrefix(endpoint, "redis://")
	redisClient = redis.NewClient(&redis.Options{
		Addr: endpoint,
	})
}

func teardown(t *testing.T) {
	ctx := context.Background()
	if redisContainer != nil {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Fatalf("Failed to terminate Redis container: %v", err)
		}
	}
	if redisClient != nil {
		_ = redisClient.Close()
	}
}

func TestNewQueueStorage(t *testing.T) {
	type args struct {
		ctx                  context.Context
		redisClient          *redis.Client
		encryptionKeyManager *keymanager.EncryptionKeyManager
		logger               *log.Logger
	}
	tests := []struct {
		name string
		args args
		want Storage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewQueueStorage(tt.args.ctx, tt.args.redisClient, tt.args.encryptionKeyManager, tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewQueueStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_CreateQueue(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Create a proper storage instance for testing (without background workers)
	logger := log.NewLogger()
	storage := NewQueueStorageForTesting(redisClient, nil, logger)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.CreateQueueRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.CreateQueueResponse
		wantErr bool
	}{
		{
			name: "Test successful create queue",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.CreateQueueRequest{
					Name: "test_queue",
					Metadata: &queue_pb.QueueMetadata{
						Type: queue_pb.QueueType_SIMPLE,
					},
				},
			},
			want: &queueservice_pb.CreateQueueResponse{
				Success: true,
			},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			args: args{
				ctx:     context.TODO(),
				request: &queueservice_pb.CreateQueueRequest{},
			},
			want: &queueservice_pb.CreateQueueResponse{
				Success: false,
			},
			wantErr: true,
		},
		{
			name: "Test missing metadata",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.CreateQueueRequest{
					Name: "test_queue_no_metadata",
				},
			},
			want: &queueservice_pb.CreateQueueResponse{
				Success: false,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := storage.CreateQueue(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.CreateQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("storage.CreateQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_DeleteQueue(t *testing.T) {
	setup(t)
	defer teardown(t)

	// Create a proper storage instance for testing (without background workers)
	logger := log.NewLogger()
	storage := NewQueueStorageForTesting(redisClient, nil, logger)

	// First create a queue to delete in the successful test case
	createRequest := &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	}
	_, err := storage.CreateQueue(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("Failed to create test queue: %v", err)
	}

	type args struct {
		ctx     context.Context
		request *queueservice_pb.DeleteQueueRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.DeleteQueueResponse
		wantErr bool
	}{
		{
			name: "Test successful delete queue",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.DeleteQueueRequest{
					Name: "test_queue",
				},
			},
			want: &queueservice_pb.DeleteQueueResponse{
				Success: true,
			},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.DeleteQueueRequest{
					Name: "",
				},
			},
			want: &queueservice_pb.DeleteQueueResponse{
				Success: false,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := storage.DeleteQueue(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.DeleteQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("storage.DeleteQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_CreateQueueMessage(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.PostMessageRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.PostMessageResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful post queue message",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.PostMessageRequest{
					QueueName: "test_queue",
					Message: &message_pb.Message{
						MessageId: "test_message_id",
						Metadata: &message_pb.Message_Metadata{
							Payload:  &common_pb.Payload{},
							Priority: time.Now().Unix(),
						},
					},
				},
			},
			want:    &queueservice_pb.PostMessageResponse{Success: true},
			wantErr: false,
		},
		{
			name: "Test missing message ID",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.PostMessageRequest{
					QueueName: "test_queue",
					Message: &message_pb.Message{
						// MessageId: "test_message_id",
						Metadata: &message_pb.Message_Metadata{
							Payload:  &common_pb.Payload{},
							Priority: time.Now().Unix(),
						},
					},
				},
			},
			want:    nil, // Expect nil response when there's an error
			wantErr: true,
		},
	}
	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.CreateQueueMessage(tt.args.ctx, tt.args.request, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.CreateQueueMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("storage.CreateQueueMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_GetQueueMessage(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.GetNextMessageRequest
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful get queue message",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.GetNextMessageRequest{
					QueueName:     "test_queue",
					LeaseDuration: durationpb.New(30 * time.Second),
				},
			},
			want:    "test_message_id",
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, directly set up a message in PENDING state and add to the stream for testing
	testMessageKey := "test_queue:test_message_id:meta"
	testMessage := &message_pb.Message{
		MessageId: "test_message_id",
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 5,
		},
	}

	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	if err != nil {
		t.Errorf("Failed to marshal message metadata: %v", err)
		return
	}
	err = redisClient.HSet(context.TODO(), testMessageKey, "metadata", metadataJSON).Err()
	if err != nil {
		t.Errorf("Failed to set message metadata in Redis: %v", err)
		return
	}

	// Add message to the stream directly (simulate scheduler)
	streamKey := as.(*storage).streamKey("test_queue", 5)
	groupKey := as.(*storage).groupKey("test_queue")
	_ = redisClient.XGroupCreateMkStream(context.TODO(), streamKey, groupKey, "0").Err()
	_, err = redisClient.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"message_id":     "test_message_id",
			"priority":       5,
			"scheduled_time": time.Now().UnixMilli(),
		},
	}).Result()
	if err != nil {
		t.Errorf("Failed to add message to stream: %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.GetQueueMessage(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.GetQueueMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.GetMessage().GetMessageId(), tt.want) {
				t.Errorf("storage.GetQueueMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_DeleteQueueMessage(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx       context.Context
		queueName string
		messageID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful delete queue message",
			args: args{
				ctx:       context.TODO(),
				queueName: "test_queue",
				messageID: "test_message_id",
			},
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &queueservice_pb.PostMessageRequest{
		QueueName: "test_queue",
		Message: &message_pb.Message{
			MessageId: "test_message_id",
			Metadata: &message_pb.Message_Metadata{
				Payload:  &common_pb.Payload{},
				State:    message_pb.Message_Metadata_PENDING,
				Priority: 0,
			},
		},
	}, nil)
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := as.DeleteQueueMessage(tt.args.ctx, tt.args.queueName, tt.args.messageID); (err != nil) != tt.wantErr {
				t.Errorf("storage.DeleteQueueMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_storage_AcknowledgeMessage(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.AcknowledgeMessageRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.AcknowledgeMessageResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful acknowledge queue message",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.AcknowledgeMessageRequest{
					QueueName: "test_queue",
					MessageId: "test_message_id",
					State:     message_pb.Message_Metadata_COMPLETED,
				},
			},
			want:    &queueservice_pb.AcknowledgeMessageResponse{Success: true},
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, set up a message in RUNNING state (ready for acknowledgment)
	testMessageKey := "test_queue:test_message_id:meta"
	testMessage := &message_pb.Message{
		MessageId: "test_message_id",
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_RUNNING,
			Priority: 0,
		},
	}
	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	if err != nil {
		t.Errorf("Failed to marshal message metadata: %v", err)
		return
	}
	err = redisClient.HSet(context.TODO(), testMessageKey, "metadata", metadataJSON).Err()
	if err != nil {
		t.Errorf("Failed to set message metadata: %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.AcknowledgeMessage(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.AcknowledgeMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("storage.AcknowledgeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_RenewMessageLease(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.RenewMessageLeaseRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.RenewMessageLeaseResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful renew queue message's lease",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.RenewMessageLeaseRequest{
					QueueName:     "test_queue",
					MessageId:     "test_message_id",
					LeaseDuration: durationpb.New(30 * time.Second),
				},
			},
			want: &queueservice_pb.RenewMessageLeaseResponse{
				RemainingTime: durationpb.New(30 * time.Second),
				State:         message_pb.Message_Metadata_RUNNING,
			},
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, set up a message in RUNNING state (leases can only be renewed for running messages)
	testMessageKey := "test_queue:test_message_id:meta"
	testMessage := &message_pb.Message{
		MessageId: "test_message_id",
		Metadata: &message_pb.Message_Metadata{
			Payload:     &common_pb.Payload{},
			State:       message_pb.Message_Metadata_RUNNING,
			Priority:    0,
			LeaseExpiry: time.Now().Add(30 * time.Second).UnixMilli(),
		},
	}
	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	if err != nil {
		t.Errorf("Failed to marshal message metadata: %v", err)
		return
	}
	err = redisClient.HSet(context.TODO(), testMessageKey, "metadata", metadataJSON).Err()
	if err != nil {
		t.Errorf("Failed to set message metadata: %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.RenewMessageLease(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.RenewMessageLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check error condition matches
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.RenewMessageLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// For successful renewal, check state and remaining time with tolerance
			if !tt.wantErr && got != nil && tt.want != nil {
				// Check state matches
				if got.State != tt.want.State {
					t.Errorf("storage.RenewMessageLease() state = %v, want %v", got.State, tt.want.State)
				}

				// Check remaining time is approximately correct (within 1 second tolerance)
				// This accounts for execution time between setup and assertion
				if got.RemainingTime != nil && tt.want.RemainingTime != nil {
					gotDuration := got.RemainingTime.AsDuration()
					wantDuration := tt.want.RemainingTime.AsDuration()
					diff := gotDuration - wantDuration
					if diff < 0 {
						diff = -diff
					}

					// Allow 1 second tolerance for timing variations
					if diff > time.Second {
						t.Errorf("storage.RenewMessageLease() remaining_time = %v, want approximately %v (diff: %v)",
							gotDuration, wantDuration, diff)
					}
				}
			} else if !reflect.DeepEqual(got, tt.want) {
				// For error cases or nil responses, use exact comparison
				t.Errorf("storage.RenewMessageLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_PeekQueueMessages(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.PeekQueueMessagesRequest
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful peek queue messages",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.PeekQueueMessagesRequest{
					QueueName: "test_queue",
					Limit:     10,
				},
			},
			want:    1,
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add message directly to the stream (simulate scheduler)
	testMessageKey := "test_queue:test_message_id:meta"
	testMessage := &message_pb.Message{
		MessageId: "test_message_id",
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 5,
		},
	}

	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	if err != nil {
		t.Errorf("Failed to marshal message metadata: %v", err)
		return
	}
	err = redisClient.HSet(context.TODO(), testMessageKey, "metadata", metadataJSON).Err()
	if err != nil {
		t.Errorf("Failed to set message metadata in Redis: %v", err)
		return
	}

	streamKey := as.(*storage).streamKey("test_queue", 5)
	groupKey := as.(*storage).groupKey("test_queue")
	_ = redisClient.XGroupCreateMkStream(context.TODO(), streamKey, groupKey, "0").Err()
	_, err = redisClient.XAdd(context.TODO(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"message_id":     "test_message_id",
			"priority":       5,
			"scheduled_time": time.Now().UnixMilli(),
		},
	}).Result()
	if err != nil {
		t.Errorf("Failed to add message to stream: %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.PeekQueueMessages(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.PeekQueueMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(len(got.GetMessages()), tt.want) {
				t.Errorf("storage.PeekQueueMessages() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_GetQueueState(t *testing.T) {
	setup(t)
	defer teardown(t)

	type args struct {
		ctx     context.Context
		request *queueservice_pb.GetQueueStateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]int32
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful peek queue messages",
			args: args{
				ctx: context.TODO(),
				request: &queueservice_pb.GetQueueStateRequest{
					QueueName: "test_queue",
				},
			},
			want:    map[string]int32{"PENDING": 1},
			wantErr: false,
		},
	}

	// First, create a queue.
	logger := log.NewLogger()
	as := NewQueueStorageForTesting(redisClient, nil, logger)
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, set up a message in PENDING state for state counting
	testMessageKey := "test_queue:test_message_id:meta"
	testMessage := &message_pb.Message{
		MessageId: "test_message_id",
		Metadata: &message_pb.Message_Metadata{
			Payload:  &common_pb.Payload{},
			State:    message_pb.Message_Metadata_PENDING,
			Priority: 0,
		},
	}
	metadataJSON, err := protojson.Marshal(testMessage.Metadata)
	if err != nil {
		t.Errorf("Failed to marshal message metadata: %v", err)
		return
	}
	err = redisClient.HSet(context.TODO(), testMessageKey, "metadata", metadataJSON).Err()
	if err != nil {
		t.Errorf("Failed to set message metadata: %v", err)
		return
	}

	// Add to sorted set
	err = redisClient.ZAdd(context.TODO(), "queue:test_queue", redis.Z{
		Score:  float64(100 * 1e10),
		Member: "test_message_id",
	}).Err()
	if err != nil {
		t.Errorf("Failed to add message to queue: %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.GetQueueState(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.GetQueueState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Check that expected state counts match
			for state, expectedCount := range tt.want {
				if gotCount, exists := got.StateCounts[state]; !exists || gotCount != expectedCount {
					t.Errorf("storage.GetQueueState() state %s = %v, want %v", state, gotCount, expectedCount)
				}
			}
		})
	}
}

// func Test_storage_RunLuaScripts(t *testing.T) {
// 	setup()
// 	defer teardown()

// 	type args struct {
// 		ctx context.Context
// 	}
// 	tests := []struct {
// 		name string
// 		args args
// 	}{
// 		// TODO: Add more test cases.
// 		{
// 			name: "Test successful running lua script",
// 			args: args{
// 				ctx: context.TODO(),
// 			},
// 		},
// 	}

// 	// First, create a queue.
// 	as := &storage{
// 		redisClient: redisClient,
// 	}
// 	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
// 		Queue: &queueservice_pb.Queue{
// 			Name: "test_queue",
// 		},
// 	})
// 	if err != nil {
// 		t.Errorf("storage.CreateQueue() error = %v", err)
// 		return
// 	}

// 	// Second, add messages to the queue.
// 	_, err = as.CreateQueueMessage(context.TODO(), &queueservice_pb.PostMessageRequest{
// 		QueueName: "test_queue",
// 		Message: &queueservice_pb.Message{
// 			MessageId: "test_message_id",
// 			Priority:  time.Now().Unix(),
// 			Metadata: &queueservice_pb.Message_Metadata{
// 				Payload: &queueservice_pb.Payload{},
// 				State:   queueservice_pb.Message_Metadata_INVISIBLE,
// 			},
// 		},
// 	})
// 	if err != nil {
// 		t.Errorf("storage.CreateQueueMessage() error = %v", err)
// 		return
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			as.RunLuaScripts(tt.args.ctx)
// 		})
// 	}
// }
