package repository

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/alicebob/miniredis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/durationpb"
)

var redisServer *miniredis.Miniredis
var redisClient *redis.Client

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()

	if err != nil {
		panic(err)
	}

	return s
}

func setup() {
	redisServer = mockRedis()
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})
}

func teardown() {
	redisServer.Close()
}

func TestNewQueueStorage(t *testing.T) {
	type args struct {
		ctx                  context.Context
		redisClient          *redis.Client
		encryptionKeyManager *keymanager.EncryptionKeyManager
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
			if got := NewQueueStorage(tt.args.ctx, tt.args.redisClient, tt.args.encryptionKeyManager); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewQueueStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_CreateQueue(t *testing.T) {
	setup()
	defer teardown()

	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx     context.Context
		request *chronoqueue.CreateQueueRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronoqueue.CreateQueueResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful create queue",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.CreateQueueRequest{
					Queue: &chronoqueue.Queue{
						Name: "test_queue",
					},
				},
			},
			want:    &chronoqueue.CreateQueueResponse{},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.CreateQueueRequest{
					Queue: &chronoqueue.Queue{
						// Name: "test_queue",
					},
				},
			},
			want:    &chronoqueue.CreateQueueResponse{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := &storage{
				redisClient: tt.fields.redisClient,
			}
			got, err := as.CreateQueue(tt.args.ctx, tt.args.request)
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
	setup()
	defer teardown()

	type fields struct {
		redisClient *redis.Client
	}
	type args struct {
		ctx     context.Context
		request *chronoqueue.DeleteQueueRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronoqueue.DeleteQueueResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful delete queue",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.DeleteQueueRequest{
					Name: "test_queue",
				},
			},
			want:    &chronoqueue.DeleteQueueResponse{},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx:     context.TODO(),
				request: &chronoqueue.DeleteQueueRequest{
					// Name: "test_queue",
				},
			},
			want:    &chronoqueue.DeleteQueueResponse{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := &storage{
				redisClient: tt.fields.redisClient,
			}
			got, err := as.DeleteQueue(tt.args.ctx, tt.args.request)
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
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.PostMessageRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *chronoqueue.PostMessageResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful post queue message",
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.PostMessageRequest{
					QueueName: "test_queue",
					Message: &chronoqueue.Message{
						MessageId: "test_message_id",
						Priority:  time.Now().Unix(),
						Metadata: &chronoqueue.Message_Metadata{
							Payload: &chronoqueue.Payload{},
						},
					},
				},
			},
			want:    &chronoqueue.PostMessageResponse{},
			wantErr: false,
		},
		{
			name: "Test missing message ID",
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.PostMessageRequest{
					QueueName: "test_queue",
					Message: &chronoqueue.Message{
						// MessageId: "test_message_id",
						Priority: time.Now().Unix(),
						Metadata: &chronoqueue.Message_Metadata{
							Payload: &chronoqueue.Payload{},
						},
					},
				},
			},
			want:    &chronoqueue.PostMessageResponse{},
			wantErr: true,
		},
	}
	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.CreateQueueMessage(tt.args.ctx, tt.args.request)
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
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.GetNextMessageRequest
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
				request: &chronoqueue.GetNextMessageRequest{
					QueueName:     "test_queue",
					LeaseDuration: durationpb.New(30 * time.Second),
				},
			},
			want:    "test_message_id",
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
			// Metadata: &chronoqueue.Queue_Options{},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  0,
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
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
	setup()
	defer teardown()

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
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  0,
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
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
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.AcknowledgeMessageRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *chronoqueue.AcknowledgeMessageResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful acknowledge queue message",
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.AcknowledgeMessageRequest{
					QueueName: "test_queue",
					MessageId: "test_message_id",
					State:     chronoqueue.Message_Metadata_RUNNING,
				},
			},
			want:    &chronoqueue.AcknowledgeMessageResponse{},
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  0,
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
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
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.RenewMessageLeaseRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *chronoqueue.RenewMessageLeaseResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful renew queue message's lease",
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.RenewMessageLeaseRequest{
					QueueName:     "test_queue",
					MessageId:     "test_message_id",
					LeaseDuration: durationpb.New(30 * time.Second),
				},
			},
			want:    &chronoqueue.RenewMessageLeaseResponse{},
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  0,
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.RenewMessageLease(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.RenewMessageLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("storage.RenewMessageLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storage_PeekQueueMessages(t *testing.T) {
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.PeekQueueMessagesRequest
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
				request: &chronoqueue.PeekQueueMessagesRequest{
					QueueName: "test_queue",
					Limit:     10,
				},
			},
			want:    1,
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  time.Now().Unix(),
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
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
	setup()
	defer teardown()

	type args struct {
		ctx     context.Context
		request *chronoqueue.GetQueueStateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    int32
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful peek queue messages",
			args: args{
				ctx: context.TODO(),
				request: &chronoqueue.GetQueueStateRequest{
					QueueName: "test_queue",
				},
			},
			want:    1,
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
		Queue: &chronoqueue.Queue{
			Name: "test_queue",
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueue() error = %v", err)
		return
	}

	// Second, add messages to the queue.
	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
		QueueName: "test_queue",
		Message: &chronoqueue.Message{
			MessageId: "test_message_id",
			Priority:  time.Now().Unix(),
			Metadata: &chronoqueue.Message_Metadata{
				Payload: &chronoqueue.Payload{},
				State:   chronoqueue.Message_Metadata_PENDING,
			},
		},
	})
	if err != nil {
		t.Errorf("storage.CreateQueueMessage() error = %v", err)
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := as.GetQueueState(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.GetQueueState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.PendingMessagesCount, tt.want) {
				t.Errorf("storage.GetQueueState() = %v, want %v", got, tt.want)
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
// 	_, err := as.CreateQueue(context.TODO(), &chronoqueue.CreateQueueRequest{
// 		Queue: &chronoqueue.Queue{
// 			Name: "test_queue",
// 		},
// 	})
// 	if err != nil {
// 		t.Errorf("storage.CreateQueue() error = %v", err)
// 		return
// 	}

// 	// Second, add messages to the queue.
// 	_, err = as.CreateQueueMessage(context.TODO(), &chronoqueue.PostMessageRequest{
// 		QueueName: "test_queue",
// 		Message: &chronoqueue.Message{
// 			MessageId: "test_message_id",
// 			Priority:  time.Now().Unix(),
// 			Metadata: &chronoqueue.Message_Metadata{
// 				Payload: &chronoqueue.Payload{},
// 				State:   chronoqueue.Message_Metadata_INVISIBLE,
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
