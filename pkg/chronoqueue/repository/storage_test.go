package repository

import (
	"context"
	"reflect"
	"testing"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
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
		request *queueservice_pb.CreateQueueRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.CreateQueueResponse
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
				request: &queueservice_pb.CreateQueueRequest{
					Name: "test_queue",
				},
			},
			want:    &queueservice_pb.CreateQueueResponse{},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx:     context.TODO(),
				request: &queueservice_pb.CreateQueueRequest{},
			},
			want:    &queueservice_pb.CreateQueueResponse{},
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
		request *queueservice_pb.DeleteQueueRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.DeleteQueueResponse
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
				request: &queueservice_pb.DeleteQueueRequest{
					Name: "test_queue",
				},
			},
			want:    &queueservice_pb.DeleteQueueResponse{},
			wantErr: false,
		},
		{
			name: "Test missing queue name",
			fields: fields{
				redisClient: redisClient,
			},
			args: args{
				ctx:     context.TODO(),
				request: &queueservice_pb.DeleteQueueRequest{
					// Name: "test_queue",
				},
			},
			want:    &queueservice_pb.DeleteQueueResponse{},
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
			want:    &queueservice_pb.PostMessageResponse{},
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
			want:    &queueservice_pb.PostMessageResponse{},
			wantErr: true,
		},
	}
	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
					State:     message_pb.Message_Metadata_RUNNING,
				},
			},
			want:    &queueservice_pb.AcknowledgeMessageResponse{},
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
			want:    &queueservice_pb.RenewMessageLeaseResponse{},
			wantErr: false,
		},
	}

	// First, create a queue.
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
				Priority: time.Now().Unix(),
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
	as := &storage{
		redisClient: redisClient,
	}
	_, err := as.CreateQueue(context.TODO(), &queueservice_pb.CreateQueueRequest{
		Name: "test_queue",
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
				Priority: time.Now().Unix(),
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
			if !reflect.DeepEqual(got.StateCounts, tt.want) {
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
