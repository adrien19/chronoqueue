package client

import (
	"context"
	"errors"
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
)

type mockChronoQueueServer struct {
	queueservice_pb.UnimplementedQueueServiceServer
	lastAckAttemptID string
	lastAckWorkerID  string
}

func dialer() func(context.Context, string) (net.Conn, error) {
	listener := bufconn.Listen(1024 * 1024)

	server := grpc.NewServer()

	queueservice_pb.RegisterQueueServiceServer(server, &mockChronoQueueServer{})

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func testConnector(dialer func(context.Context, string) (net.Conn, error)) Connector {
	return func(address string, opts ClientOptions) (queueservice_pb.QueueServiceClient, *grpc.ClientConn, error) {
		ctx := context.Background()
		//nolint:staticcheck // Using deprecated DialContext for test compatibility with bufconn
		conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		client := queueservice_pb.NewQueueServiceClient(conn)
		return client, conn, nil
	}
}

func (*mockChronoQueueServer) CreateQueue(ctx context.Context, req *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	if req.GetName() == "" {
		return &queueservice_pb.CreateQueueResponse{
			Success: false,
		}, status.Errorf(codes.InvalidArgument, "cannot create queue with no name %v", req)
	}
	return &queueservice_pb.CreateQueueResponse{
		Success: true,
	}, nil
}

func (*mockChronoQueueServer) DeleteQueue(ctx context.Context, req *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	if req.GetName() == "" {
		return &queueservice_pb.DeleteQueueResponse{Success: false}, status.Errorf(codes.InvalidArgument, "cannot delete queue with no name %v", req.Name)
	}
	return &queueservice_pb.DeleteQueueResponse{Success: true}, nil
}

func (*mockChronoQueueServer) PostMessage(ctx context.Context, req *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.PostMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "cannot post message given queue with no name %v", req.GetQueueName())
	}
	if req.Message.GetMessageId() == "" {
		return &queueservice_pb.PostMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "cannot post message with no message ID %v", req.Message.GetMessageId())
	}
	return &queueservice_pb.PostMessageResponse{Success: true}, nil
}

func (*mockChronoQueueServer) SendMessageHeartbeat(ctx context.Context, req *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, status.Errorf(codes.InvalidArgument, "cannot send heartbeat given queue with no name %v", req.GetQueueName())
	}
	if req.GetMessageId() == "" {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, status.Errorf(codes.InvalidArgument, "cannot post message with no message ID %v", req.GetMessageId())
	}
	return &queueservice_pb.SendMessageHeartBeatResponse{}, nil
}

func (*mockChronoQueueServer) GetNextMessage(ctx context.Context, req *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.GetNextMessageResponse{}, status.Errorf(codes.InvalidArgument, "cannot query queue with no name %v", req)
	}
	if req.GetQueueName() == "emptyQueue" {
		return &queueservice_pb.GetNextMessageResponse{}, nil
	}
	return &queueservice_pb.GetNextMessageResponse{
		Message: &message_pb.Message{
			MessageId: "test_message",
			Metadata:  &message_pb.Message_Metadata{},
		},
	}, nil
}

func (*mockChronoQueueServer) PeekQueueMessages(ctx context.Context, req *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.PeekQueueMessagesResponse{}, status.Errorf(codes.InvalidArgument, "cannot query queue with no name %v", req)
	}
	if req.GetQueueName() == "emptyQueue" {
		return &queueservice_pb.PeekQueueMessagesResponse{}, nil
	}
	return &queueservice_pb.PeekQueueMessagesResponse{
		Messages: []*message_pb.Message{
			{
				MessageId: "test_message",
				Metadata:  &message_pb.Message_Metadata{},
			},
		},
	}, nil
}

func (*mockChronoQueueServer) GetQueueState(ctx context.Context, req *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.GetQueueStateResponse{}, status.Errorf(codes.InvalidArgument, "cannot query queue with no name %v", req)
	}
	if req.GetQueueName() == "emptyQueue" {
		return &queueservice_pb.GetQueueStateResponse{
			StateCounts: map[string]int32{
				"INVISIBLE": 0,
				"PENDING":   0,
				"RUNNING":   0,
				"COMPLETED": 0,
				"CANCELED":  0,
				"ERRORED":   0,
			},
			EarliestDeadline: nil,
		}, nil
	}
	return &queueservice_pb.GetQueueStateResponse{
		StateCounts: map[string]int32{
			"INVISIBLE": 1,
			"PENDING":   0,
			"RUNNING":   0,
			"COMPLETED": 0,
			"CANCELED":  0,
			"ERRORED":   0,
		},
		EarliestDeadline: nil,
	}, nil
}

func (*mockChronoQueueServer) RenewMessageLease(ctx context.Context, req *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.RenewMessageLeaseResponse{}, status.Errorf(codes.InvalidArgument, "cannot renew message's lease given queue with no name %v", req.GetQueueName())
	}
	if req.GetMessageId() == "" {
		return &queueservice_pb.RenewMessageLeaseResponse{}, status.Errorf(codes.InvalidArgument, "cannot renew message's lease with no message ID %v", req.GetMessageId())
	}
	return &queueservice_pb.RenewMessageLeaseResponse{
		RemainingTime: durationpb.New(30 * time.Second),
		State:         message_pb.Message_Metadata_RUNNING,
	}, nil
}

func (s *mockChronoQueueServer) AcknowledgeMessage(ctx context.Context, req *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "cannot acknowledge message given queue with no name %v", req.GetQueueName())
	}
	if req.GetMessageId() == "" {
		return &queueservice_pb.AcknowledgeMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "cannot acknowledge message with no message ID %v", req.GetMessageId())
	}
	if req.AttemptId != nil {
		s.lastAckAttemptID = req.GetAttemptId()
	} else {
		s.lastAckAttemptID = ""
	}
	if req.WorkerId != nil {
		s.lastAckWorkerID = req.GetWorkerId()
	} else {
		s.lastAckWorkerID = ""
	}
	return &queueservice_pb.AcknowledgeMessageResponse{Success: true}, nil
}

func (*mockChronoQueueServer) SendMessageHeartBeat(ctx context.Context, req *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, status.Errorf(codes.InvalidArgument, "cannot send message's heartbeat given queue with no name %v", req.GetQueueName())
	}
	if req.GetMessageId() == "" {
		return &queueservice_pb.SendMessageHeartBeatResponse{}, status.Errorf(codes.InvalidArgument, "cannot send message's heartbeat with no message ID %v", req.GetMessageId())
	}
	return &queueservice_pb.SendMessageHeartBeatResponse{}, nil
}

func (*mockChronoQueueServer) CancelMessage(ctx context.Context, req *queueservice_pb.CancelMessageRequest) (*queueservice_pb.CancelMessageResponse, error) {
	if req.GetQueueName() == "" {
		return &queueservice_pb.CancelMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "queue name is required")
	}
	if req.GetMessageId() == "" {
		return &queueservice_pb.CancelMessageResponse{Success: false}, status.Errorf(codes.InvalidArgument, "message ID is required")
	}
	return &queueservice_pb.CancelMessageResponse{Success: true}, nil
}

func (*mockChronoQueueServer) ListQueues(ctx context.Context, req *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	return &queueservice_pb.ListQueuesResponse{
		Queues: []*queue_pb.Queue{
			{
				Name:     "test_queue",
				Metadata: &queue_pb.QueueMetadata{},
			},
		},
	}, nil
}

// DLQ Methods for mockChronoQueueServer
func (*mockChronoQueueServer) GetDLQMessages(ctx context.Context, req *queueservice_pb.GetDLQMessagesRequest) (*queueservice_pb.GetDLQMessagesResponse, error) {
	return &queueservice_pb.GetDLQMessagesResponse{
		Messages: []*message_pb.Message{
			{
				MessageId: "test-dlq-msg-1",
				Metadata: &message_pb.Message_Metadata{
					State: message_pb.Message_Metadata_ERRORED,
				},
			},
		},
	}, nil
}

func (*mockChronoQueueServer) RequeueFromDLQ(ctx context.Context, req *queueservice_pb.RequeueFromDLQRequest) (*queueservice_pb.RequeueFromDLQResponse, error) {
	return &queueservice_pb.RequeueFromDLQResponse{Success: true}, nil
}

func (*mockChronoQueueServer) DeleteFromDLQ(ctx context.Context, req *queueservice_pb.DeleteFromDLQRequest) (*queueservice_pb.DeleteFromDLQResponse, error) {
	return &queueservice_pb.DeleteFromDLQResponse{Success: true}, nil
}

func (*mockChronoQueueServer) PurgeDLQ(ctx context.Context, req *queueservice_pb.PurgeDLQRequest) (*queueservice_pb.PurgeDLQResponse, error) {
	return &queueservice_pb.PurgeDLQResponse{Success: true}, nil
}

func (*mockChronoQueueServer) GetDLQStats(ctx context.Context, req *queueservice_pb.GetDLQStatsRequest) (*queueservice_pb.GetDLQStatsResponse, error) {
	return &queueservice_pb.GetDLQStatsResponse{
		Name:         req.DlqName,
		MessageCount: 5,
		CreatedAt:    1640995200, // Example timestamp
		UpdatedAt:    1640995200,
	}, nil
}

func TestNewChronoQueueClient(t *testing.T) {
	dialer := dialer()

	type args struct {
		address string
		opts    ClientOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *ChronoQueueClient
		wantErr bool
	}{
		{
			name: "Successful client creation",
			args: args{
				address: "bufnet",
				opts: ClientOptions{
					Connector: testConnector(dialer), // pass the testConnector
				},
			},
			wantErr: false,
		},
		{
			name: "Fail client creation with invalid address",
			args: args{
				address: "",
				opts:    ClientOptions{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewChronoQueueClient(tt.args.address, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewChronoQueueClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_checkDefaultClientOptions(t *testing.T) {
	type args struct {
		opts ClientOptions
	}
	tests := []struct {
		name string
		args args
		want ClientOptions
	}{
		{
			name: "Default values when empty options provided",
			args: args{
				opts: ClientOptions{},
			},
			want: ClientOptions{
				MaxRetries:             5,
				InitialBackoff:         time.Millisecond * 500,
				MaxBackoff:             time.Second * 60,
				MaxHeartBeatWorkers:    10,
				DefaultRPCTimeout:      time.Second * 3,
				MaxHeartbeatRetryCount: 10,
			},
		},
		{
			name: "Custom values are preserved",
			args: args{
				opts: ClientOptions{
					MaxRetries:             3,
					InitialBackoff:         time.Second * 2,
					MaxBackoff:             time.Second * 2,
					MaxHeartBeatWorkers:    5,
					MaxHeartbeatRetryCount: 3,
				},
			},
			want: ClientOptions{
				MaxRetries:             3,
				InitialBackoff:         time.Second * 2,
				MaxBackoff:             time.Second * 2,
				MaxHeartBeatWorkers:    5,
				DefaultRPCTimeout:      time.Second * 3,
				MaxHeartbeatRetryCount: 3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkDefaultClientOptions(tt.args.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkDefaultClientOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultServerConnector(t *testing.T) {
	type args struct {
		address string
		opts    ClientOptions
	}
	tests := []struct {
		name    string
		args    args
		want    queueservice_pb.QueueServiceClient
		want1   *grpc.ClientConn
		wantErr bool
	}{
		{
			name: "Invalid address",
			args: args{
				address: "",
				opts:    ClientOptions{},
			},
			want:    nil,
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := DefaultServerConnector(tt.args.address, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultServerConnector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultServerConnector() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_heartbeatWorker(t *testing.T) {
	type fields struct {
		service   queueservice_pb.QueueServiceClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			client.heartbeatWorker()
		})
	}
}

func TestChronoQueueClient_setDefaultContextTimeout(t *testing.T) {
	type fields struct {
		service   queueservice_pb.QueueServiceClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   context.Context
		want1  context.CancelFunc
	}{
		{
			name: "Setting default context timeout",
			fields: fields{
				opts: ClientOptions{
					DefaultRPCTimeout: time.Second * 5,
				},
			},
			args: args{
				ctx: context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			got, _ := client.setDefaultContextTimeout(tt.args.ctx)
			if _, ok := got.Deadline(); !ok {
				t.Errorf("ChronoQueueClient.setDefaultContextTimeout() ok = %v, want %v", ok, true)
			}
		})
	}
}

func Test_parseDurationToProto(t *testing.T) {
	type args struct {
		durationStr string
	}
	tests := []struct {
		name    string
		args    args
		want    *durationpb.Duration
		wantErr bool
	}{
		{
			name: "Valid duration string",
			args: args{
				durationStr: "5s",
			},
			want:    durationpb.New(5 * time.Second),
			wantErr: false,
		},
		{
			name: "Invalid duration string",
			args: args{
				durationStr: "invalidDuration",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDurationToProto(tt.args.durationStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDurationToProto() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseDurationToProto() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_CreateQueue(t *testing.T) {
	dialer := dialer()

	type args struct {
		ctx          context.Context
		name         string
		queueOptions QueueOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.CreateQueueResponse
		wantErr bool
	}{
		{
			name: "Successful Queue Creation",
			args: args{
				ctx:  context.Background(),
				name: "validQueueName",
				queueOptions: QueueOptions{
					LeaseDuration: "15s",
				},
			},
			want: &queueservice_pb.CreateQueueResponse{
				Success: true,
			},
			wantErr: false,
		},
		{
			name: "Fail to Create Queue without Name",
			args: args{
				ctx:  context.Background(),
				name: "",
				queueOptions: QueueOptions{
					LeaseDuration: "15s",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.CreateQueue(tt.args.ctx, tt.args.name, tt.args.queueOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.CreateQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.CreateQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_DeleteQueue(t *testing.T) {
	dialer := dialer()

	type args struct {
		ctx          context.Context
		name         string
		queueOptions QueueOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.DeleteQueueResponse
		wantErr bool
	}{
		{
			name: "Successful Queue Deletion",
			args: args{
				ctx:  context.Background(),
				name: "validQueueName",
				queueOptions: QueueOptions{
					LeaseDuration: "15s",
				},
			},
			want: &queueservice_pb.DeleteQueueResponse{
				Success: true,
			},
			wantErr: false,
		},
		{
			name: "Unsuccessful deletion due to invalid queue name",
			args: args{
				ctx:  context.Background(),
				name: "",
				queueOptions: QueueOptions{
					LeaseDuration: "15s",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.DeleteQueue(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.DeleteQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.DeleteQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_PostMessage(t *testing.T) {
	dialer := dialer()

	type args struct {
		ctx            context.Context
		queue          string
		messageId      string
		messageOptions MessageOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *queueservice_pb.PostMessageResponse
		wantErr bool
	}{
		{
			name: "Successful Message Posting",
			args: args{
				ctx:       context.Background(),
				queue:     "validQueueName",
				messageId: "validMessageId",
				messageOptions: MessageOptions{
					// Your message options here.
					LeaseDuration: "3s",
				},
			},
			want: &queueservice_pb.PostMessageResponse{
				Success: true,
			},
			wantErr: false,
		},
		{
			name: "Failed Message Posting with Invalid Queue Name",
			args: args{
				ctx:       context.Background(),
				queue:     "",
				messageId: "validMessageId",
				messageOptions: MessageOptions{
					LeaseDuration: "3s",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Failed Message Posting with Invalid Message ID",
			args: args{
				ctx:       context.Background(),
				queue:     "validQueueName",
				messageId: "",
				messageOptions: MessageOptions{
					LeaseDuration: "3s",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Failed Message Posting with Invalid Delay Duration",
			args: args{
				ctx:       context.Background(),
				queue:     "validQueueName",
				messageId: "validMessageId",
				messageOptions: MessageOptions{
					LeaseDuration: "invalidDuration",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.PostMessage(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.messageOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.PostMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.PostMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_manageHeartbeats(t *testing.T) {
	dialer := dialer()

	type fields struct {
		sendHeartbeatCallCounter int
		retryCount               int
		failedCalls              int
		successfulCalls          int
	}

	type args struct {
		ctx       context.Context
		queueName string
		messageId string
		attemptID string
		workerID  string
	}
	tests := []struct {
		name     string
		args     args
		fields   fields
		setup    func(*fields, *ChronoQueueClient)
		validate func(*testing.T, *fields, *ChronoQueueClient)
	}{
		{
			name: "Heartbeat Success",
			args: args{
				ctx:       context.Background(),
				queueName: "validQueue",
				messageId: "validMessageId",
			},
			fields: fields{
				sendHeartbeatCallCounter: 0,
				retryCount:               0,
				failedCalls:              0,
				successfulCalls:          0,
			},
			setup: func(f *fields, client *ChronoQueueClient) {
				// Override SendMessageHeartbeat to always succeed
				client.opts.SendMessageHeartbeatFunc = func(ctx context.Context, queueName, messageId string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
					f.sendHeartbeatCallCounter++
					return &queueservice_pb.SendMessageHeartBeatResponse{}, nil
				}
				client.opts.MaxHeartbeatRetryCount = 1
			},
			validate: func(t *testing.T, f *fields, client *ChronoQueueClient) {
				// Validate that SendMessageHeartbeat was called
				time.Sleep(time.Second)
				if f.sendHeartbeatCallCounter == 0 {
					t.Error("SendMessageHeartbeat was never called!")
				}
			},
		},
		{
			name: "Heartbeat Fail with Retry",
			args: args{
				ctx:       context.Background(),
				queueName: "validQueue",
				messageId: "validMessageId",
			},
			fields: fields{
				sendHeartbeatCallCounter: 0,
				retryCount:               0,
				failedCalls:              0,
				successfulCalls:          0,
			},
			setup: func(f *fields, client *ChronoQueueClient) {
				client.opts.SendMessageHeartbeatFunc = func(ctx context.Context, queueName, messageId string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
					f.sendHeartbeatCallCounter++ // Increment call counter each time method is invoked

					if f.retryCount == 0 {
						f.failedCalls++ // Increment failed calls counter
						f.retryCount++  // Increment retry counter
						return nil, errors.New("forced error")
					}

					f.successfulCalls++ // Increment successful calls counter
					return &queueservice_pb.SendMessageHeartBeatResponse{}, nil
				}
			},
			validate: func(t *testing.T, f *fields, client *ChronoQueueClient) {
				// Validate that SendMessageHeartbeat was called and recovered after a retry
				time.Sleep(2 * time.Second)

				// Validate that SendMessageHeartbeat was called, failed initially, but succeeded upon retry
				if f.failedCalls != 1 {
					t.Errorf("SendMessageHeartbeat did not fail as expected: got %v failures, want 1", f.failedCalls)
				}
				// time.Sleep(time.Second)
				if f.successfulCalls != 1 {
					t.Errorf("SendMessageHeartbeat did not succeed upon retry: got %v successes, want 1", f.successfulCalls)
				}
			},
		},
		{
			name: "Max Retries Reached",
			args: args{
				ctx:       context.Background(),
				queueName: "validQueue",
				messageId: "validMessageId",
			},
			fields: fields{
				sendHeartbeatCallCounter: 0,
				retryCount:               0,
				failedCalls:              0,
				successfulCalls:          0,
			},
			setup: func(f *fields, client *ChronoQueueClient) {
				client.opts.SendMessageHeartbeatFunc = func(ctx context.Context, queueName, messageId string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
					f.sendHeartbeatCallCounter++
					return nil, errors.New("forced error")
				}
				client.opts.MaxHeartbeatRetryCount = 1
			},
			validate: func(t *testing.T, f *fields, client *ChronoQueueClient) {
				// Allow time for retries to occur
				// Note: This sleep time might need to be adjusted based on actual behavior
				time.Sleep(2 * time.Second)

				// Validate that the call counter reached max retries
				if f.sendHeartbeatCallCounter != client.opts.MaxHeartbeatRetryCount {
					t.Errorf("Unexpected number of retries. Got: %d, Expected: %d", f.sendHeartbeatCallCounter, client.opts.MaxHeartbeatRetryCount)
				}
			},
		},
		{
			name: "Context Cancellation",
			args: args{
				ctx:       context.TODO(), // To be canceled in validate
				queueName: "validQueue",
				messageId: "validMessageId",
			},
			fields: fields{
				sendHeartbeatCallCounter: 0,
				retryCount:               0,
				failedCalls:              0,
				successfulCalls:          0,
			},
			setup: func(f *fields, client *ChronoQueueClient) {
				// Setup a call counter and ensure it is reset
				client.opts.SendMessageHeartbeatFunc = func(ctx context.Context, queueName, messageId string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
					f.sendHeartbeatCallCounter++
					return &queueservice_pb.SendMessageHeartBeatResponse{}, nil
				}
			},
			validate: func(t *testing.T, f *fields, client *ChronoQueueClient) {
				// Allow for initial heartbeats
				time.Sleep(200 * time.Millisecond)

				// Cancel the context
				client.closeChan <- struct{}{}

				// Allow for any in-flight heartbeats to complete
				time.Sleep(200 * time.Millisecond)

				// Capture the call count after the context is cancelled
				finalCallCount := f.sendHeartbeatCallCounter

				// Allow more time to ensure no further heartbeats are sent
				time.Sleep(500 * time.Millisecond)

				// Validate that the call count did not increase after the context was cancelled
				if f.sendHeartbeatCallCounter != finalCallCount {
					t.Errorf("SendMessageHeartbeat was called after context cancellation. Final count: %d, Current count: %d", finalCallCount, f.sendHeartbeatCallCounter)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer),
			}
			client, err := NewChronoQueueClient("bufnet", opts)
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			// Setup client as per test case
			tt.setup(&tt.fields, client)

			// Use a separate goroutine as manageHeartbeats is blocking
			go client.manageHeartbeats(tt.args.ctx, tt.args.queueName, tt.args.messageId, tt.args.attemptID, tt.args.workerID)

			// Add a small sleep to allow for asynchronous operations to execute
			// Note: This might need to be adjusted based on actual behavior
			time.Sleep(100 * time.Millisecond) // Validate the scenario as per test case
			tt.validate(t, &tt.fields, client)
		})
	}
}

func TestChronoQueueClient_GetNextMessage(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx             context.Context
		queue           string
		leaseDuration   string
		enableHeartbeat bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.GetNextMessageResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:             context.Background(),
				queue:           "validQueue",
				leaseDuration:   "4s",
				enableHeartbeat: false,
			},
			want: &queueservice_pb.GetNextMessageResponse{
				Message: &message_pb.Message{
					MessageId: "test_message",
					Metadata:  &message_pb.Message_Metadata{},
				},
			},
			wantErr: false,
		},
		{
			name: "No Message Available",
			args: args{
				ctx:             context.Background(),
				queue:           "emptyQueue",
				leaseDuration:   "3s",
				enableHeartbeat: false,
			},
			want:    &queueservice_pb.GetNextMessageResponse{},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:             context.Background(),
				queue:           "",
				leaseDuration:   "2s",
				enableHeartbeat: false,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.GetNextMessage(tt.args.ctx, tt.args.queue, tt.args.leaseDuration, tt.args.enableHeartbeat)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.GetNextMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.GetNextMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_PeekQueueMessages(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx       context.Context
		queue     string
		limit     int32
		timeRange TimeRangeOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.PeekQueueMessagesResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:   context.Background(),
				queue: "validQueue",
				limit: 2,
				timeRange: TimeRangeOption{
					Min: 0,
					Max: 0,
				},
			},
			want: &queueservice_pb.PeekQueueMessagesResponse{
				Messages: []*message_pb.Message{
					{
						MessageId: "test_message",
						Metadata:  &message_pb.Message_Metadata{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "No Messages Available",
			args: args{
				ctx:   context.Background(),
				queue: "emptyQueue",
				limit: 2,
				timeRange: TimeRangeOption{
					Min: 0,
					Max: 0,
				},
			},
			want:    &queueservice_pb.PeekQueueMessagesResponse{},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:   context.Background(),
				queue: "",
				limit: 2,
				timeRange: TimeRangeOption{
					Min: 0,
					Max: 0,
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.PeekQueueMessages(tt.args.ctx, tt.args.queue, tt.args.limit, tt.args.timeRange)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.PeekQueueMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.PeekQueueMessages() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_GetQueueState(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx   context.Context
		queue string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.GetQueueStateResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:   context.Background(),
				queue: "validQueue",
			},
			want: &queueservice_pb.GetQueueStateResponse{
				StateCounts: map[string]int32{
					"INVISIBLE": 1,
					"PENDING":   0,
					"RUNNING":   0,
					"COMPLETED": 0,
					"CANCELED":  0,
					"ERRORED":   0,
				},
				EarliestDeadline: nil,
			},
			wantErr: false,
		},
		{
			name: "No Messages Available",
			args: args{
				ctx:   context.Background(),
				queue: "emptyQueue",
			},
			want: &queueservice_pb.GetQueueStateResponse{
				StateCounts: map[string]int32{
					"INVISIBLE": 0,
					"PENDING":   0,
					"RUNNING":   0,
					"COMPLETED": 0,
					"CANCELED":  0,
					"ERRORED":   0,
				},
				EarliestDeadline: nil,
			},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:   context.Background(),
				queue: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.GetQueueState(tt.args.ctx, tt.args.queue)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.GetQueueState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.GetQueueState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_RenewMessageLease(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx           context.Context
		queue         string
		messageId     string
		leaseDuration string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.RenewMessageLeaseResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:           context.Background(),
				queue:         "validQueue",
				messageId:     "validMessageId",
				leaseDuration: "30s",
			},
			want: &queueservice_pb.RenewMessageLeaseResponse{
				RemainingTime: &durationpb.Duration{Seconds: 30},
				State:         message_pb.Message_Metadata_RUNNING,
			},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:           context.Background(),
				queue:         "",
				messageId:     "validMessageId",
				leaseDuration: "30s",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.RenewMessageLease(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.leaseDuration)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.RenewMessageLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.RenewMessageLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_AcknowledgeMessage(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx       context.Context
		queue     string
		messageId string
		state     State
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.AcknowledgeMessageResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:       context.Background(),
				queue:     "validQueue",
				messageId: "validMessageId",
				state:     3,
			},
			want:    &queueservice_pb.AcknowledgeMessageResponse{Success: true},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:       context.Background(),
				queue:     "",
				messageId: "validMessageId",
				state:     3,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.AcknowledgeMessage(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.AcknowledgeMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.AcknowledgeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_AcknowledgeMessage_WithAttemptInfo(t *testing.T) {
	server := &mockChronoQueueServer{}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	queueservice_pb.RegisterQueueServiceServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	connector := func(address string, opts ClientOptions) (queueservice_pb.QueueServiceClient, *grpc.ClientConn, error) {
		dialer := func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}
		ctx := context.Background()
		//nolint:staticcheck // Using deprecated DialContext for test compatibility with bufconn
		conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return queueservice_pb.NewQueueServiceClient(conn), conn, nil
	}

	client, err := NewChronoQueueClient("bufnet", ClientOptions{Connector: connector})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	client.SetAttemptInfo("msg-1", "attempt-123", "worker-xyz")
	_, err = client.AcknowledgeMessage(context.Background(), "q1", "msg-1", 3)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if server.lastAckAttemptID != "attempt-123" {
		t.Fatalf("expected attempt id to propagate, got %s", server.lastAckAttemptID)
	}
	if server.lastAckWorkerID != "worker-xyz" {
		t.Fatalf("expected worker id to propagate, got %s", server.lastAckWorkerID)
	}
}

func TestChronoQueueClient_SendMessageHeartbeat(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx       context.Context
		queueName string
		messageId string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.SendMessageHeartBeatResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:       context.Background(),
				queueName: "validQueue",
				messageId: "validMessageId",
			},
			want:    &queueservice_pb.SendMessageHeartBeatResponse{},
			wantErr: false,
		},
		{
			name: "Error Case",
			args: args{
				ctx:       context.Background(),
				queueName: "",
				messageId: "validMessageId",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.SendMessageHeartbeat(tt.args.ctx, tt.args.queueName, tt.args.messageId)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.SendMessageHeartbeat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.SendMessageHeartbeat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_ListQueues(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	type args struct {
		ctx    context.Context
		prefix string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *queueservice_pb.ListQueuesResponse
		wantErr bool
	}{
		{
			name: "Success Case",
			args: args{
				ctx:    context.Background(),
				prefix: "",
			},
			want: &queueservice_pb.ListQueuesResponse{Queues: []*queue_pb.Queue{
				{
					Name:     "test_queue",
					Metadata: &queue_pb.QueueMetadata{},
				},
			}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			got, err := client.ListQueues(tt.args.ctx, tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.ListQueues() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("ChronoQueueClient.ListQueues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_Close(t *testing.T) {
	dialer := dialer()

	type fields struct{}
	tests := []struct {
		name    string
		fields  fields
		execute func(client *ChronoQueueClient) error // a function that tries to execute something with the client
		wantErr bool
	}{
		{
			name: "Successful Closure",
			execute: func(client *ChronoQueueClient) error {
				_, err := client.GetNextMessage(context.Background(), "testQueue", "2s", false)
				return err
			},
			wantErr: false,
		},
		{
			name: "Use After Closure",
			execute: func(client *ChronoQueueClient) error {
				client.Close()
				_, err := client.GetNextMessage(context.Background(), "testQueue", "2s", false)
				return err
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClientOptions{
				Connector: testConnector(dialer), // use testConnector with dialer
			}
			client, err := NewChronoQueueClient("bufnet", opts) // using "bufnet" as address, but it doesn't matter
			if err != nil {
				t.Fatalf("Failed to create client: %v", err)
			}
			defer client.Close()

			err = tt.execute(client)
			if (err != nil) != tt.wantErr {
				t.Errorf("Error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestChronoQueueClient_DLQMethods tests all DLQ operations
func TestChronoQueueClient_DLQMethods(t *testing.T) {
	opts := ClientOptions{
		Connector: testConnector(dialer()),
	}
	client, err := NewChronoQueueClient("bufnet", opts)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	dlqName := "test-dlq"
	messageId := "test-msg-1"
	targetQueue := "target-queue"

	// Test GetDLQMessages
	t.Run("GetDLQMessages", func(t *testing.T) {
		resp, err := client.GetDLQMessages(ctx, dlqName, 10)
		if err != nil {
			t.Fatalf("GetDLQMessages failed: %v", err)
		}
		if resp == nil {
			t.Fatal("Response is nil")
		}
		if len(resp.Messages) == 0 {
			t.Error("Expected messages, got empty slice")
		}
	})

	// Test RequeueFromDLQ
	t.Run("RequeueFromDLQ", func(t *testing.T) {
		resp, err := client.RequeueFromDLQ(ctx, dlqName, messageId, targetQueue)
		if err != nil {
			t.Fatalf("RequeueFromDLQ failed: %v", err)
		}
		if resp == nil {
			t.Fatal("Response is nil")
		}
		if !resp.Success {
			t.Error("Expected success=true, got false")
		}
	})

	// Test DeleteFromDLQ
	t.Run("DeleteFromDLQ", func(t *testing.T) {
		resp, err := client.DeleteFromDLQ(ctx, dlqName, messageId)
		if err != nil {
			t.Fatalf("DeleteFromDLQ failed: %v", err)
		}
		if resp == nil {
			t.Fatal("Response is nil")
		}
		if !resp.Success {
			t.Error("Expected success=true, got false")
		}
	})

	// Test PurgeDLQ
	t.Run("PurgeDLQ", func(t *testing.T) {
		resp, err := client.PurgeDLQ(ctx, dlqName)
		if err != nil {
			t.Fatalf("PurgeDLQ failed: %v", err)
		}
		if resp == nil {
			t.Fatal("Response is nil")
		}
		if !resp.Success {
			t.Error("Expected success=true, got false")
		}
	})

	// Test GetDLQStats
	t.Run("GetDLQStats", func(t *testing.T) {
		resp, err := client.GetDLQStats(ctx, dlqName)
		if err != nil {
			t.Fatalf("GetDLQStats failed: %v", err)
		}
		if resp == nil {
			t.Fatal("Response is nil")
		}
		if resp.Name != dlqName {
			t.Errorf("Expected name=%s, got %s", dlqName, resp.Name)
		}
		if resp.MessageCount != 5 {
			t.Errorf("Expected message count=5, got %d", resp.MessageCount)
		}
	})
}

func TestChronoQueueClient_CancelMessage(t *testing.T) {
	dialer := dialer()
	client, err := NewChronoQueueClient("bufnet", ClientOptions{
		Connector: testConnector(dialer),
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	tests := []struct {
		name      string
		queueName string
		messageID string
		reason    string
		wantErr   bool
	}{
		{
			name:      "successful cancel without reason",
			queueName: "test-queue",
			messageID: "msg-123",
			reason:    "",
			wantErr:   false,
		},
		{
			name:      "successful cancel with reason",
			queueName: "test-queue",
			messageID: "msg-456",
			reason:    "Order cancelled by user",
			wantErr:   false,
		},
		{
			name:      "empty queue name",
			queueName: "",
			messageID: "msg-123",
			reason:    "",
			wantErr:   true,
		},
		{
			name:      "empty message ID",
			queueName: "test-queue",
			messageID: "",
			reason:    "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.CancelMessage(context.Background(), tt.queueName, tt.messageID, tt.reason)
			if (err != nil) != tt.wantErr {
				t.Errorf("CancelMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !resp.GetSuccess() {
				t.Errorf("CancelMessage() expected success = true, got false")
			}
		})
	}
}
