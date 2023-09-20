package client

import (
	"context"
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

type mockChronoQueueServer struct {
	pb_chronoqueue.UnimplementedChronoQueueServer
}

func dialer() func(context.Context, string) (net.Conn, error) {
	listener := bufconn.Listen(1024 * 1024)

	server := grpc.NewServer()

	pb_chronoqueue.RegisterChronoQueueServer(server, &mockChronoQueueServer{})

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func (*mockChronoQueueServer) CreateQueue(ctx context.Context, req *pb_chronoqueue.CreateQueueRequest) (*pb_chronoqueue.CreateQueueResponse, error) {
	if req.Queue.GetName() == "" {
		return &pb_chronoqueue.CreateQueueResponse{}, status.Errorf(codes.InvalidArgument, "cannot create queue with no name %v", req.Queue)
	}
	return &pb_chronoqueue.CreateQueueResponse{}, nil
}

func (*mockChronoQueueServer) GetNextMessage(ctx context.Context, req *pb_chronoqueue.GetNextMessageRequest) (*pb_chronoqueue.GetNextMessageResponse, error) {
	if req.GetQueueName() == "" {
		return &pb_chronoqueue.GetNextMessageResponse{}, status.Errorf(codes.InvalidArgument, "cannot query queue with no name %v", req)
	}
	return &pb_chronoqueue.GetNextMessageResponse{
		Message: &pb_chronoqueue.Message{
			MessageId: "test_message",
			Metadata:  &pb_chronoqueue.Message_Metadata{},
		},
	}, nil
}

func TestNewChronoQueueClient(t *testing.T) {
	type args struct {
		conn *grpc.ClientConn
	}
	tests := []struct {
		name string
		args args
		want *ChronoQueueClient
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewChronoQueueClient(tt.args.conn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewChronoQueueClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_CreateQueue(t *testing.T) {
	type args struct {
		name         string
		queueOptions QueueOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *pb_chronoqueue.CreateQueueResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "Test successful queue creation",
			args: args{
				name:         "my_queue",
				queueOptions: QueueOptions{},
			},
			want:    &pb_chronoqueue.CreateQueueResponse{},
			wantErr: false,
		},
	}

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := &ChronoQueueClient{
		service: pb_chronoqueue.NewChronoQueueClient(conn),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.CreateQueue(tt.args.name, tt.args.queueOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.CreateQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.String(), tt.want.String()) {
				t.Errorf("ChronoQueueClient.CreateQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_DeleteQueue(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.DeleteQueueResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.DeleteQueue(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.DeleteQueue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.DeleteQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_PostMessage(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		queue          string
		messageId      string
		messageOptions MessageOptions
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.PostMessageResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.PostMessage(tt.args.queue, tt.args.messageId, tt.args.messageOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.PostMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.PostMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_GetNextMessage(t *testing.T) {

	type args struct {
		queue         string
		leaseDuration *durationpb.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    *pb_chronoqueue.GetNextMessageResponse
		wantErr bool
	}{
		// TODO: Add more test cases.
		{
			name: "test successful get next message from a queue",
			args: args{
				queue:         "test_queue",
				leaseDuration: durationpb.New(time.Minute),
			},
			want: &pb_chronoqueue.GetNextMessageResponse{
				Message: &pb_chronoqueue.Message{
					MessageId: "test_message",
					Metadata:  &pb_chronoqueue.Message_Metadata{},
				},
			},
			wantErr: false,
		},
	}

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := &ChronoQueueClient{
		service: pb_chronoqueue.NewChronoQueueClient(conn),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetNextMessage(tt.args.queue, tt.args.leaseDuration)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.GetNextMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.GetMessage(), tt.want.GetMessage()) {
				t.Errorf("ChronoQueueClient.GetNextMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_PeekQueueMessages(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		queue     string
		limit     int32
		timeRange TimeRangeOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.PeekQueueMessagesResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.PeekQueueMessages(tt.args.queue, tt.args.limit, tt.args.timeRange)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.PeekQueueMessages() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.PeekQueueMessages() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_GetQueueState(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		queue string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.GetQueueStateResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.GetQueueState(tt.args.queue)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.GetQueueState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.GetQueueState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_RenewMessageLease(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		queue         string
		messageId     string
		leaseDuration *durationpb.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.RenewMessageLeaseResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.RenewMessageLease(tt.args.queue, tt.args.messageId, tt.args.leaseDuration)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.RenewMessageLease() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.RenewMessageLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_AcknowledgeMessage(t *testing.T) {
	type fields struct {
		service pb_chronoqueue.ChronoQueueClient
	}
	type args struct {
		queue     string
		messageId string
		state     State
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.AcknowledgeMessageResponse
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &ChronoQueueClient{
				service: tt.fields.service,
			}
			got, err := client.AcknowledgeMessage(tt.args.queue, tt.args.messageId, tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.AcknowledgeMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.AcknowledgeMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}
