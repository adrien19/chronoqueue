package client

import (
	"context"
	"log"
	"net"
	"reflect"
	"testing"

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

func testConnector(dialer func(context.Context, string) (net.Conn, error)) Connector {
	return func(address string, opts ClientOptions) (pb_chronoqueue.ChronoQueueClient, *grpc.ClientConn, error) {
		ctx := context.Background()
		conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		client := pb_chronoqueue.NewChronoQueueClient(conn)
		return client, conn, nil
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
			got, err := NewChronoQueueClient(tt.args.address, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewChronoQueueClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewChronoQueueClient() = %v, want %v", got, tt.want)
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
		// TODO: Add test cases.
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
		want    pb_chronoqueue.ChronoQueueClient
		want1   *grpc.ClientConn
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := DefaultServerConnector(tt.args.address, tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("DefaultServerConnector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultServerConnector() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DefaultServerConnector() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestChronoQueueClient_heartbeatWorker(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
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
		service   pb_chronoqueue.ChronoQueueClient
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
			got, got1 := client.setDefaultContextTimeout(tt.args.ctx)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.setDefaultContextTimeout() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ChronoQueueClient.setDefaultContextTimeout() got1 = %v, want %v", got1, tt.want1)
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
		// TODO: Add test cases.
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
	dialer := dialer() // assuming dialer() is defined as in your previous code

	type args struct {
		ctx          context.Context
		name         string
		queueOptions QueueOptions
	}
	tests := []struct {
		name    string
		args    args
		want    *pb_chronoqueue.CreateQueueResponse
		wantErr bool
	}{
		{
			name: "Successful Queue Creation",
			args: args{
				ctx:  context.Background(),
				name: "validQueueName",
				queueOptions: QueueOptions{
					LeaseDuration:        "15s",
					InvisibilityDuration: "10s",
				},
			},
			want:    &pb_chronoqueue.CreateQueueResponse{},
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.CreateQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_DeleteQueue(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx  context.Context
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
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			got, err := client.DeleteQueue(tt.args.ctx, tt.args.name)
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
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx            context.Context
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
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			got, err := client.PostMessage(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.messageOptions)
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

func TestChronoQueueClient_manageHeartbeats(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx       context.Context
		queueName string
		messageId string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
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
			client.manageHeartbeats(tt.args.ctx, tt.args.queueName, tt.args.messageId)
		})
	}
}

func TestChronoQueueClient_GetNextMessage(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx             context.Context
		queue           string
		leaseDuration   *durationpb.Duration
		enableHeartbeat bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.GetNextMessageResponse
		wantErr bool
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
			got, err := client.GetNextMessage(tt.args.ctx, tt.args.queue, tt.args.leaseDuration, tt.args.enableHeartbeat)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.GetNextMessage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.GetNextMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_PeekQueueMessages(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
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
		want    *pb_chronoqueue.PeekQueueMessagesResponse
		wantErr bool
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
			got, err := client.PeekQueueMessages(tt.args.ctx, tt.args.queue, tt.args.limit, tt.args.timeRange)
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
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx   context.Context
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
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			got, err := client.GetQueueState(tt.args.ctx, tt.args.queue)
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
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx           context.Context
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
				service:   tt.fields.service,
				conn:      tt.fields.conn,
				workChan:  tt.fields.workChan,
				closeChan: tt.fields.closeChan,
				opts:      tt.fields.opts,
			}
			got, err := client.RenewMessageLease(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.leaseDuration)
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
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
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
		want    *pb_chronoqueue.AcknowledgeMessageResponse
		wantErr bool
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
			got, err := client.AcknowledgeMessage(tt.args.ctx, tt.args.queue, tt.args.messageId, tt.args.state)
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

func TestChronoQueueClient_SendMessageHeartbeat(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
		conn      *grpc.ClientConn
		workChan  chan WorkItem
		closeChan chan struct{}
		opts      ClientOptions
	}
	type args struct {
		ctx       context.Context
		queueName string
		messageId string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pb_chronoqueue.SendMessageHeartBeatResponse
		wantErr bool
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
			got, err := client.SendMessageHeartbeat(tt.args.ctx, tt.args.queueName, tt.args.messageId)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChronoQueueClient.SendMessageHeartbeat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChronoQueueClient.SendMessageHeartbeat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChronoQueueClient_Close(t *testing.T) {
	type fields struct {
		service   pb_chronoqueue.ChronoQueueClient
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
			client.Close()
		})
	}
}
