package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	pb_queue "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	State int32

	QueueOptions struct {
		DequeueAttempts      int32  `json:"dequeueAttempts,omitempty"`
		ExclusivityKey       string `json:"exclusivityKey,omitempty"`
		InvisibilityDuration string `json:"invisibilityDuration"`
		LeaseDuration        string `json:"leaseDuration"`
		Type                 int32  `json:"type,omitempty"`
	}
	MessageOptions struct {
		Payload              Payload `json:"payload,omitempty"`
		AttemptsLeft         int32   `json:"attemptsLeft,omitempty"`
		InvisibilityDuration string  `json:"invisibilityDuration"`
		LeaseDuration        string  `json:"leaseDuration"`
		LeaseExpiry          int64   `json:"leaseExpiry,omitempty"`
		State                State   `json:"state,omitempty"`
		InvisibilityExpiry   int64   `json:"invisibilityExpiry,omitempty"`
		Priority             int64   `json:"Priority,omitempty"`
	}
	Payload struct {
		Metadata map[string]*structpb.Value `json:"metadata,omitempty"`
		Data     *structpb.Struct           `json:"data,omitempty"`
	}
	TimeRangeOption struct {
		Min int64 `json:"min,omitempty"`
		Max int64 `json:"max,omitempty"`
	}
	ScheduleOptions struct {
		Payload        Payload `json:"payload,omitempty"`
		State          State   `json:"state,omitempty"`
		CronSchedule   string  `json:"cronSchedule,omitempty"`
		QueueName      string  `json:"queueName,omitempty"`
		ExclusivityKey string  `json:"exclusivityKey,omitempty"`
		MaxMessages    int64   `json:"maxMessages,omitempty"`
		LeaseDuration  string  `json:"leaseDuration,omitempty"`
	}

	Connector func(address string, opts ClientOptions) (queueservice_pb.QueueServiceClient, *grpc.ClientConn, error)
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

const (
	defaultRPCTimeout             = 3 * time.Second
	defaultHeartbeatInterval      = time.Second
	defaultMaxHeartbeatRetryCount = 10
)

const (
	DefaultMaxRetries          = 5
	DefaultInitialBackoff      = 500 * time.Millisecond
	DefaultMaxBackoff          = 60 * time.Second
	DefaultMaxHeartBeatWorkers = 10
)

type ClientOptions struct {
	MaxRetries               int
	InitialBackoff           time.Duration
	MaxBackoff               time.Duration
	MaxHeartBeatWorkers      int
	DefaultRPCTimeout        time.Duration
	TLSCredentials           credentials.TransportCredentials // Define as per your gRPC setup
	Connector                Connector                        // User-provided Connector
	MaxHeartbeatRetryCount   int
	SendMessageHeartbeatFunc func(context.Context, string, string) (*queueservice_pb.SendMessageHeartBeatResponse, error)
}

type WorkItem struct {
	ctx       context.Context
	queue     string
	messageID string
}

// ChronoQueueClient is a client to call ChronoQueue RPC
type ChronoQueueClient struct {
	service   queueservice_pb.QueueServiceClient
	conn      *grpc.ClientConn
	workChan  chan WorkItem
	closeChan chan struct{}
	closed    bool
	mu        sync.Mutex
	opts      ClientOptions
}

// NewChronoQueueClient returns a new ChronoQueue client
func NewChronoQueueClient(address string, opts ClientOptions) (*ChronoQueueClient, error) {
	client := &ChronoQueueClient{
		closeChan: make(chan struct{}),
		closed:    false,
		mu:        sync.Mutex{},
		opts:      checkDefaultClientOptions(opts),
	}
	client.workChan = make(chan WorkItem, client.opts.MaxHeartBeatWorkers)

	// Use user-provided Connector if available, otherwise, use default
	var connector Connector
	if client.opts.Connector != nil {
		connector = client.opts.Connector
	} else {
		connector = DefaultServerConnector // Use default connector
	}

	service, conn, err := connector(address, opts)
	if err != nil {
		return nil, err
	}

	client.service = service
	client.conn = conn

	for i := 0; i < client.opts.MaxHeartBeatWorkers; i++ {
		go client.heartbeatWorker()
	}
	return client, nil
}

func checkDefaultClientOptions(opts ClientOptions) ClientOptions {
	if opts.MaxRetries == 0 {
		opts.MaxRetries = DefaultMaxRetries
	}
	if opts.InitialBackoff == 0 {
		opts.InitialBackoff = DefaultInitialBackoff
	}
	if opts.MaxBackoff == 0 {
		opts.MaxBackoff = DefaultMaxBackoff
	}
	if opts.TLSCredentials == nil {
		opts.TLSCredentials = nil // or an appropriate default TLS config
	}
	if opts.MaxHeartBeatWorkers == 0 {
		opts.MaxHeartBeatWorkers = DefaultMaxHeartBeatWorkers
	}
	if opts.DefaultRPCTimeout == 0 {
		opts.DefaultRPCTimeout = defaultRPCTimeout
	}
	if opts.MaxHeartbeatRetryCount == 0 {
		opts.MaxHeartbeatRetryCount = defaultMaxHeartbeatRetryCount
	}
	return opts
}

func DefaultServerConnector(address string, opts ClientOptions) (queueservice_pb.QueueServiceClient, *grpc.ClientConn, error) {
	backoff := opts.InitialBackoff
	for i := 0; i < opts.MaxRetries; i++ {
		// ...
		var conn *grpc.ClientConn
		var err error
		if opts.TLSCredentials != nil {
			conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(opts.TLSCredentials))
		} else {
			conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		if err == nil {
			return queueservice_pb.NewQueueServiceClient(conn), conn, nil
		}

		// Log or handle error, then retry
		log.Printf("Failed to connect to %s (attempt %d/%d): %v", address, i+1, opts.MaxRetries, err)

		// Sleep for backoff duration, then increase backoff, adding jitter
		time.Sleep(backoff + time.Duration(rand.Intn(100))*time.Millisecond)
		backoff *= 2
		if backoff > opts.MaxBackoff {
			backoff = opts.MaxBackoff
		}
	}
	return nil, nil, errors.New("max retry count reached. cannot connect to server")
}

func (client *ChronoQueueClient) heartbeatWorker() {
	for workItem := range client.workChan {
		// Perform work here, e.g., manage heartbeats
		client.manageHeartbeats(workItem.ctx, workItem.queue, workItem.messageID)
	}
}

func (client *ChronoQueueClient) setDefaultContextTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	_, ok := ctx.Deadline()
	if !ok {
		ctx, cancel := context.WithTimeout(ctx, client.opts.DefaultRPCTimeout)
		return ctx, cancel
	}
	return ctx, nil
}

func parseDurationToProto(durationStr string) (*durationpb.Duration, error) {
	if durationStr == "" {
		return nil, nil
	}
	parsedDuration, err := time.ParseDuration(durationStr)
	if err != nil {
		return nil, err
	}
	return durationpb.New(parsedDuration), nil
}

// Function to convert SIMPLE queue type string to enum
func ParseQueueType(queueType string) int32 {
	switch queueType {
	case "simple", "SIMPLE":
		return int32(pb_queue.QueueType_SIMPLE)
	case "exclusive", "EXCLUSIVE":
		return int32(pb_queue.QueueType_EXCLUSIVE)
	default:
		// Default to SIMPLE if unknown
		return int32(pb_queue.QueueType_SIMPLE)
	}
}

func ParseMessageState(state string) (State, error) {
	switch state {
	case "PENDING":
		return State(message_pb.Message_Metadata_PENDING), nil
	case "RUNNING":
		return State(message_pb.Message_Metadata_RUNNING), nil
	case "COMPLETED":
		return State(message_pb.Message_Metadata_COMPLETED), nil
	case "INVISIBLE":
		return State(message_pb.Message_Metadata_INVISIBLE), nil
	case "CANCELED":
		return State(message_pb.Message_Metadata_CANCELED), nil
	case "ERRORED":
		return State(message_pb.Message_Metadata_ERRORED), nil
	default:
		return 0, errors.New("invalid message state")
	}
}

// CreateQueue create a queue and returns empty response
func (client *ChronoQueueClient) CreateQueue(ctx context.Context, name string, queueOptions QueueOptions) (*queueservice_pb.CreateQueueResponse, error) {

	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	leaseDuration, err := parseDurationToProto(queueOptions.LeaseDuration)
	if err != nil {
		return &queueservice_pb.CreateQueueResponse{Success: false}, fmt.Errorf("invalid lease duration: %v", err)
	}
	invisibilityDuration, err := parseDurationToProto(queueOptions.InvisibilityDuration)
	if err != nil {
		return &queueservice_pb.CreateQueueResponse{Success: false}, fmt.Errorf("invalid invisibility duration: %v", err)
	}

	req := &queueservice_pb.CreateQueueRequest{
		Name: name,
		Metadata: &pb_queue.QueueMetadata{
			Type:                 pb_queue.QueueType(queueOptions.Type),
			DequeueAttempts:      int32(queueOptions.DequeueAttempts),
			LeaseDuration:        leaseDuration,
			ExclusivityKey:       queueOptions.ExclusivityKey,
			InvisibilityDuration: invisibilityDuration,
		},
	}
	res, err := client.service.CreateQueue(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// DeleteQueue deletes a queue and returns empty response
func (client *ChronoQueueClient) DeleteQueue(ctx context.Context, name string) (*queueservice_pb.DeleteQueueResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	req := &queueservice_pb.DeleteQueueRequest{Name: name}
	res, err := client.service.DeleteQueue(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// PostMessage create adds a message to the queue and returns empty response
func (client *ChronoQueueClient) PostMessage(ctx context.Context, queue string, messageId string, messageOptions MessageOptions) (*queueservice_pb.PostMessageResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	leaseDuration, err := parseDurationToProto(messageOptions.LeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid lease duration: %v", err)
	}
	invisibilityDuration, err := parseDurationToProto(messageOptions.InvisibilityDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid invisibility duration: %v", err)
	}

	req := &queueservice_pb.PostMessageRequest{
		QueueName: queue,
		Message: &message_pb.Message{
			MessageId: messageId,
			Metadata: &message_pb.Message_Metadata{
				Payload: &common_pb.Payload{
					Metadata: messageOptions.Payload.Metadata,
					Data:     messageOptions.Payload.Data,
				},
				AttemptsLeft:         messageOptions.AttemptsLeft,
				LeaseDuration:        leaseDuration,
				LeaseExpiry:          messageOptions.LeaseExpiry,
				InvisibilityDuration: invisibilityDuration,
				State:                message_pb.Message_Metadata_State(messageOptions.State),
				Priority:             messageOptions.Priority,
			},
		},
	}
	res, err := client.service.PostMessage(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (client *ChronoQueueClient) manageHeartbeats(ctx context.Context, queueName string, messageId string) {

	// Set up a ticker to send a heartbeat at regular intervals
	ticker := time.NewTicker(defaultHeartbeatInterval)
	defer ticker.Stop()

	retryCount := 0 // Initialize retry counter

	for {
		select {
		case <-ticker.C:

			if retryCount >= client.opts.MaxHeartbeatRetryCount {
				log.Printf("Max retry attempts reached for heartbeat of message: %s on queue: %s", messageId, queueName)
				return // TODO: Or handle according to use-case: log, metric, alert, etc.
			}

			ctx, cancel := client.setDefaultContextTimeout(ctx)
			defer cancel()

			_, err := client.SendMessageHeartbeat(ctx, queueName, messageId)
			if err != nil {
				log.Printf("Error occurred in manageHeartbeats: %v", err)

				// Increment retry counter
				retryCount++

				// Calculate exponential backoff time with jitter
				backoffTime := (1 << retryCount) + rand.Intn(1000) // 2^retryCount + [0, 1000)ms jitter
				log.Printf("Retrying in %d milliseconds...", backoffTime)

				// Wait for backoff time before the next retry
				time.Sleep(time.Duration(backoffTime) * time.Millisecond)

				continue // Skip to the next iteration of the loop
			}

			// Reset retry counter upon successful heartbeat
			retryCount = 0
		case <-client.closeChan:
			// client is closed, terminate the goroutine
			return
		case <-ctx.Done(): // Assuming client.ctx is a context that gets cancelled when processing is complete or an error occurs
			// Stop sending heartbeats if the message processing is complete or an error occurred
			return
		}
	}
}

// GetNextMessage returns next message on a queue
func (client *ChronoQueueClient) GetNextMessage(ctx context.Context, queue string, leaseDuration string, enableHeartbeat bool) (*queueservice_pb.GetNextMessageResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	leaseDurationpb, err := parseDurationToProto(leaseDuration)
	if err != nil {
		return nil, err
	}
	req := &queueservice_pb.GetNextMessageRequest{QueueName: queue, LeaseDuration: leaseDurationpb}
	res, err := client.service.GetNextMessage(ctx, req)
	if err != nil {
		return nil, err
	}

	if enableHeartbeat && res.Message != nil {
		client.workChan <- WorkItem{
			ctx:       ctx,
			queue:     queue,
			messageID: res.Message.MessageId,
		}
	}
	return res, nil
}

// PeekQueueMessages returns messages on a queue that are in pending state
func (client *ChronoQueueClient) PeekQueueMessages(ctx context.Context, queue string, limit int32, timeRange TimeRangeOption) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	var priorityRange queueservice_pb.PeekQueueMessagesRequest_PriorityRange
	priorityRangeBytes, err := json.Marshal(timeRange)
	if err != nil {
		return nil, err
	}
	err = protojson.Unmarshal(priorityRangeBytes, &priorityRange)
	if err != nil {
		log.Println("Failed to deserialize priorityRange - err: ", err)
		return nil, err
	}

	req := &queueservice_pb.PeekQueueMessagesRequest{QueueName: queue, PriorityRange: &priorityRange}
	res, err := client.service.PeekQueueMessages(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetQueueState returns state of a queue
func (client *ChronoQueueClient) GetQueueState(ctx context.Context, queue string) (*queueservice_pb.GetQueueStateResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	req := &queueservice_pb.GetQueueStateRequest{QueueName: queue}
	res, err := client.service.GetQueueState(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RenewMessageLease updates a message's lease duration and returns empty response
func (client *ChronoQueueClient) RenewMessageLease(ctx context.Context, queue string, messageId string, leaseDuration string) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	leaseDurationpb, err := parseDurationToProto(leaseDuration)
	if err != nil {
		return nil, err
	}
	req := &queueservice_pb.RenewMessageLeaseRequest{QueueName: queue, MessageId: messageId, LeaseDuration: leaseDurationpb}
	res, err := client.service.RenewMessageLease(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// AcknowledgeMessage updates state of a message and empty response
func (client *ChronoQueueClient) AcknowledgeMessage(ctx context.Context, queue string, messageId string, state State) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	req := &queueservice_pb.AcknowledgeMessageRequest{QueueName: queue, MessageId: messageId, State: message_pb.Message_Metadata_State(state)}
	res, err := client.service.AcknowledgeMessage(ctx, req)
	if err != nil {
		return res, err
	}
	return res, nil
}

// SendMessageHeartbeat sends a heartbeat for an in-flight message.
func (client *ChronoQueueClient) SendMessageHeartbeat(ctx context.Context, queueName string, messageId string) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	if client.opts.SendMessageHeartbeatFunc != nil {
		return client.opts.SendMessageHeartbeatFunc(ctx, queueName, messageId)
	}
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	req := &queueservice_pb.SendMessageHeartBeatRequest{
		QueueName: queueName,
		MessageId: messageId,
	}
	res, err := client.service.SendMessageHeartBeat(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ListQueues returns list of available queues.
func (client *ChronoQueueClient) ListQueues(ctx context.Context, prefix string) (*queueservice_pb.ListQueuesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	req := &queueservice_pb.ListQueuesRequest{
		Prefix: prefix,
	}
	res, err := client.service.ListQueues(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// CreateSchedule creates a schedule and returns an empty response
func (client *ChronoQueueClient) CreateSchedule(ctx context.Context, scheduleId string, scheduleOptions ScheduleOptions) (*queueservice_pb.CreateScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()

	leaseDurationpb, err := parseDurationToProto(scheduleOptions.LeaseDuration)
	if err != nil {
		return nil, err
	}
	req := &queueservice_pb.CreateScheduleRequest{
		Schedule: &schedule_pb.Schedule{
			ScheduleId: scheduleId,
			Metadata: &schedule_pb.Schedule_Metadata{
				Payload: &common_pb.Payload{
					Metadata: scheduleOptions.Payload.Metadata,
					Data:     scheduleOptions.Payload.Data,
				},
				State:          schedule_pb.Schedule_Metadata_State(scheduleOptions.State),
				CronSchedule:   scheduleOptions.CronSchedule,
				QueueName:      scheduleOptions.QueueName,
				ExclusivityKey: scheduleOptions.ExclusivityKey,
				HasMaxMessages: scheduleOptions.MaxMessages > 0,
				MaxMessages:    scheduleOptions.MaxMessages,
				LeaseDuration:  leaseDurationpb,
			},
		},
	}
	res, err := client.service.CreateSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DeleteSchedule deletes a schedule and returns an empty response
func (client *ChronoQueueClient) DeleteSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.DeleteScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.DeleteScheduleRequest{
		ScheduleId: scheduleId,
	}
	res, err := client.service.DeleteSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetSchedule returns a schedule
func (client *ChronoQueueClient) GetSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.GetScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.GetScheduleRequest{
		ScheduleId: scheduleId,
	}
	res, err := client.service.GetSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ListSchedules returns list of schedules
func (client *ChronoQueueClient) ListSchedules(ctx context.Context, prefix string) (*queueservice_pb.ListSchedulesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.ListSchedulesRequest{
		Prefix: prefix,
	}
	res, err := client.service.ListSchedules(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetScheduleHistory returns the history of a schedule
func (client *ChronoQueueClient) GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.GetScheduleHistoryRequest{
		ScheduleId: scheduleId,
		Limit:      limit,
	}
	res, err := client.service.GetScheduleHistory(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// PauseSchedule pauses a schedule
func (client *ChronoQueueClient) PauseSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.PauseScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.PauseScheduleRequest{
		ScheduleId: scheduleId,
	}
	res, err := client.service.PauseSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ResumeSchedule resumes a schedule
func (client *ChronoQueueClient) ResumeSchedule(ctx context.Context, scheduleId string) (*queueservice_pb.ResumeScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	defer cancel()
	req := &queueservice_pb.ResumeScheduleRequest{
		ScheduleId: scheduleId,
	}
	res, err := client.service.ResumeSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Close closes the client
func (client *ChronoQueueClient) Close() {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.closed {
		close(client.closeChan)
		close(client.workChan)
		client.conn.Close()
		client.closed = true
	}
}
