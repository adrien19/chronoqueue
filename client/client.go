package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	structpb "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	pb_queue "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

type (
	State int32

	QueueOptions struct {
		DequeueAttempts      int32  `json:"dequeueAttempts,omitempty"`
		ExclusivityKey       string `json:"exclusivityKey,omitempty"`
		InvisibilityDuration string `json:"invisibilityDuration"`
		LeaseDuration        string `json:"leaseDuration"`
		Type                 int32  `json:"type,omitempty"`
		DeadLetterQueueName  string `json:"deadLetterQueueName,omitempty"`
		AutoCreateDLQ        bool   `json:"autoCreateDLQ,omitempty"`
	}
	MessageOptions struct {
		Payload              Payload `json:"payload,omitempty"`
		AttemptsLeft         int32   `json:"attemptsLeft,omitempty"`
		MaxAttempts          int32   `json:"maxAttempts,omitempty"`
		InvisibilityDuration string  `json:"invisibilityDuration"`
		LeaseDuration        string  `json:"leaseDuration"`
		LeaseExpiry          int64   `json:"leaseExpiry,omitempty"`
		State                State   `json:"state,omitempty"`
		InvisibilityExpiry   int64   `json:"invisibilityExpiry,omitempty"`
		Priority             int64   `json:"Priority,omitempty"`
	}
	Payload struct {
		Metadata      map[string]*structpb.Value `json:"metadata,omitempty"`
		Data          *structpb.Struct           `json:"data,omitempty"`
		ContentType   string                     `json:"contentType,omitempty"`   // NEW: MIME type
		SchemaID      string                     `json:"schemaId,omitempty"`      // NEW: Schema reference
		SchemaVersion int32                      `json:"schemaVersion,omitempty"` // NEW: Schema version
	}
	TimeRangeOption struct {
		Min int64 `json:"min,omitempty"`
		Max int64 `json:"max,omitempty"`
	}
	ScheduleOptions struct {
		Payload          Payload                       `json:"payload,omitempty"`
		State            State                         `json:"state,omitempty"`
		CronSchedule     string                        `json:"cronSchedule,omitempty"`
		CalendarSchedule *schedule_pb.CalendarSchedule `json:"calendarSchedule,omitempty"` // New: for calendar-based scheduling
		QueueName        string                        `json:"queueName,omitempty"`
		ExclusivityKey   string                        `json:"exclusivityKey,omitempty"`
		MaxMessages      int64                         `json:"maxMessages,omitempty"`
		LeaseDuration    string                        `json:"leaseDuration,omitempty"`
	}

	// DLQStats represents statistics about a Dead Letter Queue
	DLQStats struct {
		Name         string `json:"name"`
		MessageCount int64  `json:"message_count"`
		CreatedAt    int64  `json:"created_at"`
		UpdatedAt    int64  `json:"updated_at"`
	}

	Connector func(address string, opts ClientOptions) (queueservice_pb.QueueServiceClient, *grpc.ClientConn, error)
)

const (
	// Message states match the proto enum values
	MESSAGE_INVISIBLE State = 0
	MESSAGE_PENDING   State = 1
	MESSAGE_RUNNING   State = 2
	MESSAGE_COMPLETED State = 3
	MESSAGE_CANCELED  State = 4
	MESSAGE_ERRORED   State = 5
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
	cancel    context.CancelFunc
	queue     string
	messageID string
}

// ChronoQueueClient is a client to call ChronoQueue RPC
type ChronoQueueClient struct {
	service          queueservice_pb.QueueServiceClient
	conn             *grpc.ClientConn
	workChan         chan WorkItem
	closeChan        chan struct{}
	closed           bool
	mu               sync.Mutex
	opts             ClientOptions
	activeHeartbeats sync.Map // messageID -> context.CancelFunc for explicit lifecycle management
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
			//nolint:staticcheck // Using Dial for immediate connection; will migrate to NewClient in future
			conn, err = grpc.Dial(address, grpc.WithTransportCredentials(opts.TLSCredentials))
		} else {
			//nolint:staticcheck // Using Dial for immediate connection; will migrate to NewClient in future
			conn, err = grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		if err == nil {
			// Connection successful, return the client
			service := queueservice_pb.NewQueueServiceClient(conn)
			return service, conn, nil
		}

		// Log the error and retry
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
	if cancel != nil {
		defer cancel()
	}

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
			DefaultMaxAttempts:   int32(queueOptions.DequeueAttempts),
			LeaseDuration:        leaseDuration,
			ExclusivityKey:       queueOptions.ExclusivityKey,
			InvisibilityDuration: invisibilityDuration,
			DeadLetterQueueName:  queueOptions.DeadLetterQueueName,
			AutoCreateDlq:        queueOptions.AutoCreateDLQ,
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
	if cancel != nil {
		defer cancel()
	}

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
	if cancel != nil {
		defer cancel()
	}

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
					Metadata:      messageOptions.Payload.Metadata,
					Data:          messageOptions.Payload.Data,
					ContentType:   messageOptions.Payload.ContentType,
					SchemaId:      messageOptions.Payload.SchemaID,
					SchemaVersion: messageOptions.Payload.SchemaVersion,
				},
				AttemptsLeft:         messageOptions.AttemptsLeft,
				MaxAttempts:          messageOptions.MaxAttempts,
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

	// Clean up when heartbeat stops
	defer func() {
		client.activeHeartbeats.Delete(messageId)
	}()

	retryCount := 0 // Initialize retry counter

	for {
		select {
		case <-ticker.C:

			if retryCount >= client.opts.MaxHeartbeatRetryCount {
				log.Printf("Max retry attempts reached for heartbeat of message: %s on queue: %s", messageId, queueName)
				return
			}

			// Use background context for RPC, not the heartbeat lifecycle context
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), client.opts.DefaultRPCTimeout)

			_, err := client.SendMessageHeartbeat(rpcCtx, queueName, messageId)
			rpcCancel() // Always cancel RPC context after use

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
		case <-ctx.Done():
			// Stop sending heartbeats when explicitly cancelled (via StopHeartbeat)
			return
		}
	}
}

// GetNextMessage returns next message on a queue
func (client *ChronoQueueClient) GetNextMessage(ctx context.Context, queue string, leaseDuration string, enableHeartbeat bool) (*queueservice_pb.GetNextMessageResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

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
		// Create independent context for heartbeat lifecycle (not tied to request context)
		heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())

		// Store cancel function for explicit cleanup via StopHeartbeat
		client.activeHeartbeats.Store(res.Message.MessageId, heartbeatCancel)

		client.workChan <- WorkItem{
			ctx:       heartbeatCtx,
			cancel:    heartbeatCancel,
			queue:     queue,
			messageID: res.Message.MessageId,
		}
	}
	return res, nil
}

// PeekQueueMessages returns messages on a queue that are in pending state
func (client *ChronoQueueClient) PeekQueueMessages(ctx context.Context, queue string, limit int32, timeRange TimeRangeOption) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

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
	if cancel != nil {
		defer cancel()
	}

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
	if cancel != nil {
		defer cancel()
	}

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
// Automatically stops heartbeat for the message if one is active
func (client *ChronoQueueClient) AcknowledgeMessage(ctx context.Context, queue string, messageId string, state State) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	// Stop heartbeat before acknowledging (if active)
	client.StopHeartbeat(messageId)

	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

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
	if cancel != nil {
		defer cancel()
	}

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

// StopHeartbeat explicitly stops the heartbeat for a specific message.
// This should be called when message processing completes (success or failure)
// to ensure the heartbeat goroutine terminates cleanly.
func (client *ChronoQueueClient) StopHeartbeat(messageID string) {
	if cancel, ok := client.activeHeartbeats.LoadAndDelete(messageID); ok {
		if cancelFunc, ok := cancel.(context.CancelFunc); ok {
			cancelFunc()
		}
	}
}

// ListQueues returns list of available queues.
func (client *ChronoQueueClient) ListQueues(ctx context.Context, prefix string) (*queueservice_pb.ListQueuesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

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
	if cancel != nil {
		defer cancel()
	}

	leaseDurationpb, err := parseDurationToProto(scheduleOptions.LeaseDuration)
	if err != nil {
		return nil, err
	}

	// Prepare schedule metadata
	metadata := &schedule_pb.Schedule_Metadata{
		Payload: &common_pb.Payload{
			Metadata: scheduleOptions.Payload.Metadata,
			Data:     scheduleOptions.Payload.Data,
		},
		State:          schedule_pb.Schedule_Metadata_State(scheduleOptions.State),
		QueueName:      scheduleOptions.QueueName,
		ExclusivityKey: scheduleOptions.ExclusivityKey,
		HasMaxMessages: scheduleOptions.MaxMessages > 0,
		MaxMessages:    scheduleOptions.MaxMessages,
		LeaseDuration:  leaseDurationpb,
	}

	// Set schedule config (cron or calendar)
	if scheduleOptions.CalendarSchedule != nil {
		metadata.ScheduleConfig = &schedule_pb.Schedule_Metadata_CalendarSchedule{
			CalendarSchedule: scheduleOptions.CalendarSchedule,
		}
	} else {
		metadata.ScheduleConfig = &schedule_pb.Schedule_Metadata_CronSchedule{
			CronSchedule: scheduleOptions.CronSchedule,
		}
	}

	req := &queueservice_pb.CreateScheduleRequest{
		Schedule: &schedule_pb.Schedule{
			ScheduleId: scheduleId,
			Metadata:   metadata,
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
	if cancel != nil {
		defer cancel()
	}
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
	if cancel != nil {
		defer cancel()
	}
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
	if cancel != nil {
		defer cancel()
	}
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
	if cancel != nil {
		defer cancel()
	}
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
	if cancel != nil {
		defer cancel()
	}
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
	if cancel != nil {
		defer cancel()
	}
	req := &queueservice_pb.ResumeScheduleRequest{
		ScheduleId: scheduleId,
	}
	res, err := client.service.ResumeSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ValidateCalendarSchedule validates a calendar schedule configuration
func (client *ChronoQueueClient) ValidateCalendarSchedule(ctx context.Context, calendarScheduleJSON string) (*queueservice_pb.ValidateCalendarScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	// Parse JSON into CalendarSchedule protobuf
	var calendarSchedule schedule_pb.CalendarSchedule
	if err := protojson.Unmarshal([]byte(calendarScheduleJSON), &calendarSchedule); err != nil {
		return nil, fmt.Errorf("failed to parse calendar schedule JSON: %w", err)
	}

	req := &queueservice_pb.ValidateCalendarScheduleRequest{
		CalendarSchedule: &calendarSchedule,
	}
	res, err := client.service.ValidateCalendarSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// PreviewCalendarSchedule previews execution times for a calendar schedule
func (client *ChronoQueueClient) PreviewCalendarSchedule(ctx context.Context, calendarScheduleJSON string, count int32) (*queueservice_pb.PreviewCalendarScheduleResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	// Parse JSON into CalendarSchedule protobuf
	var calendarSchedule schedule_pb.CalendarSchedule
	if err := protojson.Unmarshal([]byte(calendarScheduleJSON), &calendarSchedule); err != nil {
		return nil, fmt.Errorf("failed to parse calendar schedule JSON: %w", err)
	}

	req := &queueservice_pb.PreviewCalendarScheduleRequest{
		CalendarSchedule: &calendarSchedule,
		Count:            count,
	}
	res, err := client.service.PreviewCalendarSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Dead Letter Queue Management Methods

// GetDLQMessages retrieves messages from a Dead Letter Queue
func (client *ChronoQueueClient) GetDLQMessages(ctx context.Context, dlqName string, limit int32) (*queueservice_pb.GetDLQMessagesResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.GetDLQMessagesRequest{
		DlqName: dlqName,
		Limit:   limit,
	}
	res, err := client.service.GetDLQMessages(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RequeueFromDLQ moves a message from DLQ back to its original queue or specified target queue
func (client *ChronoQueueClient) RequeueFromDLQ(ctx context.Context, dlqName string, messageId string, targetQueue string) (*queueservice_pb.RequeueFromDLQResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.RequeueFromDLQRequest{
		DlqName:     dlqName,
		MessageId:   messageId,
		TargetQueue: targetQueue,
	}
	res, err := client.service.RequeueFromDLQ(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DeleteFromDLQ permanently deletes a message from a DLQ
func (client *ChronoQueueClient) DeleteFromDLQ(ctx context.Context, dlqName string, messageId string) (*queueservice_pb.DeleteFromDLQResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.DeleteFromDLQRequest{
		DlqName:   dlqName,
		MessageId: messageId,
	}
	res, err := client.service.DeleteFromDLQ(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// PurgeDLQ removes all messages from a DLQ
func (client *ChronoQueueClient) PurgeDLQ(ctx context.Context, dlqName string) (*queueservice_pb.PurgeDLQResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.PurgeDLQRequest{
		DlqName: dlqName,
	}
	res, err := client.service.PurgeDLQ(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetDLQStats returns statistics about a DLQ
func (client *ChronoQueueClient) GetDLQStats(ctx context.Context, dlqName string) (*queueservice_pb.GetDLQStatsResponse, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.GetDLQStatsRequest{
		DlqName: dlqName,
	}
	res, err := client.service.GetDLQStats(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Schema Management Methods

// SchemaOptions contains options for schema registration
type SchemaOptions struct {
	Name        string            // Human-readable schema name
	Description string            // Schema description
	Content     string            // JSON Schema content
	ContentType string            // Schema type (default: "json-schema")
	Metadata    map[string]string // Additional metadata
}

// RegisterSchema registers a new schema or creates a new version of an existing schema
// This is a client-side implementation that will work once server-side methods are added
func (client *ChronoQueueClient) RegisterSchema(ctx context.Context, schemaID string, options SchemaOptions) error {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	if options.ContentType == "" {
		options.ContentType = "json-schema"
	}

	req := &queueservice_pb.RegisterSchemaRequest{
		SchemaId:    schemaID,
		Name:        options.Name,
		Description: options.Description,
		Content:     options.Content,
		ContentType: options.ContentType,
		Metadata:    options.Metadata,
	}
	res, err := client.service.RegisterSchema(ctx, req)
	if err != nil {
		return err
	}
	log.Printf("Schema registered successfully: %s, version: %d", res.GetSchemaId(), res.GetVersion())
	return nil
}

// GetSchema retrieves a schema by ID and optional version
// version = 0 means get the latest version
func (client *ChronoQueueClient) GetSchema(ctx context.Context, schemaID string, version int32) (map[string]interface{}, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.GetSchemaRequest{
		SchemaId: schemaID,
		Version:  version,
	}
	res, err := client.service.GetSchema(ctx, req)
	if err != nil {
		return nil, err
	}
	if res.Schema == nil {
		return nil, fmt.Errorf("schema not found: %s", schemaID)
	}

	// Convert to map for easier handling
	schema := map[string]interface{}{
		"schema_id":    res.Schema.SchemaId,
		"version":      res.Schema.Version,
		"name":         res.Schema.Name,
		"description":  res.Schema.Description,
		"content":      res.Schema.Content,
		"content_type": res.Schema.ContentType,
		"created_at":   res.Schema.CreatedAt,
		"updated_at":   res.Schema.UpdatedAt,
		"is_active":    res.Schema.IsActive,
		"metadata":     res.Schema.Metadata,
	}
	return schema, nil
}

// ListSchemas returns all schemas matching the criteria
func (client *ChronoQueueClient) ListSchemas(ctx context.Context, prefix string, limit int32, activeOnly bool) ([]map[string]interface{}, error) {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.ListSchemasRequest{
		Prefix:     prefix,
		Limit:      limit,
		ActiveOnly: activeOnly,
	}
	res, err := client.service.ListSchemas(ctx, req)
	if err != nil {
		return nil, err
	}

	schemas := make([]map[string]interface{}, len(res.Schemas))
	for i, schema := range res.Schemas {
		schemas[i] = map[string]interface{}{
			"schema_id":   schema.GetSchemaId(),
			"name":        schema.GetName(),
			"version":     schema.GetLatestVersion(),
			"versions":    schema.GetVersionCount(),
			"description": schema.GetDescription(),
			// "content_type": schema.GetContentType(),
			"created_at": schema.GetCreatedAt(),
			"is_active":  schema.GetIsActive(),
		}
	}
	return schemas, nil
}

// DeleteSchema removes a schema version or all versions
// version = 0 means delete all versions
func (client *ChronoQueueClient) DeleteSchema(ctx context.Context, schemaID string, version int32) error {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.DeleteSchemaRequest{
		SchemaId: schemaID,
		Version:  version,
	}
	res, err := client.service.DeleteSchema(ctx, req)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("failed to delete schema: %s", schemaID)
	}

	log.Printf("Schema deleted successfully: %s, version: %d", schemaID, version)
	return nil
}

// ValidatePayload validates a payload against a schema
func (client *ChronoQueueClient) ValidatePayload(ctx context.Context, schemaID string, version int32, payloadJSON string) error {
	ctx, cancel := client.setDefaultContextTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	req := &queueservice_pb.ValidatePayloadRequest{
		SchemaId: schemaID,
		Version:  version,
		Payload:  payloadJSON,
	}
	res, err := client.service.ValidatePayload(ctx, req)
	if err != nil {
		return err
	}
	if res != nil && !res.Valid {
		var errMsgs []string
		for _, valErr := range res.Errors {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %s", valErr.Field, valErr.Message))
		}
		return fmt.Errorf("validation failed:\n  - %s", strings.Join(errMsgs, "\n  - "))
	}
	return nil
}

// Close closes the client
func (client *ChronoQueueClient) Close() {
	client.mu.Lock()
	defer client.mu.Unlock()

	if !client.closed {
		close(client.closeChan)
		close(client.workChan)
		_ = client.conn.Close() // Best-effort close
		client.closed = true
	}
}
