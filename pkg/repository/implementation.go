package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	"github.com/adrien19/chronoqueue/pkg/repository/postgres"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
	"github.com/adrien19/chronoqueue/pkg/repository/sql/background"
	"github.com/adrien19/chronoqueue/pkg/repository/sqlite"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

// BackendType represents the type of storage backend
type BackendType string

const (
	BackendSQLite   BackendType = "sqlite"
	BackendPostgres BackendType = "postgres"
)

// implementation is the concrete implementation of Storage interface
// It delegates to a backend storage implementation
type implementation struct {
	backend          BackendStorage
	schedulerService *background.SchedulerService
	reclaimService   *background.ReclaimService
	metricsReporter  *background.MetricsReporterService
	calendarService  *background.CalendarService
	cronService      *background.CronProcessorService
	cleanupService   *background.CleanupService
	calendarEngine   calendar.Engine
	cancelFunc       context.CancelFunc
}

// NewSQLiteStorage creates a new storage implementation using SQLite backend
func NewSQLiteStorage(ctx context.Context, config *sqlite.Config) (Storage, error) {
	storage, err := sqlite.NewStorage(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create sqlite storage: %w", err)
	}

	// Create context for background services
	bgCtx, cancel := context.WithCancel(context.Background())

	calendarEngine := calendar.NewDefaultEngine()

	// Start background services
	schedulerService := background.NewSchedulerService(storage.BaseSQL, 1*time.Second)
	go schedulerService.Start(bgCtx)
	config.Logger.Info("Starting SQLite scheduler service", "interval", "1s")

	reclaimService := background.NewReclaimService(storage, storage.BaseSQL, 5*time.Second)
	go reclaimService.Start(bgCtx)
	config.Logger.Info("Starting SQLite reclaim service", "interval", "5s")

	metricsReporter := background.NewMetricsReporterService(storage.BaseSQL, 30*time.Second)
	go metricsReporter.Start(bgCtx)
	config.Logger.Info("Starting SQLite metrics reporter", "interval", "30s")

	calendarService := background.NewCalendarService(storage.BaseSQL, calendarEngine, 1*time.Second)
	go calendarService.Start(bgCtx)
	config.Logger.Info("Starting SQLite calendar service", "interval", "1s")

	cronService := background.NewCronProcessorService(storage.BaseSQL, 1*time.Second)
	go cronService.Start(bgCtx)
	config.Logger.Info("Starting SQLite cron processor", "interval", "1s")

	cleanupService := background.NewCleanupService(storage.BaseSQL, 1*time.Hour)
	go cleanupService.Start(bgCtx)
	config.Logger.Info("Starting SQLite cleanup service", "interval", "1h")

	config.Logger.Info("SQLite background services started")

	return &implementation{
		backend:          storage,
		schedulerService: schedulerService,
		reclaimService:   reclaimService,
		metricsReporter:  metricsReporter,
		calendarService:  calendarService,
		cronService:      cronService,
		cleanupService:   cleanupService,
		calendarEngine:   calendarEngine,
		cancelFunc:       cancel,
	}, nil
}

// NewPostgresStorage creates a new storage implementation using PostgreSQL backend
func NewPostgresStorage(ctx context.Context, config *postgres.Config) (Storage, error) {
	storage, err := postgres.NewStorage(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create postgres storage: %w", err)
	}

	bgCtx, cancel := context.WithCancel(context.Background())

	calendarEngine := calendar.NewDefaultEngine()

	schedulerService := background.NewSchedulerService(storage.BaseSQL, 1*time.Second)
	go schedulerService.Start(bgCtx)
	config.Logger.Info("Starting Postgres scheduler service", "interval", "1s")

	reclaimService := background.NewReclaimService(storage, storage.BaseSQL, 5*time.Second)
	go reclaimService.Start(bgCtx)
	config.Logger.Info("Starting Postgres reclaim service", "interval", "5s")

	metricsReporter := background.NewMetricsReporterService(storage.BaseSQL, 30*time.Second)
	go metricsReporter.Start(bgCtx)
	config.Logger.Info("Starting Postgres metrics reporter", "interval", "30s")

	calendarService := background.NewCalendarService(storage.BaseSQL, calendarEngine, 1*time.Second)
	go calendarService.Start(bgCtx)
	config.Logger.Info("Starting Postgres calendar service", "interval", "1s")

	cronService := background.NewCronProcessorService(storage.BaseSQL, 1*time.Second)
	go cronService.Start(bgCtx)
	config.Logger.Info("Starting Postgres cron processor", "interval", "1s")

	cleanupService := background.NewCleanupService(storage.BaseSQL, 1*time.Hour)
	go cleanupService.Start(bgCtx)
	config.Logger.Info("Starting Postgres cleanup service", "interval", "1h")

	config.Logger.Info("Postgres background services started")

	return &implementation{
		backend:          storage,
		schedulerService: schedulerService,
		reclaimService:   reclaimService,
		metricsReporter:  metricsReporter,
		calendarService:  calendarService,
		cronService:      cronService,
		cleanupService:   cleanupService,
		calendarEngine:   calendarEngine,
		cancelFunc:       cancel,
	}, nil
}

// Close closes the storage and background services with graceful shutdown
func (impl *implementation) Close() error {
	// Signal background services to stop
	if impl.cancelFunc != nil {
		impl.cancelFunc()
	}

	// Wait for graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var errScheduler, errReclaim, errMetrics, errCalendar, errCron error

	if impl.schedulerService != nil {
		errScheduler = impl.schedulerService.StopGracefully(shutdownCtx)
	}
	if impl.reclaimService != nil {
		errReclaim = impl.reclaimService.StopGracefully(shutdownCtx)
	}
	if impl.metricsReporter != nil {
		errMetrics = impl.metricsReporter.StopGracefully(shutdownCtx)
	}
	if impl.calendarService != nil {
		errCalendar = impl.calendarService.StopGracefully(shutdownCtx)
	}
	if impl.cronService != nil {
		errCron = impl.cronService.StopGracefully(shutdownCtx)
	}

	// Close the appropriate backend
	var errBackend error
	if impl.backend != nil {
		errBackend = impl.backend.Close()
	}

	// Return combined errors
	if errScheduler != nil {
		return fmt.Errorf("scheduler shutdown failed: %w", errScheduler)
	}
	if errReclaim != nil {
		return fmt.Errorf("reclaim shutdown failed: %w", errReclaim)
	}
	if errMetrics != nil {
		return fmt.Errorf("metrics reporter shutdown failed: %w", errMetrics)
	}
	if errCalendar != nil {
		return fmt.Errorf("calendar shutdown failed: %w", errCalendar)
	}
	if errCron != nil {
		return fmt.Errorf("cron processor shutdown failed: %w", errCron)
	}
	if errBackend != nil {
		return fmt.Errorf("backend close failed: %w", errBackend)
	}

	return nil
}

// CreateQueue creates a new queue
func (impl *implementation) CreateQueue(ctx context.Context, request *queueservicepb.CreateQueueRequest) (*queueservicepb.CreateQueueResponse, error) {
	metadata := request.Metadata
	if metadata == nil {
		metadata = &queuepb.QueueMetadata{}
	}

	// Set default lease policy if not provided
	if metadata.LeasePolicy == nil {
		metadata.LeasePolicy = &commonpb.LeasePolicy{
			BaseLease:    durationpb.New(30 * time.Second),
			MaxExtension: durationpb.New(5 * time.Minute),
		}
	}

	queue := &queuepb.Queue{
		Name:     request.Name,
		Metadata: metadata,
	}

	if err := impl.backend.CreateQueue(ctx, queue); err != nil {
		return nil, err
	}

	// Update metrics
	metrics.IncrementQueuesTotal()

	return &queueservicepb.CreateQueueResponse{
		Success: true,
	}, nil
}

// GetQueueMetadata retrieves queue metadata
func (impl *implementation) GetQueueMetadata(ctx context.Context, queueName string) (*queuepb.QueueMetadata, error) {
	queue, err := impl.backend.GetQueue(ctx, queueName)
	if err != nil {
		return nil, err
	}
	return queue.Metadata, nil
}

// DeleteQueue deletes a queue
func (impl *implementation) DeleteQueue(ctx context.Context, request *queueservicepb.DeleteQueueRequest) (*queueservicepb.DeleteQueueResponse, error) {
	if err := impl.backend.DeleteQueue(ctx, request.Name); err != nil {
		return nil, err
	}

	// Update metrics
	metrics.DecrementQueuesTotal()

	return &queueservicepb.DeleteQueueResponse{
		Success: true,
	}, nil
}

// ListQueues lists all queues
func (impl *implementation) ListQueues(ctx context.Context, request *queueservicepb.ListQueuesRequest) (*queueservicepb.ListQueuesResponse, error) {
	queues, err := impl.backend.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	return &queueservicepb.ListQueuesResponse{
		Queues: queues,
	}, nil
}

// GetQueueState returns the current state of a queue
func (impl *implementation) GetQueueState(ctx context.Context, request *queueservicepb.GetQueueStateRequest) (*queueservicepb.GetQueueStateResponse, error) {
	if request == nil || request.GetQueueName() == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	baseSQL := impl.getSQLBackend()
	if baseSQL == nil {
		return nil, fmt.Errorf("queue state aggregation not supported for this backend")
	}

	qb := repositorysql.NewQueryBuilder(baseSQL.Dialect)
	counts := map[string]int32{
		"INVISIBLE": 0,
		"PENDING":   0,
		"RUNNING":   0,
		"COMPLETED": 0,
		"CANCELED":  0,
		"ERRORED":   0,
	}

	countQuery := qb.BuildCountByStateQuery()
	rows, err := baseSQL.DB.QueryContext(ctx, countQuery, request.GetQueueName())
	if err != nil {
		return nil, fmt.Errorf("count states: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var stateInt int32
		var count int64
		if err := rows.Scan(&stateInt, &count); err != nil {
			return nil, fmt.Errorf("scan state count: %w", err)
		}
		switch messagepb.Message_Metadata_State(stateInt) {
		case messagepb.Message_Metadata_INVISIBLE:
			counts["INVISIBLE"] = int32(count)
		case messagepb.Message_Metadata_PENDING:
			counts["PENDING"] = int32(count)
		case messagepb.Message_Metadata_RUNNING:
			counts["RUNNING"] = int32(count)
		case messagepb.Message_Metadata_COMPLETED:
			counts["COMPLETED"] = int32(count)
		case messagepb.Message_Metadata_CANCELED:
			counts["CANCELED"] = int32(count)
		case messagepb.Message_Metadata_ERRORED:
			counts["ERRORED"] = int32(count)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate state counts: %w", err)
	}

	earliestDeadline, err := impl.findEarliestDeadline(ctx, baseSQL, qb, request.GetQueueName())
	if err != nil {
		return nil, err
	}

	return &queueservicepb.GetQueueStateResponse{
		StateCounts:      counts,
		EarliestDeadline: earliestDeadline,
	}, nil
}

func (impl *implementation) getSQLBackend() *repositorysql.BaseSQL {
	switch b := impl.backend.(type) {
	case *postgres.Storage:
		return b.BaseSQL
	case *sqlite.Storage:
		return b.BaseSQL
	default:
		return nil
	}
}

func (impl *implementation) findEarliestDeadline(ctx context.Context, baseSQL *repositorysql.BaseSQL, qb *repositorysql.QueryBuilder, queueName string) (*timestamppb.Timestamp, error) {
	var earliestMs int64

	leaseQuery := qb.BuildFindEarliestDeadlineQuery()
	var leaseExpiry sql.NullInt64
	if err := baseSQL.DB.QueryRowContext(ctx, leaseQuery, queueName).Scan(&leaseExpiry); err != nil {
		return nil, fmt.Errorf("query earliest lease expiry: %w", err)
	}
	if leaseExpiry.Valid && leaseExpiry.Int64 > 0 {
		earliestMs = leaseExpiry.Int64
	}

	scheduledQuery := fmt.Sprintf(`
		SELECT MIN(scheduled_at)
		FROM cq_messages
		WHERE queue_name = %s
		  AND scheduled_at IS NOT NULL
		  AND state IN (%s, %s)
	`,
		baseSQL.Dialect.Placeholder(1),
		baseSQL.Dialect.Placeholder(2),
		baseSQL.Dialect.Placeholder(3),
	)

	var scheduledAt sql.NullInt64
	if err := baseSQL.DB.QueryRowContext(ctx, scheduledQuery, queueName, messagepb.Message_Metadata_PENDING, messagepb.Message_Metadata_INVISIBLE).Scan(&scheduledAt); err != nil {
		return nil, fmt.Errorf("query earliest schedule: %w", err)
	}
	if scheduledAt.Valid && scheduledAt.Int64 > 0 {
		if earliestMs == 0 || scheduledAt.Int64 < earliestMs {
			earliestMs = scheduledAt.Int64
		}
	}

	if earliestMs == 0 {
		return nil, nil
	}

	return timestamppb.New(time.UnixMilli(earliestMs)), nil
}

// CreateQueueMessage posts a message to a queue
func (impl *implementation) CreateQueueMessage(ctx context.Context, request *queueservicepb.PostMessageRequest, v validator.Validator) (*queueservicepb.PostMessageResponse, error) {
	if request == nil || request.GetMessage() == nil {
		return nil, fmt.Errorf("message is required")
	}

	queueName := request.GetQueueName()
	message := request.GetMessage()

	// Get queue metadata to inherit defaults
	queueMetadata, err := impl.backend.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get queue metadata: %w", err)
	}

	// Ensure message has metadata
	if message.Metadata == nil {
		message.Metadata = &messagepb.Message_Metadata{}
	}

	// Set state
	message.Metadata.State = messagepb.Message_Metadata_PENDING

	// Inherit max_attempts from queue if not set on message
	if message.Metadata.MaxAttempts == 0 {
		if queueMetadata.DefaultMaxAttempts > 0 {
			message.Metadata.MaxAttempts = queueMetadata.DefaultMaxAttempts
		} else {
			message.Metadata.MaxAttempts = 3 // default
		}
	}

	// Set attempts_left based on max_attempts
	if message.Metadata.AttemptsLeft == 0 {
		message.Metadata.AttemptsLeft = message.Metadata.MaxAttempts
	}

	// Set default priority if not set
	if message.Metadata.Priority == 0 {
		message.Metadata.Priority = 5
	}

	// Inherit lease_policy from queue if not set on message
	if message.Metadata.LeasePolicy == nil && queueMetadata.GetLeasePolicy() != nil {
		message.Metadata.LeasePolicy = queueMetadata.GetLeasePolicy()
	}

	if v != nil {
		validationResult := v.Validate(ctx, message)
		if !validationResult.Valid {
			errorDetails := "Message validation failed:"
			for _, valErr := range validationResult.Errors {
				errorDetails += fmt.Sprintf("\n  - %s: %s", valErr.Field, valErr.Message)
			}
			metrics.IncrementValidationFailures(queueName, "schema_mismatch")
			return nil, fmt.Errorf("%s", errorDetails)
		}
		metrics.IncrementMessagesValidated(queueName)
	}

	if err := impl.backend.EnqueueMessage(ctx, queueName, message); err != nil {
		return nil, err
	}

	return &queueservicepb.PostMessageResponse{
		Success: true,
	}, nil
}

// CreateQueueMessagesBulk posts multiple messages to a queue in a single operation
func (impl *implementation) CreateQueueMessagesBulk(ctx context.Context, request *queueservicepb.PostMessagesBulkRequest, v validator.Validator) (*queueservicepb.PostMessagesBulkResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("request is required")
	}

	queueName := request.GetQueueName()
	messages := request.GetMessages()
	transactionMode := request.GetTransactionMode()

	// Validate request limits
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages provided")
	}
	if len(messages) > 1000 {
		return nil, fmt.Errorf("too many messages: %d (max 1000)", len(messages))
	}

	// Get queue metadata to inherit defaults
	queueMetadata, err := impl.backend.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, fmt.Errorf("get queue metadata: %w", err)
	}

	// Pre-process and validate all messages
	validationErrors := make([]error, len(messages))
	for i, message := range messages {
		// Ensure message has metadata
		if message.Metadata == nil {
			message.Metadata = &messagepb.Message_Metadata{}
		}

		// Set state
		message.Metadata.State = messagepb.Message_Metadata_PENDING

		// Inherit max_attempts from queue if not set on message
		if message.Metadata.MaxAttempts == 0 {
			if queueMetadata.DefaultMaxAttempts > 0 {
				message.Metadata.MaxAttempts = queueMetadata.DefaultMaxAttempts
			} else {
				message.Metadata.MaxAttempts = 3 // default
			}
		}

		// Set attempts_left based on max_attempts
		if message.Metadata.AttemptsLeft == 0 {
			message.Metadata.AttemptsLeft = message.Metadata.MaxAttempts
		}

		// Set default priority if not set
		if message.Metadata.Priority == 0 {
			message.Metadata.Priority = 5
		}

		// Inherit lease_policy from queue if not set on message
		if message.Metadata.LeasePolicy == nil && queueMetadata.GetLeasePolicy() != nil {
			message.Metadata.LeasePolicy = queueMetadata.GetLeasePolicy()
		}

		// Schema validation
		if v != nil {
			validationResult := v.Validate(ctx, message)
			if !validationResult.Valid {
				metrics.IncrementValidationFailures(queueName, "schema_mismatch")

				// In ALL_OR_NOTHING mode, fail fast on first validation error
				if transactionMode == queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING {
					errorDetails := fmt.Sprintf("Message[%d] validation failed:", i)
					for _, valErr := range validationResult.Errors {
						errorDetails += fmt.Sprintf("\n  - %s: %s", valErr.Field, valErr.Message)
					}
					return nil, fmt.Errorf("%s", errorDetails)
				}
				// Record validation error for this message
				errorDetails := "validation failed:"
				for _, valErr := range validationResult.Errors {
					errorDetails += fmt.Sprintf(" %s: %s;", valErr.Field, valErr.Message)
				}
				validationErrors[i] = fmt.Errorf("%s", errorDetails)
			} else {
				metrics.IncrementMessagesValidated(queueName)
			}
		}
	}

	// Filter out messages that failed validation
	validMessages := make([]*messagepb.Message, 0, len(messages))
	validIndices := make([]int, 0, len(messages))
	for i, msg := range messages {
		if validationErrors[i] == nil {
			validMessages = append(validMessages, msg)
			validIndices = append(validIndices, i)
		}
	}

	// Enqueue valid messages using the backend's bulk operation
	var messageErrors []error
	var txErr error
	if len(validMessages) > 0 {
		messageErrors, txErr = impl.backend.EnqueueMessagesBulk(ctx, queueName, validMessages, int32(transactionMode))
	}

	if messageErrors == nil {
		messageErrors = make([]error, len(validMessages))
	}
	if len(messageErrors) != len(validMessages) {
		return nil, fmt.Errorf("internal error: message errors count mismatch")
	}

	// Build response with per-message results
	results := make([]*queueservicepb.PostMessagesBulkResponse_MessagePostResult, len(messages))
	successCount := int32(0)

	// First, populate results for messages that failed validation
	for i, message := range messages {
		if validationErrors[i] != nil {
			results[i] = &queueservicepb.PostMessagesBulkResponse_MessagePostResult{
				MessageId: message.MessageId,
				Success:   false,
				ErrorCode: queueservicepb.PostMessagesBulkResponse_MessagePostResult_VALIDATION_FAILED,
				Error:     validationErrors[i].Error(),
			}
		}
	}

	// Then, populate results for messages that were enqueued
	for j, origIdx := range validIndices {
		message := messages[origIdx]
		result := &queueservicepb.PostMessagesBulkResponse_MessagePostResult{
			MessageId: message.MessageId,
		}

		if messageErrors[j] != nil {
			result.Success = false
			result.ErrorCode = queueservicepb.PostMessagesBulkResponse_MessagePostResult_INTERNAL_ERROR
			result.Error = messageErrors[j].Error()

			if errors.Is(messageErrors[j], repositorycommon.ErrDuplicateMessageID) {
				result.ErrorCode = queueservicepb.PostMessagesBulkResponse_MessagePostResult_DUPLICATE_MESSAGE_ID
			}
		} else {
			result.Success = true
			result.ErrorCode = queueservicepb.PostMessagesBulkResponse_MessagePostResult_SUCCESS
			successCount++
		}

		results[origIdx] = result
	}

	response := &queueservicepb.PostMessagesBulkResponse{
		Success:         successCount > 0,
		SuccessfulCount: successCount,
		FailedCount:     int32(len(messages)) - successCount,
		Results:         results,
	}

	// For ALL_OR_NOTHING mode, if transaction failed, indicate total failure
	if txErr != nil && transactionMode == queueservicepb.PostMessagesBulkRequest_ALL_OR_NOTHING {
		return response, fmt.Errorf("transaction failed: %w", txErr)
	}
	if txErr != nil && transactionMode == queueservicepb.PostMessagesBulkRequest_BEST_EFFORT {
		if baseSQL := impl.getSQLBackend(); baseSQL != nil {
			baseSQL.Logger.WarnWithFields("bulk enqueue partial failure in BEST_EFFORT mode",
				"error", txErr.Error(), "queue", queueName)
		}
	}

	return response, nil
}

// GetQueueMessage retrieves the next available message from a queue
func (impl *implementation) GetQueueMessage(ctx context.Context, request *queueservicepb.GetNextMessageRequest) (*queueservicepb.GetNextMessageResponse, error) {
	workerId := ""
	if request.WorkerId != nil {
		workerId = *request.WorkerId
	}
	attemptId := ""
	if request.AttemptId != nil {
		attemptId = *request.AttemptId
	}

	message, err := impl.backend.ClaimMessage(ctx, request.QueueName, workerId, attemptId)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return &queueservicepb.GetNextMessageResponse{Message: nil}, nil
	}

	respAttemptId := ""
	respWorkerId := ""
	if ca := message.GetMetadata().GetCurrentAttempt(); ca != nil {
		respAttemptId = ca.GetAttemptId()
		respWorkerId = ca.GetWorkerId()
	}

	return &queueservicepb.GetNextMessageResponse{
		Message:   message,
		AttemptId: optionalString(respAttemptId),
		WorkerId:  optionalString(respWorkerId),
	}, nil
}

func optionalString(val string) *string {
	if val == "" {
		return nil
	}
	return &val
}

// AcknowledgeMessage marks a message as processed (either successfully completed or errored)
//
// Design Note: This method routes to different backend operations based on the request.State:
// - State = COMPLETED → backend.AcknowledgeMessage() (marks as successfully processed)
// - State = ERRORED   → backend.NackMessage() (marks as failed, triggers retry or DLQ)
//
// Why not expose NackMessage separately in the Storage interface?
//
//  1. Public API Alignment: The gRPC service (service.proto) only exposes a single
//     AcknowledgeMessage RPC. Both success and failure acknowledgments use the same endpoint
//     with different state values, providing a unified acknowledgement API.
//
//  2. Semantic Correctness: Acknowledging a message means "I'm done processing it"
//     regardless of whether processing succeeded or failed. The state parameter indicates
//     the outcome, not the intent.
//
// 3. Backend Separation: NackMessage exists at the BackendStorage level because:
//   - Background services (reclaim, cleanup) need direct access to mark failed messages
//   - Allows different side-effect handling for each state transition
//   - Keeps internal operations separate from public API operations
//
// Example client usage:
//
//	// Success case
//	AcknowledgeMessage(ctx, &AcknowledgeMessageRequest{
//	    QueueName: "orders",
//	    MessageId: "msg-123",
//	    State: COMPLETED,  // → routes to backend.AcknowledgeMessage()
//	})
//
//	// Failure case
//	AcknowledgeMessage(ctx, &AcknowledgeMessageRequest{
//	    QueueName: "orders",
//	    MessageId: "msg-123",
//	    State: ERRORED,  // → routes to backend.NackMessage()
//	})
func (impl *implementation) AcknowledgeMessage(ctx context.Context, request *queueservicepb.AcknowledgeMessageRequest) (*queueservicepb.AcknowledgeMessageResponse, error) {
	attemptId := ""
	if request.AttemptId != nil {
		attemptId = *request.AttemptId
	}

	// Route to appropriate backend method based on requested state
	switch request.State {
	case messagepb.Message_Metadata_COMPLETED:
		if err := impl.backend.AcknowledgeMessage(ctx, request.QueueName, request.MessageId, attemptId); err != nil {
			return nil, err
		}
	case messagepb.Message_Metadata_ERRORED:
		if err := impl.backend.NackMessage(ctx, request.QueueName, request.MessageId, attemptId); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid acknowledgment state: %v (must be COMPLETED or ERRORED)", request.State)
	}

	return &queueservicepb.AcknowledgeMessageResponse{
		Success: true,
	}, nil
}

// CancelMessage cancels a pending message before it has been processed.
//
// This is a producer-side operation for cancelling messages that haven't been processed yet.
// Only messages in INVISIBLE or PENDING state can be cancelled.
//
// Use cases:
// - Business logic invalidates the need for a message (order cancelled, user unsubscribed)
// - Scheduled messages no longer needed (event rescheduled)
// - Cleanup operations (purge old scheduled notifications)
//
// Effects:
// - Message moves to CANCELED state
// - Message removed from processing queue
// - Message retained based on retention policy (soft delete)
//
// Example:
//
//	CancelMessage(ctx, &CancelMessageRequest{
//	    QueueName: "notifications",
//	    MessageId: "reminder-456",
//	    Reason: "User unsubscribed",
//	})
func (impl *implementation) CancelMessage(ctx context.Context, request *queueservicepb.CancelMessageRequest) (*queueservicepb.CancelMessageResponse, error) {
	if request == nil || request.GetQueueName() == "" || request.GetMessageId() == "" {
		return nil, fmt.Errorf("queue name and message id are required")
	}
	reason := ""
	if request.Reason != nil {
		reason = *request.Reason
	}

	if err := impl.backend.CancelMessage(ctx, request.GetQueueName(), request.GetMessageId(), reason); err != nil {
		return nil, err
	}

	return &queueservicepb.CancelMessageResponse{
		Success: true,
	}, nil
}

// SendMessageHeartBeat updates the heartbeat for a message
func (impl *implementation) SendMessageHeartBeat(ctx context.Context, request *queueservicepb.SendMessageHeartBeatRequest) (*queueservicepb.SendMessageHeartBeatResponse, error) {
	attemptId := ""
	if request.AttemptId != nil {
		attemptId = *request.AttemptId
	}

	state, remainingTimeMs, err := impl.backend.HeartbeatMessage(ctx, request.QueueName, request.MessageId, attemptId)
	if err != nil {
		return nil, err
	}

	// Convert milliseconds to duration
	remainingDuration := time.Duration(remainingTimeMs) * time.Millisecond

	return &queueservicepb.SendMessageHeartBeatResponse{
		RemainingTime: durationpb.New(remainingDuration),
		State:         state,
	}, nil
}

// RenewMessageLease extends the lease on a message
func (impl *implementation) RenewMessageLease(ctx context.Context, request *queueservicepb.RenewMessageLeaseRequest) (*queueservicepb.RenewMessageLeaseResponse, error) {
	extensionMs := int64(0)
	if request.LeaseDuration != nil {
		extensionMs = request.LeaseDuration.AsDuration().Milliseconds()
	}

	if err := impl.backend.ExtendMessageLease(ctx, request.QueueName, request.MessageId, "", extensionMs); err != nil {
		return nil, err
	}

	// Return remaining time if lease duration was provided
	var remainingTime *durationpb.Duration
	if request.LeaseDuration != nil {
		remainingTime = request.LeaseDuration
	}

	return &queueservicepb.RenewMessageLeaseResponse{
		RemainingTime: remainingTime,
	}, nil
}

// PeekQueueMessages retrieves messages without claiming them
func (impl *implementation) PeekQueueMessages(ctx context.Context, request *queueservicepb.PeekQueueMessagesRequest) (*queueservicepb.PeekQueueMessagesResponse, error) {
	messages, err := impl.backend.PeekMessages(ctx, request.QueueName, int32(request.Limit))
	if err != nil {
		return nil, err
	}
	return &queueservicepb.PeekQueueMessagesResponse{
		Messages: messages,
	}, nil
}

// CreateSchedule creates a new schedule
func (impl *implementation) CreateSchedule(ctx context.Context, request *queueservicepb.CreateScheduleRequest) (*queueservicepb.CreateScheduleResponse, error) {
	if request.Schedule == nil {
		return nil, fmt.Errorf("schedule is required")
	}

	// Pre-compute next_run for calendar schedules so the background processor can pick them up.
	if meta := request.Schedule.GetMetadata(); meta != nil {
		if meta.GetCalendarSchedule() != nil && meta.GetNextRun() == nil {
			if err := impl.calendarEngine.ValidateSchedule(ctx, meta.GetCalendarSchedule()); err != nil {
				return nil, fmt.Errorf("validate calendar schedule: %w", err)
			}

			nextRun, err := impl.calendarEngine.CalculateNextRun(ctx, meta.GetCalendarSchedule(), time.Now())
			if err != nil {
				return nil, fmt.Errorf("calculate next run: %w", err)
			}

			if nextRun == nil {
				meta.State = schedulepb.Schedule_Metadata_PAUSED
				meta.StateMessage = "no future runs"
			} else {
				meta.NextRun = timestamppb.New(*nextRun)
			}
		}
	}

	if err := impl.backend.CreateSchedule(ctx, request.Schedule); err != nil {
		return nil, err
	}

	return &queueservicepb.CreateScheduleResponse{
		Success: true,
	}, nil
}

// DeleteSchedule deletes a schedule
func (impl *implementation) DeleteSchedule(ctx context.Context, request *queueservicepb.DeleteScheduleRequest) (*queueservicepb.DeleteScheduleResponse, error) {
	if err := impl.backend.DeleteSchedule(ctx, request.ScheduleId); err != nil {
		return nil, err
	}

	return &queueservicepb.DeleteScheduleResponse{
		Success: true,
	}, nil
}

// GetSchedule retrieves a schedule by ID
func (impl *implementation) GetSchedule(ctx context.Context, request *queueservicepb.GetScheduleRequest) (*queueservicepb.GetScheduleResponse, error) {
	schedule, err := impl.backend.GetSchedule(ctx, request.ScheduleId)
	if err != nil {
		return nil, err
	}
	return &queueservicepb.GetScheduleResponse{
		Schedule: schedule,
	}, nil
}

// ListSchedules lists schedules for a queue
func (impl *implementation) ListSchedules(ctx context.Context, request *queueservicepb.ListSchedulesRequest) (*queueservicepb.ListSchedulesResponse, error) {
	// TODO: The low-level storage expects queueName but the gRPC API uses prefix
	// For now, pass empty string to list all schedules
	schedules, err := impl.backend.ListSchedules(ctx, "")
	if err != nil {
		return nil, err
	}
	return &queueservicepb.ListSchedulesResponse{
		Schedules: schedules,
	}, nil
}

// GetScheduleHistory retrieves execution history for a schedule
func (impl *implementation) GetScheduleHistory(ctx context.Context, request *queueservicepb.GetScheduleHistoryRequest) (*queueservicepb.GetScheduleHistoryResponse, error) {
	history, err := impl.backend.GetScheduleHistory(ctx, request.ScheduleId, request.Limit)
	if err != nil {
		return nil, err
	}

	return &queueservicepb.GetScheduleHistoryResponse{
		ScheduleHistory: history,
	}, nil
}

// PauseSchedule pauses a schedule
func (impl *implementation) PauseSchedule(ctx context.Context, request *queueservicepb.PauseScheduleRequest) (*queueservicepb.PauseScheduleResponse, error) {
	if err := impl.backend.PauseSchedule(ctx, request.ScheduleId); err != nil {
		return nil, err
	}

	return &queueservicepb.PauseScheduleResponse{
		Success: true,
	}, nil
}

// ResumeSchedule resumes a paused schedule
func (impl *implementation) ResumeSchedule(ctx context.Context, request *queueservicepb.ResumeScheduleRequest) (*queueservicepb.ResumeScheduleResponse, error) {
	if err := impl.backend.ResumeSchedule(ctx, request.ScheduleId); err != nil {
		return nil, err
	}

	return &queueservicepb.ResumeScheduleResponse{
		Success: true,
	}, nil
}

// ValidateCalendarSchedule validates a calendar schedule
func (impl *implementation) ValidateCalendarSchedule(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule) error {
	if calendarSchedule == nil {
		return fmt.Errorf("calendar schedule is nil")
	}

	// Use the calendar engine to validate the schedule
	return impl.calendarEngine.ValidateSchedule(ctx, calendarSchedule)
}

// GetCalendarSchedulePreview generates preview of calendar schedule executions
func (impl *implementation) GetCalendarSchedulePreview(ctx context.Context, calendarSchedule *schedulepb.CalendarSchedule, count int) (*queueservicepb.PreviewCalendarScheduleResponse, error) {
	// Validate input
	if calendarSchedule == nil {
		return nil, fmt.Errorf("preview failed: calendar schedule cannot be nil")
	}

	// Generate preview from now
	now := time.Now()
	preview, err := impl.calendarEngine.PreviewSchedule(ctx, calendarSchedule, now, count)
	if err != nil {
		return nil, fmt.Errorf("failed to generate schedule preview: %w", err)
	}

	// Convert to protobuf response
	executionTimes := make([]*timestamppb.Timestamp, len(preview.ExecutionTimes))
	for i, et := range preview.ExecutionTimes {
		executionTimes[i] = timestamppb.New(et.Time)
	}

	return &queueservicepb.PreviewCalendarScheduleResponse{
		ExecutionTimes: executionTimes,
		Timezone:       preview.Timezone,
		PreviewStart:   timestamppb.New(now),
		TotalCount:     int32(len(executionTimes)),
	}, nil
}

// GetDLQMessages retrieves messages from the dead letter queue
func (impl *implementation) GetDLQMessages(ctx context.Context, dlqName string, limit int32) ([]*messagepb.Message, error) {
	return impl.backend.GetDLQMessages(ctx, dlqName, limit)
}

// RequeueFromDLQ moves a message from DLQ back to the original queue
func (impl *implementation) RequeueFromDLQ(ctx context.Context, dlqName string, messageID string, targetQueueName string, resetRetries bool) error {
	return impl.backend.RetryDLQMessage(ctx, dlqName, messageID)
}

// DeleteFromDLQ permanently deletes a message from DLQ
func (impl *implementation) DeleteFromDLQ(ctx context.Context, dlqName string, messageID string) error {
	return impl.backend.DeleteDLQMessage(ctx, dlqName, messageID)
}

// PurgeDLQ removes all messages from a DLQ
func (impl *implementation) PurgeDLQ(ctx context.Context, dlqName string) error {
	_, err := impl.backend.PurgeDLQ(ctx, dlqName)
	return err
}

// GetDLQStats returns statistics about a DLQ
func (impl *implementation) GetDLQStats(ctx context.Context, dlqName string) (*DLQStats, error) {
	baseSQL := impl.getSQLBackend()
	if baseSQL == nil {
		return nil, fmt.Errorf("DLQ stats not supported for this backend")
	}

	// Count ERRORED messages for this DLQ
	var messageCount int64
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM cq_messages WHERE queue_name = %s AND state = %s`,
		baseSQL.Dialect.Placeholder(1), baseSQL.Dialect.Placeholder(2))
	err := baseSQL.DB.QueryRowContext(ctx, countQuery, dlqName, int(messagepb.Message_Metadata_ERRORED)).Scan(&messageCount)
	if err != nil {
		return nil, fmt.Errorf("count DLQ messages: %w", err)
	}

	// Get queue metadata (created_at, updated_at)
	var createdAt, updatedAt int64
	metaQuery := fmt.Sprintf(`SELECT created_at, updated_at FROM cq_queues WHERE name = %s`,
		baseSQL.Dialect.Placeholder(1))
	err = baseSQL.DB.QueryRowContext(ctx, metaQuery, dlqName).Scan(&createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		// DLQ doesn't exist yet (no queue created)
		return &DLQStats{
			Name:         dlqName,
			MessageCount: 0,
			CreatedAt:    0,
			UpdatedAt:    0,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query DLQ metadata: %w", err)
	}

	return &DLQStats{
		Name:         dlqName,
		MessageCount: messageCount,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}, nil
}
