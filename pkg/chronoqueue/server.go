package chronoqueue

import (
	"context"
	"fmt"

	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schema_pb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository"
	"github.com/adrien19/chronoqueue/pkg/schema"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

// ChronoQueueServer implements the gRPC QueueService interface directly
// using the storage layer without intermediate service abstractions
type ChronoQueueServer struct {
	storage        repository.Storage
	logger         *log.Logger
	schemaRegistry schema.Registry
	queueservice_pb.UnimplementedQueueServiceServer
}

// NewChronoQueueServer creates a new gRPC server instance
func NewChronoQueueServer(storage repository.Storage, schemaRegistry schema.Registry, logger *log.Logger) *ChronoQueueServer {
	return &ChronoQueueServer{
		storage:        storage,
		logger:         logger,
		schemaRegistry: schemaRegistry,
	}
}

// Queue Management Methods

func (s *ChronoQueueServer) CreateQueue(ctx context.Context, req *queueservice_pb.CreateQueueRequest) (*queueservice_pb.CreateQueueResponse, error) {
	s.logger.InfoWithFields("CreateQueue called", "queue_name", req.GetName())

	resp, err := s.storage.CreateQueue(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to create queue", "queue_name", req.GetName(), "error", err)
		return nil, fmt.Errorf("failed to create queue: %w", err)
	}

	s.logger.InfoWithFields("Queue created successfully", "queue_name", req.GetName())
	return resp, nil
}

func (s *ChronoQueueServer) DeleteQueue(ctx context.Context, req *queueservice_pb.DeleteQueueRequest) (*queueservice_pb.DeleteQueueResponse, error) {
	s.logger.InfoWithFields("DeleteQueue called", "queue_name", req.GetName())

	resp, err := s.storage.DeleteQueue(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to delete queue", "queue_name", req.GetName(), "error", err)
		return nil, fmt.Errorf("failed to delete queue: %w", err)
	}

	s.logger.InfoWithFields("Queue deleted successfully", "queue_name", req.GetName())
	return resp, nil
}

func (s *ChronoQueueServer) ListQueues(ctx context.Context, req *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	s.logger.Info("ListQueues called")

	resp, err := s.storage.ListQueues(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to list queues", "error", err)
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) GetQueueState(ctx context.Context, req *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	s.logger.InfoWithFields("GetQueueState called", "queue_name", req.GetQueueName())

	resp, err := s.storage.GetQueueState(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to get queue state", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to get queue state: %w", err)
	}

	return resp, nil
}

// Message Operations

func (s *ChronoQueueServer) PostMessage(ctx context.Context, req *queueservice_pb.PostMessageRequest) (*queueservice_pb.PostMessageResponse, error) {
	s.logger.InfoWithFields("PostMessage called", "queue_name", req.GetQueueName())

	queueMeta, err := s.storage.GetQueueMetadata(ctx, req.GetQueueName())
	if err != nil {
		s.logger.ErrorWithFields("Failed to get queue metadata", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	validator := validator.NewPayloadValidator(queueMeta, s.schemaRegistry)

	resp, err := s.storage.CreateQueueMessage(ctx, req, validator)
	if err != nil {
		s.logger.ErrorWithFields("Failed to post message", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to post message: %w", err)
	}

	s.logger.InfoWithFields("Message posted successfully", "queue_name", req.GetQueueName())
	return resp, nil
}

func (s *ChronoQueueServer) PostMessagesBulk(ctx context.Context, req *queueservice_pb.PostMessagesBulkRequest) (*queueservice_pb.PostMessagesBulkResponse, error) {
	s.logger.InfoWithFields("PostMessagesBulk called",
		"queue_name", req.GetQueueName(),
		"message_count", len(req.GetMessages()),
		"transaction_mode", req.GetTransactionMode().String())

	// Validate queue exists
	queueMeta, err := s.storage.GetQueueMetadata(ctx, req.GetQueueName())
	if err != nil {
		s.logger.ErrorWithFields("Failed to get queue metadata",
			"queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to get queue metadata: %w", err)
	}

	// Create validator with schema registry
	validator := validator.NewPayloadValidator(queueMeta, s.schemaRegistry)

	// Process bulk message posting
	resp, err := s.storage.CreateQueueMessagesBulk(ctx, req, validator)
	if err != nil {
		s.logger.ErrorWithFields("Failed to post messages in bulk",
			"queue_name", req.GetQueueName(),
			"message_count", len(req.GetMessages()),
			"error", err)
		return nil, fmt.Errorf("failed to post messages in bulk: %w", err)
	}

	s.logger.InfoWithFields("Messages posted successfully",
		"queue_name", req.GetQueueName(),
		"successful", resp.SuccessfulCount,
		"failed", resp.FailedCount)

	return resp, nil
}

func (s *ChronoQueueServer) GetNextMessage(ctx context.Context, req *queueservice_pb.GetNextMessageRequest) (*queueservice_pb.GetNextMessageResponse, error) {
	s.logger.InfoWithFields("GetNextMessage called", "queue_name", req.GetQueueName())

	resp, err := s.storage.GetQueueMessage(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to get next message", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to get next message: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) AcknowledgeMessage(ctx context.Context, req *queueservice_pb.AcknowledgeMessageRequest) (*queueservice_pb.AcknowledgeMessageResponse, error) {
	s.logger.InfoWithFields("AcknowledgeMessage called",
		"queue_name", req.GetQueueName(),
		"message_id", req.GetMessageId())

	resp, err := s.storage.AcknowledgeMessage(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to acknowledge message",
			"queue_name", req.GetQueueName(),
			"message_id", req.GetMessageId(),
			"error", err)
		return nil, fmt.Errorf("failed to acknowledge message: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) CancelMessage(ctx context.Context, req *queueservice_pb.CancelMessageRequest) (*queueservice_pb.CancelMessageResponse, error) {
	s.logger.InfoWithFields("CancelMessage called",
		"queue_name", req.GetQueueName(),
		"message_id", req.GetMessageId())

	resp, err := s.storage.CancelMessage(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to cancel message",
			"queue_name", req.GetQueueName(),
			"message_id", req.GetMessageId(),
			"error", err)
		return nil, fmt.Errorf("failed to cancel message: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) RenewMessageLease(ctx context.Context, req *queueservice_pb.RenewMessageLeaseRequest) (*queueservice_pb.RenewMessageLeaseResponse, error) {
	s.logger.InfoWithFields("RenewMessageLease called",
		"queue_name", req.GetQueueName(),
		"message_id", req.GetMessageId())

	resp, err := s.storage.RenewMessageLease(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to renew message lease",
			"queue_name", req.GetQueueName(),
			"message_id", req.GetMessageId(),
			"error", err)
		return nil, fmt.Errorf("failed to renew message lease: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) PeekQueueMessages(ctx context.Context, req *queueservice_pb.PeekQueueMessagesRequest) (*queueservice_pb.PeekQueueMessagesResponse, error) {
	s.logger.InfoWithFields("PeekQueueMessages called", "queue_name", req.GetQueueName())

	resp, err := s.storage.PeekQueueMessages(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to peek queue messages", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to peek queue messages: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) SendMessageHeartBeat(ctx context.Context, req *queueservice_pb.SendMessageHeartBeatRequest) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	s.logger.InfoWithFields("SendMessageHeartBeat called", "queue_name", req.GetQueueName())

	resp, err := s.storage.SendMessageHeartBeat(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to send message heartbeat", "queue_name", req.GetQueueName(), "error", err)
		return nil, fmt.Errorf("failed to send message heartbeat: %w", err)
	}

	return resp, nil
}

// Schedule Management Methods

func (s *ChronoQueueServer) CreateSchedule(ctx context.Context, req *queueservice_pb.CreateScheduleRequest) (*queueservice_pb.CreateScheduleResponse, error) {
	s.logger.InfoWithFields("CreateSchedule called", "schedule_name", req.Schedule.GetScheduleId())

	resp, err := s.storage.CreateSchedule(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to create schedule", "schedule_name", req.GetSchedule().ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to create schedule: %w", err)
	}

	s.logger.InfoWithFields("Schedule created successfully", "schedule_name", req.GetSchedule().ScheduleId)
	return resp, nil
}

func (s *ChronoQueueServer) DeleteSchedule(ctx context.Context, req *queueservice_pb.DeleteScheduleRequest) (*queueservice_pb.DeleteScheduleResponse, error) {
	s.logger.InfoWithFields("DeleteSchedule called", "schedule_name", req.ScheduleId)

	resp, err := s.storage.DeleteSchedule(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to delete schedule", "schedule_name", req.ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to delete schedule: %w", err)
	}

	s.logger.InfoWithFields("Schedule deleted successfully", "schedule_name", req.ScheduleId)
	return resp, nil
}

func (s *ChronoQueueServer) GetSchedule(ctx context.Context, req *queueservice_pb.GetScheduleRequest) (*queueservice_pb.GetScheduleResponse, error) {
	s.logger.InfoWithFields("GetSchedule called", "schedule_name", req.ScheduleId)

	resp, err := s.storage.GetSchedule(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to get schedule", "schedule_name", req.ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) ListSchedules(ctx context.Context, req *queueservice_pb.ListSchedulesRequest) (*queueservice_pb.ListSchedulesResponse, error) {
	s.logger.Info("ListSchedules called")

	resp, err := s.storage.ListSchedules(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to list schedules", "error", err)
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) GetScheduleHistory(ctx context.Context, req *queueservice_pb.GetScheduleHistoryRequest) (*queueservice_pb.GetScheduleHistoryResponse, error) {
	s.logger.InfoWithFields("GetScheduleHistory called", "schedule_name", req.ScheduleId)

	resp, err := s.storage.GetScheduleHistory(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to get schedule history", "schedule_name", req.ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to get schedule history: %w", err)
	}

	return resp, nil
}

func (s *ChronoQueueServer) PauseSchedule(ctx context.Context, req *queueservice_pb.PauseScheduleRequest) (*queueservice_pb.PauseScheduleResponse, error) {
	s.logger.InfoWithFields("PauseSchedule called", "schedule_name", req.ScheduleId)

	resp, err := s.storage.PauseSchedule(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to pause schedule", "schedule_name", req.ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to pause schedule: %w", err)
	}

	s.logger.InfoWithFields("Schedule paused successfully", "schedule_name", req.ScheduleId)
	return resp, nil
}

func (s *ChronoQueueServer) ResumeSchedule(ctx context.Context, req *queueservice_pb.ResumeScheduleRequest) (*queueservice_pb.ResumeScheduleResponse, error) {
	s.logger.InfoWithFields("ResumeSchedule called", "schedule_name", req.ScheduleId)

	resp, err := s.storage.ResumeSchedule(ctx, req)
	if err != nil {
		s.logger.ErrorWithFields("Failed to resume schedule", "schedule_name", req.ScheduleId, "error", err)
		return nil, fmt.Errorf("failed to resume schedule: %w", err)
	}

	s.logger.InfoWithFields("Schedule resumed successfully", "schedule_name", req.ScheduleId)
	return resp, nil
}

// Calendar Schedule Operations

func (s *ChronoQueueServer) ValidateCalendarSchedule(ctx context.Context, req *queueservice_pb.ValidateCalendarScheduleRequest) (*queueservice_pb.ValidateCalendarScheduleResponse, error) {
	s.logger.InfoWithFields("ValidateCalendarSchedule called", "schedule", req.GetCalendarSchedule())

	err := s.storage.ValidateCalendarSchedule(ctx, req.GetCalendarSchedule())
	if err != nil {
		s.logger.ErrorWithFields("Calendar schedule validation failed", "error", err)
		// Return validation error details
		return &queueservice_pb.ValidateCalendarScheduleResponse{
			Valid:        false,
			ErrorMessage: err.Error(),
		}, nil
	}

	s.logger.InfoWithFields("Calendar schedule validated successfully")
	return &queueservice_pb.ValidateCalendarScheduleResponse{
		Valid: true,
	}, nil
}

func (s *ChronoQueueServer) PreviewCalendarSchedule(ctx context.Context, req *queueservice_pb.PreviewCalendarScheduleRequest) (*queueservice_pb.PreviewCalendarScheduleResponse, error) {
	s.logger.InfoWithFields("PreviewCalendarSchedule called", "schedule", req.GetCalendarSchedule(), "count", req.GetCount())

	// Default to 10 if not specified
	count := req.GetCount()
	if count == 0 {
		count = 10
	}
	if count > 100 {
		count = 100
	}

	preview, err := s.storage.GetCalendarSchedulePreview(ctx, req.GetCalendarSchedule(), int(count))
	if err != nil {
		s.logger.ErrorWithFields("Failed to generate calendar schedule preview", "error", err)
		return nil, fmt.Errorf("failed to generate preview: %w", err)
	}

	s.logger.InfoWithFields("Calendar schedule preview generated successfully", "execution_count", len(preview.ExecutionTimes))
	return preview, nil
}

// Dead Letter Queue Management Methods

func (s *ChronoQueueServer) GetDLQMessages(ctx context.Context, req *queueservice_pb.GetDLQMessagesRequest) (*queueservice_pb.GetDLQMessagesResponse, error) {
	s.logger.InfoWithFields("GetDLQMessages called", "dlq_name", req.GetDlqName())

	messages, err := s.storage.GetDLQMessages(ctx, req.GetDlqName(), req.GetLimit())
	if err != nil {
		s.logger.ErrorWithFields("Failed to get DLQ messages", "dlq_name", req.GetDlqName(), "error", err)
		return nil, fmt.Errorf("failed to get DLQ messages: %w", err)
	}

	return &queueservice_pb.GetDLQMessagesResponse{
		Messages: messages,
	}, nil
}

func (s *ChronoQueueServer) RequeueFromDLQ(ctx context.Context, req *queueservice_pb.RequeueFromDLQRequest) (*queueservice_pb.RequeueFromDLQResponse, error) {
	s.logger.InfoWithFields("RequeueFromDLQ called",
		"dlq_name", req.GetDlqName(),
		"message_id", req.GetMessageId(),
		"target_queue", req.GetTargetQueue())

	err := s.storage.RequeueFromDLQ(ctx, req.GetDlqName(), req.GetMessageId(), req.GetTargetQueue(), true)
	if err != nil {
		s.logger.ErrorWithFields("Failed to requeue message from DLQ",
			"dlq_name", req.GetDlqName(),
			"message_id", req.GetMessageId(),
			"target_queue", req.GetTargetQueue(),
			"error", err)
		return nil, fmt.Errorf("failed to requeue message from DLQ: %w", err)
	}

	s.logger.InfoWithFields("Message requeued from DLQ successfully",
		"dlq_name", req.GetDlqName(),
		"message_id", req.GetMessageId(),
		"target_queue", req.GetTargetQueue())

	return &queueservice_pb.RequeueFromDLQResponse{
		Success: true,
	}, nil
}

func (s *ChronoQueueServer) DeleteFromDLQ(ctx context.Context, req *queueservice_pb.DeleteFromDLQRequest) (*queueservice_pb.DeleteFromDLQResponse, error) {
	s.logger.InfoWithFields("DeleteFromDLQ called",
		"dlq_name", req.GetDlqName(),
		"message_id", req.GetMessageId())

	err := s.storage.DeleteFromDLQ(ctx, req.GetDlqName(), req.GetMessageId())
	if err != nil {
		s.logger.ErrorWithFields("Failed to delete message from DLQ",
			"dlq_name", req.GetDlqName(),
			"message_id", req.GetMessageId(),
			"error", err)
		return nil, fmt.Errorf("failed to delete message from DLQ: %w", err)
	}

	s.logger.InfoWithFields("Message deleted from DLQ successfully",
		"dlq_name", req.GetDlqName(),
		"message_id", req.GetMessageId())

	return &queueservice_pb.DeleteFromDLQResponse{
		Success: true,
	}, nil
}

func (s *ChronoQueueServer) PurgeDLQ(ctx context.Context, req *queueservice_pb.PurgeDLQRequest) (*queueservice_pb.PurgeDLQResponse, error) {
	s.logger.InfoWithFields("PurgeDLQ called", "dlq_name", req.GetDlqName())

	err := s.storage.PurgeDLQ(ctx, req.GetDlqName())
	if err != nil {
		s.logger.ErrorWithFields("Failed to purge DLQ", "dlq_name", req.GetDlqName(), "error", err)
		return nil, fmt.Errorf("failed to purge DLQ: %w", err)
	}

	s.logger.InfoWithFields("DLQ purged successfully", "dlq_name", req.GetDlqName())
	return &queueservice_pb.PurgeDLQResponse{
		Success: true,
	}, nil
}

func (s *ChronoQueueServer) GetDLQStats(ctx context.Context, req *queueservice_pb.GetDLQStatsRequest) (*queueservice_pb.GetDLQStatsResponse, error) {
	s.logger.InfoWithFields("GetDLQStats called", "dlq_name", req.GetDlqName())

	stats, err := s.storage.GetDLQStats(ctx, req.GetDlqName())
	if err != nil {
		s.logger.ErrorWithFields("Failed to get DLQ stats", "dlq_name", req.GetDlqName(), "error", err)
		return nil, fmt.Errorf("failed to get DLQ stats: %w", err)
	}

	return &queueservice_pb.GetDLQStatsResponse{
		Name:         stats.Name,
		MessageCount: stats.MessageCount,
		CreatedAt:    stats.CreatedAt,
		UpdatedAt:    stats.UpdatedAt,
	}, nil
}

// Schema Management Methods

func (s *ChronoQueueServer) RegisterSchema(ctx context.Context, req *queueservice_pb.RegisterSchemaRequest) (*queueservice_pb.RegisterSchemaResponse, error) {
	s.logger.InfoWithFields("RegisterSchema called", "schema_id", req.GetSchemaId())

	reqSchema := &schema_pb.Schema{
		SchemaId:    req.GetSchemaId(),
		Name:        req.GetName(),
		Content:     req.GetContent(),
		Description: req.GetDescription(),
		ContentType: req.GetContentType(),
		Metadata:    req.GetMetadata(),
	}

	registryResp, err := s.schemaRegistry.Register(ctx, reqSchema)
	if err != nil {
		s.logger.ErrorWithFields("Failed to register schema", "schema_id", req.GetSchemaId(), "error", err)
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}

	s.logger.InfoWithFields("Schema registered successfully", "schema_id", req.GetSchemaId())
	resp := &queueservice_pb.RegisterSchemaResponse{
		SchemaId: registryResp.SchemaID,
		Version:  registryResp.LatestVersion,
	}
	return resp, nil
}

func (s *ChronoQueueServer) GetSchema(ctx context.Context, req *queueservice_pb.GetSchemaRequest) (*queueservice_pb.GetSchemaResponse, error) {
	s.logger.InfoWithFields("GetSchema called", "schema_id", req.GetSchemaId())

	resp, err := s.schemaRegistry.Get(ctx, req.SchemaId, req.Version)
	if err != nil {
		s.logger.ErrorWithFields("Failed to get schema", "schema_id", req.GetSchemaId(), "error", err)
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	return &queueservice_pb.GetSchemaResponse{
		Schema: resp,
	}, nil
}

func (s *ChronoQueueServer) ListSchemas(ctx context.Context, req *queueservice_pb.ListSchemasRequest) (*queueservice_pb.ListSchemasResponse, error) {
	s.logger.Info("ListSchemas called")

	resp, err := s.schemaRegistry.List(ctx)
	if err != nil {
		s.logger.ErrorWithFields("Failed to list schemas", "error", err)
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}

	schemasInfo := make([]*queueservice_pb.SchemaInfo, 0, len(resp))
	for _, schema := range resp {
		schemasInfo = append(schemasInfo, &queueservice_pb.SchemaInfo{
			Name:          schema.Name,
			SchemaId:      schema.SchemaId,
			Description:   schema.Description,
			LatestVersion: schema.Version,
			IsActive:      schema.IsActive,
		})
	}

	return &queueservice_pb.ListSchemasResponse{
		Schemas: schemasInfo,
	}, nil
}

func (s *ChronoQueueServer) DeleteSchema(ctx context.Context, req *queueservice_pb.DeleteSchemaRequest) (*queueservice_pb.DeleteSchemaResponse, error) {
	s.logger.InfoWithFields("DeleteSchema called", "schema_id", req.GetSchemaId())

	err := s.schemaRegistry.Deactivate(ctx, req.GetSchemaId(), req.GetVersion())
	if err != nil {
		s.logger.ErrorWithFields("Failed to delete schema", "schema_id", req.GetSchemaId(), "error", err)
		return nil, fmt.Errorf("failed to delete schema: %w", err)
	}

	s.logger.InfoWithFields("Schema deleted successfully", "schema_id", req.GetSchemaId())
	return &queueservice_pb.DeleteSchemaResponse{
		Success:         true,
		VersionsDeleted: req.GetVersion(),
	}, nil
}

func (s *ChronoQueueServer) ValidatePayload(ctx context.Context, req *queueservice_pb.ValidatePayloadRequest) (*queueservice_pb.ValidatePayloadResponse, error) {
	s.logger.InfoWithFields("ValidatePayload called", "schema_id", req.GetSchemaId())

	resp, err := s.schemaRegistry.Validate(ctx, req.GetSchemaId(), req.GetVersion(), []byte(req.GetPayload()))
	if err != nil {
		s.logger.ErrorWithFields("Failed to validate payload", "schema_id", req.GetSchemaId(), "error", err)
		return nil, fmt.Errorf("failed to validate payload: %w", err)
	}

	return &queueservice_pb.ValidatePayloadResponse{
		Valid:         resp.Valid,
		Errors:        resp.Errors,
		SchemaId:      req.SchemaId,
		SchemaVersion: req.Version,
	}, nil
}
