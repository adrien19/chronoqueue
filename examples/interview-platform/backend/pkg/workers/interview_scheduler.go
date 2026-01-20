package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	structpb "github.com/golang/protobuf/ptypes/struct"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// InterviewSchedulerWorker handles interview scheduling tasks
type InterviewSchedulerWorker struct {
	queue *client.ChronoQueueClient
	db    *db.Database
}

// NewInterviewSchedulerWorker creates a new interview scheduler worker
func NewInterviewSchedulerWorker(queue *client.ChronoQueueClient, database *db.Database) *InterviewSchedulerWorker {
	return &InterviewSchedulerWorker{
		queue: queue,
		db:    database,
	}
}

// Start begins processing messages from the interview-scheduler queue
func (w *InterviewSchedulerWorker) Start(ctx context.Context) error {
	log.Println("[Interview Scheduler] Worker started")

	queueName := "interview-scheduler"
	leaseDuration := "30s"

	for {
		select {
		case <-ctx.Done():
			log.Println("[Interview Scheduler] Worker stopping...")
			return nil
		default:
			response, err := w.queue.GetNextMessage(ctx, queueName, leaseDuration, false)
			if err != nil {
				log.Printf("[Interview Scheduler] Error getting message: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if response.GetMessage() == nil {
				time.Sleep(2 * time.Second)
				continue
			}

			msg := response.GetMessage()
			attemptID := response.GetAttemptId()
			if err := w.processMessage(ctx, queueName, msg, attemptID); err != nil {
				log.Printf("[Interview Scheduler] Error processing message %s: %v", msg.GetMessageId(), err)
			}
		}
	}
}

// processMessage handles a single interview scheduling message
func (w *InterviewSchedulerWorker) processMessage(ctx context.Context, queueName string, msg *message_pb.Message, attemptID string) error {
	log.Printf("[Interview Scheduler] Processing message: %s", msg.GetMessageId())

	metadata := msg.GetMetadata()
	if metadata == nil || metadata.GetPayload() == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID)
		return nil
	}

	payloadData := metadata.GetPayload().GetData()
	if payloadData == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID)
		return nil
	}

	fields := payloadData.AsMap()
	interviewID, _ := fields["interview_id"].(string)
	action, _ := fields["action"].(string)

	if interviewID == "" {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID)
		return nil
	}

	// Get interview from database
	interview, err := w.db.GetInterview(interviewID)
	if err != nil {
		log.Printf("[Interview Scheduler] Interview not found: %v", err)
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID)
		return err
	}

	// Process based on action
	switch action {
	case "schedule_interview":
		if interview.Status == models.StatusScheduled {
			log.Printf("[Interview Scheduler] Interview %s already scheduled", interviewID)
			w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID)
			return nil
		}

		// Update interview status
		if err := w.db.UpdateInterviewStatus(interviewID, models.StatusScheduled); err != nil {
			log.Printf("[Interview Scheduler] Failed to update status: %v", err)
			return err
		}

		log.Printf("[Interview Scheduler] Interview %s scheduled for candidate %s with interviewers: %v",
			interviewID, interview.CandidateEmail, interview.InterviewerIDs)

		// Send calendar invites (simulated)
		if err := w.sendCalendarInvite(ctx, interview); err != nil {
			log.Printf("[Interview Scheduler] Failed to send calendar invite: %v", err)
		}

		// Create evaluation tasks for each interviewer
		for _, interviewerID := range interview.InterviewerIDs {
			if err := w.createEvaluationTask(ctx, interviewID, interviewerID); err != nil {
				log.Printf("[Interview Scheduler] Failed to create evaluation task for %s: %v", interviewerID, err)
			}
		}

	case "reschedule_interview":
		log.Printf("[Interview Scheduler] Rescheduling interview %s", interviewID)
		// Reschedule logic here

	case "cancel_interview":
		if err := w.db.UpdateInterviewStatus(interviewID, models.StatusCancelled); err != nil {
			log.Printf("[Interview Scheduler] Failed to cancel interview: %v", err)
			return err
		}
		log.Printf("[Interview Scheduler] Interview %s cancelled", interviewID)

	default:
		log.Printf("[Interview Scheduler] Unknown action: %s", action)
	}

	// Acknowledge message
	if _, err := w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, attemptID); err != nil {
		log.Printf("[Interview Scheduler] Failed to acknowledge message: %v", err)
		return err
	}

	log.Printf("[Interview Scheduler] Successfully processed message: %s", msg.GetMessageId())
	return nil
}

// sendCalendarInvite sends calendar invites to all participants
func (w *InterviewSchedulerWorker) sendCalendarInvite(ctx context.Context, interview *models.Interview) error {
	log.Printf("[Interview Scheduler] Sending calendar invite for interview %s", interview.ID)

	// Create notification payload
	notificationPayload := map[string]interface{}{
		"interview_id":    interview.ID,
		"candidate_email": interview.CandidateEmail,
		"scheduled_at":    interview.ScheduledAt.Format(time.RFC3339),
		"type":            "calendar_invite",
		"recipients":      append(interview.InterviewerIDs, interview.CandidateEmail),
		"subject":         fmt.Sprintf("Interview Scheduled: %s", interview.Position),
		"meeting_link":    "https://meet.example.com/" + interview.ID,
		"timezone":        "America/New_York",
	}

	// Convert to protobuf Struct
	payloadBytes, err := json.Marshal(notificationPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var payloadMap map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &payloadMap); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	payloadStruct, err := toProtoStruct(payloadMap)
	if err != nil {
		return fmt.Errorf("failed to convert to proto struct: %w", err)
	}

	// Post message to notification queue
	messageID := fmt.Sprintf("calendar-invite-%s-%d", interview.ID, time.Now().Unix())
	_, err = w.queue.PostMessage(ctx, "notification-sender", messageID, client.MessageOptions{
		Payload: client.Payload{
			Data: payloadStruct,
		},
		LeaseDuration: "30s",
	})
	if err != nil {
		return fmt.Errorf("failed to post notification message: %w", err)
	}

	log.Printf("[Interview Scheduler] Calendar invite sent for interview %s", interview.ID)
	return nil
}

// createEvaluationTask creates an evaluation task for an interviewer
func (w *InterviewSchedulerWorker) createEvaluationTask(ctx context.Context, interviewID, interviewerID string) error {
	log.Printf("[Interview Scheduler] Creating evaluation task for interviewer %s", interviewerID)

	// Create evaluation in database
	evaluation := &models.Evaluation{
		ID:          fmt.Sprintf("eval-%s-%s", interviewID, interviewerID),
		InterviewID: interviewID,
		EvaluatorID: interviewerID,
		Status:      models.EvalStatusPending,
		CreatedAt:   time.Now(),
	}

	if err := w.db.CreateEvaluation(evaluation); err != nil {
		return fmt.Errorf("failed to create evaluation: %w", err)
	}

	log.Printf("[Interview Scheduler] Evaluation task created: %s", evaluation.ID)
	return nil
}

// toProtoStruct converts a map to a protobuf Struct
func toProtoStruct(data map[string]interface{}) (*structpb.Struct, error) {
	fields := make(map[string]*structpb.Value)
	for k, v := range data {
		val, err := toProtoValue(v)
		if err != nil {
			return nil, err
		}
		fields[k] = val
	}
	return &structpb.Struct{Fields: fields}, nil
}

// toProtoValue converts a Go value to a protobuf Value
func toProtoValue(v interface{}) (*structpb.Value, error) {
	switch val := v.(type) {
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: val}}, nil
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: val}}, nil
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: val}}, nil
	case []interface{}:
		listValues := make([]*structpb.Value, len(val))
		for i, item := range val {
			itemVal, err := toProtoValue(item)
			if err != nil {
				return nil, err
			}
			listValues[i] = itemVal
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{ListValue: &structpb.ListValue{Values: listValues}}}, nil
	case map[string]interface{}:
		structVal, err := toProtoStruct(val)
		if err != nil {
			return nil, err
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{StructValue: structVal}}, nil
	default:
		return &structpb.Value{Kind: &structpb.Value_NullValue{}}, nil
	}
}
