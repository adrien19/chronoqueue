package workers

import (
	"context"
	"log"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// EvaluationProcessorWorker processes evaluation messages
type EvaluationProcessorWorker struct {
	queue *client.ChronoQueueClient
	db    *db.Database
}

// NewEvaluationProcessorWorker creates a new evaluation processor worker
func NewEvaluationProcessorWorker(queue *client.ChronoQueueClient, database *db.Database) *EvaluationProcessorWorker {
	return &EvaluationProcessorWorker{
		queue: queue,
		db:    database,
	}
}

// Start begins processing messages from the evaluation-processor queue
func (w *EvaluationProcessorWorker) Start(ctx context.Context) error {
	log.Println("[Evaluation Processor] Worker started")

	queueName := "evaluation-processor"
	leaseDuration := "30s"

	for {
		select {
		case <-ctx.Done():
			log.Println("[Evaluation Processor] Worker stopping...")
			return nil
		default:
			response, err := w.queue.GetNextMessage(ctx, queueName, leaseDuration, false)
			if err != nil {
				log.Printf("[Evaluation Processor] Error getting message: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if response.GetMessage() == nil {
				time.Sleep(2 * time.Second)
				continue
			}

			msg := response.GetMessage()
			streamEntryID := response.GetStreamEntryId()
			if err := w.processMessage(ctx, queueName, msg, streamEntryID); err != nil {
				log.Printf("[Evaluation Processor] Error processing message %s: %v", msg.GetMessageId(), err)
			}
		}
	}
}

// processMessage handles a single evaluation message
func (w *EvaluationProcessorWorker) processMessage(ctx context.Context, queueName string, msg *message_pb.Message, streamEntryID string) error {
	log.Printf("[Evaluation Processor] Processing message: %s", msg.GetMessageId())

	metadata := msg.GetMetadata()
	if metadata == nil || metadata.GetPayload() == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	payloadData := metadata.GetPayload().GetData()
	if payloadData == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	fields := payloadData.AsMap()
	evaluationID, _ := fields["evaluation_id"].(string)
	interviewID, _ := fields["interview_id"].(string)
	evaluatorName, _ := fields["evaluator_name"].(string)
	action, _ := fields["action"].(string)

	if evaluationID == "" {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	// Get evaluation from database
	evaluation, err := w.db.GetEvaluation(evaluationID)
	if err != nil {
		log.Printf("[Evaluation Processor] Evaluation not found: %v", err)
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return err
	}

	// Process based on action
	switch action {
	case "process_evaluation":
		if evaluation.Status != models.EvalStatusCompleted {
			if err := w.db.UpdateEvaluationStatus(evaluationID, models.EvalStatusCompleted); err != nil {
				log.Printf("[Evaluation Processor] Failed to update status: %v", err)
				return err
			}
			log.Printf("[Evaluation Processor] Evaluation %s marked as completed", evaluationID)
		}

		// Check if all evaluations for the interview are complete
		evaluations, err := w.db.GetEvaluationsByInterview(interviewID)
		if err != nil {
			log.Printf("[Evaluation Processor] Failed to get evaluations: %v", err)
			return err
		}

		allCompleted := true
		for _, eval := range evaluations {
			if eval.Status != models.EvalStatusCompleted {
				allCompleted = false
				break
			}
		}

		if allCompleted && len(evaluations) > 0 {
			log.Printf("[Evaluation Processor] All evaluations complete for interview %s. Ready for report generation.", interviewID)
		}

		log.Printf("[Evaluation Processor] Would send notification about evaluation from %s", evaluatorName)

	default:
		log.Printf("[Evaluation Processor] Unknown action: %s", action)
	}

	// Acknowledge message
	if _, err := w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID); err != nil {
		log.Printf("[Evaluation Processor] Failed to acknowledge message: %v", err)
		return err
	}

	log.Printf("[Evaluation Processor] Successfully processed message: %s", msg.GetMessageId())
	return nil
}
