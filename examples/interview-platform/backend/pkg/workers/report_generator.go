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

// ReportGeneratorWorker processes report generation requests
type ReportGeneratorWorker struct {
	queue *client.ChronoQueueClient
	db    *db.Database
}

// NewReportGeneratorWorker creates a new report generator worker
func NewReportGeneratorWorker(queue *client.ChronoQueueClient, database *db.Database) *ReportGeneratorWorker {
	return &ReportGeneratorWorker{
		queue: queue,
		db:    database,
	}
}

// Start begins processing messages from the report-generator queue
func (w *ReportGeneratorWorker) Start(ctx context.Context) error {
	log.Println("[Report Generator] Worker started")

	queueName := "report-generator"
	leaseDuration := "30s"

	for {
		select {
		case <-ctx.Done():
			log.Println("[Report Generator] Worker stopping...")
			return nil
		default:
			response, err := w.queue.GetNextMessage(ctx, queueName, leaseDuration, false)
			if err != nil {
				log.Printf("[Report Generator] Error getting message: %v", err)
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
				log.Printf("[Report Generator] Error processing message %s: %v", msg.GetMessageId(), err)
			}
		}
	}
}

// processMessage handles a single report generation message
func (w *ReportGeneratorWorker) processMessage(ctx context.Context, queueName string, msg *message_pb.Message, streamEntryID string) error {
	log.Printf("[Report Generator] Processing message: %s", msg.GetMessageId())

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
	reportID, _ := fields["report_id"].(string)
	interviewID, _ := fields["interview_id"].(string)
	candidateName, _ := fields["candidate_name"].(string)
	action, _ := fields["action"].(string)

	if reportID == "" || interviewID == "" {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	// Get report from database
	report, err := w.db.GetReport(reportID)
	if err != nil {
		log.Printf("[Report Generator] Report not found: %v", err)
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return err
	}

	// Process based on action
	switch action {
	case "generate_report":
		// Get all evaluations for this interview
		evaluations, err := w.db.GetEvaluationsByInterview(interviewID)
		if err != nil {
			log.Printf("[Report Generator] Failed to get evaluations: %v", err)
			return err
		}

		// Calculate average scores and generate summary
		var totalScore float64
		var completedCount int
		for _, eval := range evaluations {
			if eval.Status == models.EvalStatusCompleted && eval.OverallScore > 0 {
				totalScore += float64(eval.OverallScore)
				completedCount++
			}
		}

		var avgScore float64
		if completedCount > 0 {
			avgScore = totalScore / float64(completedCount)
		}

		log.Printf("[Report Generator] Interview %s for %s: %d evaluations, avg score: %.2f",
			interviewID, candidateName, completedCount, avgScore)

		// Update report status to ready
		if report.Status != models.ReportStatusReady {
			if err := w.db.UpdateReportStatus(reportID, models.ReportStatusReady); err != nil {
				log.Printf("[Report Generator] Failed to update status: %v", err)
				return err
			}
			log.Printf("[Report Generator] Report %s marked as ready", reportID)
		}

		// In a real system, you would:
		// 1. Generate a PDF or HTML report
		// 2. Calculate detailed statistics
		// 3. Create visualizations
		// 4. Store the report file
		// 5. Send notification to hiring manager

		log.Printf("[Report Generator] Would generate comprehensive report for interview %s", interviewID)

	default:
		log.Printf("[Report Generator] Unknown action: %s", action)
	}

	// Acknowledge message
	if _, err := w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID); err != nil {
		log.Printf("[Report Generator] Failed to acknowledge message: %v", err)
		return err
	}

	log.Printf("[Report Generator] Successfully processed message: %s", msg.GetMessageId())
	return nil
}
