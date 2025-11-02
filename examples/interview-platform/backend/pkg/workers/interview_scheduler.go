package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/client"

	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// InterviewSchedulerWorker processes interview scheduling messages
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
	leaseDuration := "30s" // 30 seconds lease

	for {
		select {
		case <-ctx.Done():
			log.Println("[Interview Scheduler] Worker stopping...")
			return nil
		default:
			// Get next message from queue
			response, err := w.queue.GetNextMessage(ctx, queueName, leaseDuration, false)
			if err != nil {
				log.Printf("[Interview Scheduler] Error getting message: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if response.GetMessage() == nil {
				// No messages available, wait before polling again
				log.Printf("[Interview Scheduler] No messages available")
				time.Sleep(2 * time.Second)
				continue
			}

			// Process the message
			msg := response.GetMessage()
			streamEntryID := response.GetStreamEntryId()
			log.Printf("[Interview Scheduler] Received message: %s", msg.GetMessageId())
			if err := w.processMessage(ctx, queueName, msg, streamEntryID); err != nil {
				log.Printf("[Interview Scheduler] Error processing message %s: %v", msg.GetMessageId(), err)
				// Message lease will expire and become available again
			}
		}
	}
}

// processMessage handles a single interview scheduling message
func (w *InterviewSchedulerWorker) processMessage(ctx context.Context, queueName string, msg *message_pb.Message, streamEntryID string) error {
	log.Printf("[Interview Scheduler] Processing message: %s", msg.GetMessageId())

	// Get payload data
	metadata := msg.GetMetadata()
	if metadata == nil || metadata.GetPayload() == nil {
		log.Printf("[Interview Scheduler] Empty payload")
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	payloadData := metadata.GetPayload().GetData()
	if payloadData == nil {
		log.Printf("[Interview Scheduler] Empty payload data")
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	// Extract fields from struct
	fields := payloadData.AsMap()

	interviewID, _ := fields["interview_id"].(string)
	candidateName, _ := fields["candidate_name"].(string)
	candidateEmail, _ := fields["candidate_email"].(string)
	action, _ := fields["action"].(string)

	if interviewID == "" {
		log.Printf("[Interview Scheduler] Missing interview_id")
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return nil
	}

	// Get interview from database
	interview, err := w.db.GetInterview(interviewID)
	if err != nil {
		log.Printf("[Interview Scheduler] Interview not found: %v", err)
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID)
		return err
	}

	// Process based on action
	switch action {
	case "schedule_interview":
		// Update interview status to scheduled (logistics set up)
		if interview.Status != models.StatusScheduled {
			if err := w.db.UpdateInterviewStatus(interviewID, models.StatusScheduled); err != nil {
				log.Printf("[Interview Scheduler] Failed to update status: %v", err)
				return err
			}
			log.Printf("[Interview Scheduler] Interview %s for %s marked as scheduled", interviewID, candidateName)
		}

		// In a real system, send calendar invites immediately
		log.Printf("[Interview Scheduler] Sending calendar invites to %s and interviewers", candidateEmail)

		// Get the scheduled time (already a time.Time)
		scheduledTime := interview.ScheduledAt

		// Schedule reminder notifications using ChronoQueue's ScheduledTime feature
		// Reminder 1: 10 hours before interview (if interview is more than 10 hours away)
		tenHoursBefore := scheduledTime.Add(-10 * time.Hour)
		if time.Now().Before(tenHoursBefore) {
			reminderPayload1 := map[string]interface{}{
				"notification_type": "interview_reminder",
				"recipient":         candidateEmail,
				"subject":           "Reminder: Interview Tomorrow",
				"body":              fmt.Sprintf("You have an interview scheduled for tomorrow at %s for the position of %s", scheduledTime.Format("3:04 PM"), interview.Position),
				"related_id":        interviewID,
			}

			payloadStruct1, _ := structpb.NewStruct(reminderPayload1)
			messageID1 := fmt.Sprintf("reminder-10h-%s", interviewID)

			scheduledAt1 := tenHoursBefore
			_, err = w.queue.PostMessage(context.Background(), "notification-sender", messageID1, client.MessageOptions{
				Payload: client.Payload{
					Data:        payloadStruct1,
					ContentType: "application/json",
				},
				ScheduledTime: &scheduledAt1,
				MaxAttempts:   3,
			})
			if err != nil {
				log.Printf("[Interview Scheduler] Failed to schedule 10-hour reminder: %v", err)
			} else {
				log.Printf("[Interview Scheduler] Scheduled 10-hour reminder for %s", tenHoursBefore.Format(time.RFC3339))
			}
		}

		// Reminder 2: 1 hour before interview (if interview is more than 1 hour away)
		oneHourBefore := scheduledTime.Add(-1 * time.Hour)
		if time.Now().Before(oneHourBefore) {
			reminderPayload2 := map[string]interface{}{
				"notification_type": "interview_reminder",
				"recipient":         candidateEmail,
				"subject":           "Reminder: Interview in 1 Hour",
				"body":              fmt.Sprintf("Your interview starts in 1 hour at %s for the position of %s", scheduledTime.Format("3:04 PM"), interview.Position),
				"related_id":        interviewID,
			}

			payloadStruct2, _ := structpb.NewStruct(reminderPayload2)
			messageID2 := fmt.Sprintf("reminder-1h-%s", interviewID)

			scheduledAt2 := oneHourBefore
			_, err = w.queue.PostMessage(context.Background(), "notification-sender", messageID2, client.MessageOptions{
				Payload: client.Payload{
					Data:        payloadStruct2,
					ContentType: "application/json",
				},
				ScheduledTime: &scheduledAt2,
				MaxAttempts:   3,
			})
			if err != nil {
				log.Printf("[Interview Scheduler] Failed to schedule 1-hour reminder: %v", err)
			} else {
				log.Printf("[Interview Scheduler] Scheduled 1-hour reminder for %s", oneHourBefore.Format(time.RFC3339))
			}
		} // Send immediate confirmation notification
		confirmPayload := map[string]interface{}{
			"notification_type": "interview_scheduled",
			"recipient":         candidateEmail,
			"subject":           fmt.Sprintf("Interview Scheduled - %s", interview.Position),
			"body":              fmt.Sprintf("Your interview has been scheduled for %s. Calendar invite has been sent.", scheduledTime.Format("Monday, January 2, 2006 at 3:04 PM")),
			"related_id":        interviewID,
		}

		confirmStruct, _ := structpb.NewStruct(confirmPayload)
		confirmMsgID := fmt.Sprintf("confirm-%s", interviewID)

		_, err = w.queue.PostMessage(context.Background(), "notification-sender", confirmMsgID, client.MessageOptions{
			Payload: client.Payload{
				Data:        confirmStruct,
				ContentType: "application/json",
			},
			MaxAttempts: 3,
		})
		if err != nil {
			log.Printf("[Interview Scheduler] Failed to send confirmation: %v", err)
		} else {
			log.Printf("[Interview Scheduler] Sent confirmation notification")
		}

	default:
		log.Printf("[Interview Scheduler] Unknown action: %s", action)
	}

	// Acknowledge message as completed
	if _, err := w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED, streamEntryID); err != nil {
		log.Printf("[Interview Scheduler] Failed to acknowledge message: %v", err)
		return err
	}

	log.Printf("[Interview Scheduler] Successfully processed message: %s", msg.GetMessageId())
	return nil
}
