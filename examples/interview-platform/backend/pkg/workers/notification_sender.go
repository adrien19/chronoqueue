package workers

import (
	"context"
	"log"
	"time"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/client"

	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
)

// NotificationSenderWorker sends notifications to users
type NotificationSenderWorker struct {
	queue *client.ChronoQueueClient
	db    *db.Database
}

// NewNotificationSenderWorker creates a new notification sender worker
func NewNotificationSenderWorker(queue *client.ChronoQueueClient, database *db.Database) *NotificationSenderWorker {
	return &NotificationSenderWorker{
		queue: queue,
		db:    database,
	}
}

// Start begins processing messages from the notification-sender queue
func (w *NotificationSenderWorker) Start(ctx context.Context) error {
	log.Println("[Notification Sender] Worker started")

	queueName := "notification-sender"
	leaseDuration := "30s"

	for {
		select {
		case <-ctx.Done():
			log.Println("[Notification Sender] Worker stopping...")
			return nil
		default:
			response, err := w.queue.GetNextMessage(ctx, queueName, leaseDuration, false)
			if err != nil {
				log.Printf("[Notification Sender] Error getting message: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if response.GetMessage() == nil {
				time.Sleep(2 * time.Second)
				continue
			}

			msg := response.GetMessage()
			if err := w.processMessage(ctx, queueName, msg); err != nil {
				log.Printf("[Notification Sender] Error processing message %s: %v", msg.GetMessageId(), err)
			}
		}
	}
}

// processMessage handles a single notification message
func (w *NotificationSenderWorker) processMessage(ctx context.Context, queueName string, msg *message_pb.Message) error {
	log.Printf("[Notification Sender] Processing message: %s", msg.GetMessageId())

	metadata := msg.GetMetadata()
	if metadata == nil || metadata.GetPayload() == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED)
		return nil
	}

	payloadData := metadata.GetPayload().GetData()
	if payloadData == nil {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED)
		return nil
	}

	fields := payloadData.AsMap()
	notificationType, _ := fields["notification_type"].(string)
	recipient, _ := fields["recipient"].(string)
	subject, _ := fields["subject"].(string)
	body, _ := fields["body"].(string)
	relatedID, _ := fields["related_id"].(string)

	if recipient == "" || subject == "" {
		w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED)
		return nil
	}

	// Process based on notification type
	switch notificationType {
	case "interview_scheduled":
		log.Printf("[Notification Sender] Sending interview scheduled notification to %s", recipient)
		log.Printf("  Subject: %s", subject)
		log.Printf("  Body: %s", body)
		log.Printf("  Interview ID: %s", relatedID)

		// In a real system, you would:
		// 1. Send email via SMTP or email service (SendGrid, AWS SES)
		// 2. Send in-app notification
		// 3. Send SMS for urgent notifications
		// 4. Log notification in database

	case "evaluation_submitted":
		log.Printf("[Notification Sender] Sending evaluation submitted notification to %s", recipient)
		log.Printf("  Subject: %s", subject)
		log.Printf("  Evaluation ID: %s", relatedID)

	case "report_ready":
		log.Printf("[Notification Sender] Sending report ready notification to %s", recipient)
		log.Printf("  Subject: %s", subject)
		log.Printf("  Report ID: %s", relatedID)

	case "interview_reminder":
		log.Printf("[Notification Sender] Sending interview reminder to %s", recipient)
		log.Printf("  Subject: %s", subject)
		log.Printf("  Time: 1 hour before interview")

	default:
		log.Printf("[Notification Sender] Unknown notification type: %s", notificationType)
	}

	// Simulate sending time
	time.Sleep(100 * time.Millisecond)

	// Acknowledge message
	if _, err := w.queue.AcknowledgeMessage(ctx, queueName, msg.GetMessageId(), client.MESSAGE_COMPLETED); err != nil {
		log.Printf("[Notification Sender] Failed to acknowledge message: %v", err)
		return err
	}

	log.Printf("[Notification Sender] Successfully sent notification: %s to %s", notificationType, recipient)
	return nil
}
