package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/adrien19/chronoqueue/client"
)

// NotificationAgent handles final report delivery
type NotificationAgent struct {
	*BaseAgent
}

// NewNotificationAgent creates a new notification agent
func NewNotificationAgent(c *client.ChronoQueueClient, workers int, verbose bool) *NotificationAgent {
	baseAgent := NewBaseAgent(c, "agent-notification", "notification", workers, verbose)
	baseAgent.skipResultPosting = true // Notification results don't need to be in agent-results
	return &NotificationAgent{
		BaseAgent: baseAgent,
	}
}

// Start begins processing notification subtasks
func (agent *NotificationAgent) Start(ctx context.Context) error {
	return agent.BaseAgent.Start(ctx, agent.processNotification)
}

// processNotification delivers the final report
func (agent *NotificationAgent) processNotification(ctx context.Context, subtask *Subtask) (*AgentResult, error) {
	if agent.verbose {
		fmt.Printf("   📧 Delivering final report: %s\n", subtask.Description)
	}

	// Extract report data from subtask input
	reportData := subtask.Input

	// Simulate delivery delay
	time.Sleep(200 * time.Millisecond)

	// Extract key information
	taskID := reportData["task_id"]
	summary, _ := reportData["summary"].(string)
	sectionsCount := 0
	if sections, ok := reportData["sections"].([]interface{}); ok {
		sectionsCount = len(sections)
	}

	// Simulate different delivery methods based on subtask
	deliveryMethod := "email" // Default
	if method, ok := reportData["delivery_method"].(string); ok {
		deliveryMethod = method
	}

	// Perform delivery simulation
	var deliveryStatus string
	switch deliveryMethod {
	case "webhook":
		deliveryStatus = agent.deliverViaWebhook(ctx, reportData)
	case "slack":
		deliveryStatus = agent.deliverViaSlack(ctx, reportData)
	default:
		deliveryStatus = agent.deliverViaEmail(ctx, reportData)
	}

	if agent.verbose {
		fmt.Printf("   ✅ Report delivered via %s: %s\n", deliveryMethod, deliveryStatus)
	}

	// Create notification result
	notificationResult := map[string]interface{}{
		"task_id":         taskID,
		"delivery_method": deliveryMethod,
		"delivery_status": deliveryStatus,
		"summary_preview": truncateString(summary, 100),
		"sections_count":  sectionsCount,
		"delivered_at":    time.Now().Unix(),
		"recipient_count": 1, // Mock recipient count
	}

	return &AgentResult{
		SubtaskID:   subtask.SubtaskID,
		ParentID:    subtask.ParentID,
		AgentType:   "notification",
		Status:      "completed",
		Result:      notificationResult,
		CompletedAt: time.Now(),
	}, nil
}

// deliverViaEmail simulates email delivery
func (agent *NotificationAgent) deliverViaEmail(ctx context.Context, report map[string]interface{}) string {
	if agent.verbose {
		fmt.Printf("   → Sending email to: user@example.com\n")
	}

	// Simulate email sending delay
	time.Sleep(150 * time.Millisecond)

	return "sent_to_user@example.com"
}

// deliverViaWebhook simulates webhook delivery
func (agent *NotificationAgent) deliverViaWebhook(ctx context.Context, report map[string]interface{}) string {
	if agent.verbose {
		fmt.Printf("   → Posting to webhook: https://example.com/webhook\n")
	}

	// Simulate HTTP POST delay
	time.Sleep(100 * time.Millisecond)

	return "posted_to_https://example.com/webhook"
}

// deliverViaSlack simulates Slack delivery
func (agent *NotificationAgent) deliverViaSlack(ctx context.Context, report map[string]interface{}) string {
	if agent.verbose {
		fmt.Printf("   → Posting to Slack channel: #reports\n")
	}

	// Simulate Slack API delay
	time.Sleep(200 * time.Millisecond)

	return "posted_to_#reports"
}

// truncateString truncates a string to maxLen characters
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
