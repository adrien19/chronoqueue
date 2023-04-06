package endpoints

import (
	"time"

	"github.com/adrien19/chronoqueue/internal"
)

type AcknowledgeMessageRequest struct {
	QueueName string         `json:"queueName"`
	MessageID string         `json:"messageID"`
	State     internal.State `json:"state"`
}

type RenewMessageLeaseRequest struct {
	QueueName     string `json:"queueName"`
	MessageID     string `json:"messageID"`
	LeaseDuration int64  `json:"leaseDuration"`
}

type GetQueueStateRequest struct {
	QueueName string `json:"queueName"`
}

type DeleteQueueRequest struct {
	QueueName string `json:"queueName"`
}

type GetQueueStateResponse struct {
	InvisibleMessagesCount int32     `json:"invisibleMessagesCount"`
	PendingMessagesCount   int32     `json:"pendingMessagesCount"`
	RunningMessagesCount   int32     `json:"runningMessagesCount"`
	CompletedMessagesCount int32     `json:"completedMessagesCount"`
	CanceledMessagesCount  int32     `json:"canceledMessagesCount"`
	ErroredMessagesCount   int32     `json:"erroredMessagesCount"`
	EarliestDeadline       time.Time `json:"earliestDeadline"`
}

type ErrorResponse struct {
	Code    internal.ErrorCode `json:"code"`
	Message string             `json:"message"`
}
