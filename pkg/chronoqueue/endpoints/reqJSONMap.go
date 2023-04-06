package endpoints

import (
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

type DeleteQueueRequest struct {
	QueueName string `json:"queueName"`
}

type ErrorResponse struct {
	Code    internal.ErrorCode `json:"code"`
	Message string             `json:"message"`
}
