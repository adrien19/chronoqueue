package endpoints

import (
	"log"
	"time"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"
	"google.golang.org/protobuf/encoding/protojson"
)

type CreateQueueRequest struct {
	QueueInfo *pb_chronoqueue.Queue
}

func (cqr CreateQueueRequest) MarshalBinary() ([]byte, error) {
	log.Println("Marshalling ===>> ", cqr)
	return protojson.Marshal(cqr.QueueInfo)
}

// func (cqr CreateQueueRequest) MarshalToMapOfInterface() (map[string]interface{}, error) {
// 	var tempMap map[string]interface{}
// 	jsonData, err := protojson.Marshal(cqr.QueueInfo)
// 	if err != nil {
// 		log.Println("Failed to serialize queue's metadata")
// 		return tempMap, err
// 	}

// 	if err := json.Unmarshal(jsonData, &tempMap); err != nil {
// 		log.Println("Failed to deserialize queue's metadata")
// 		return tempMap, err
// 	}
// 	return tempMap, nil
// }

func (cqr CreateQueueRequest) UnmarshalBinary(data []byte) error {
	if err := protojson.Unmarshal(data, cqr.QueueInfo); err != nil {
		return err
	}
	return nil
}

type CreateQueueResponse struct {
	Reply *pb_chronoqueue.CreateQueueResponse `json:"reply"`
}

type PostMessageRequest struct {
	Request *pb_chronoqueue.PostMessageRequest
}

type PostMessageResponse struct {
	Request *pb_chronoqueue.PostMessageResponse
}

// type PostMessageRequest struct {
// 	QueueName string                    `json:"queueName"`
// 	Message   internal.QueueMessageInfo `json:"message"`
// }

type GetNextMessageRequest struct {
	QueueName     string `json:"queueName"`
	LeaseDuration int64  `json:"leaseDuration,omitempty"`
}

type GetNextMessageResponse struct {
	Message internal.QueueMessageInfo `json:"message"`
}

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

type PeekQueueMessagesRequest struct {
	QueueName     string                 `json:"queueName"`
	Limit         int64                  `json:"limit"`
	PriorityRange internal.PriorityRange `json:"priorityRange,omitempty"`
}

type PeekQueueMessagesResponse struct {
	Messages []internal.QueueMessageInfo `json:"messages"`
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
