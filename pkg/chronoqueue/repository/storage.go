package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Storage interface {
	CreateQueue(ctx context.Context, request *chronoqueue.CreateQueueRequest) (*chronoqueue.CreateQueueResponse, error)
	DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error)
	CreateQueueMessage(ctx context.Context, request *chronoqueue.PostMessageRequest) (*chronoqueue.PostMessageResponse, error)
	GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error)
	DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error
	AcknowledgeMessage(ctx context.Context, request *chronoqueue.AcknowledgeMessageRequest) (*chronoqueue.AcknowledgeMessageResponse, error)
	RenewMessageLease(ctx context.Context, request *chronoqueue.RenewMessageLeaseRequest) (*chronoqueue.RenewMessageLeaseResponse, error)
	PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error)
	GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error)
}

type storage struct {
	redisClient *redis.Client
}

func NewQueueStorage(redisClient *redis.Client) Storage {
	storage := &storage{redisClient: redisClient}
	ctx := context.Background()

	go storage.RunLuaScripts(ctx)
	return storage
}

func (as *storage) DeleteQueue(ctx context.Context, request *chronoqueue.DeleteQueueRequest) (*chronoqueue.DeleteQueueResponse, error) {

	if request == nil || request.GetName() == "" {
		return &chronoqueue.DeleteQueueResponse{}, errors.New("error: queue information missing")
	}
	checker := NewKeyChecker(as.redisClient, 100)

	start := time.Now()
	checker.Start(ctx)

	iter := as.redisClient.Scan(ctx, 0, fmt.Sprintf("%s*", request.GetName()), 0).Iterator()
	for iter.Next(ctx) {
		checker.Add(iter.Val())
	}
	if err := iter.Err(); err != nil {
		return &chronoqueue.DeleteQueueResponse{}, err
	}

	deleted := checker.Stop()
	log.Println("deleted", deleted, "keys", "in", time.Since(start))

	return &chronoqueue.DeleteQueueResponse{}, nil
}

func (as *storage) GetQueueMessage(ctx context.Context, request *chronoqueue.GetNextMessageRequest) (*chronoqueue.GetNextMessageResponse, error) {
	queueMeta, err := as.getQueueMetadata(ctx, request.GetQueueName())
	if err != nil {
		return handleError(err, "Failed to get queue's metadata. Err: ")
	}

	// if err := as.validateExclusivity(queueMeta, request.GetExclusivityKey()); err != nil {
	// 	return handleError(err, "Failed to validate exclusivity. Err: ")
	// }

	members, err := as.fetchQueueMembersBeforeNow(ctx, request.GetQueueName())
	if err != nil {
		return handleError(err, "Failed to get queue members. Err: ")
	}
	if len(members) == 0 {
		log.Println("No messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	message, err := as.getNextPendingMessage(ctx, request.GetQueueName(), members)
	if err != nil {
		log.Println("Error getting Next Pending Messsage for: ", queueMeta.Type)
		return handleError(err, "Failed to get next pending message. Err: ")
	}
	if message == nil {
		log.Println("No pending messages found with a deadline before now")
		return &chronoqueue.GetNextMessageResponse{}, nil
	}

	// Update the message's state to "Running" and restore the message
	as.updateMessageStateAndLease(message, request, queueMeta)

	if err := as.saveMessageWithMetadata(ctx, request.GetQueueName(), message); err != nil {
		return handleError(err, "Failed to save message's metadata. Err: ")
	}

	log.Println("Successfully leased the message until: ", message.Metadata.GetLeaseExpiry())
	return &chronoqueue.GetNextMessageResponse{
		Message: message,
	}, nil
}

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
}

func (as *storage) PeekQueueMessages(ctx context.Context, request *chronoqueue.PeekQueueMessagesRequest) (*chronoqueue.PeekQueueMessagesResponse, error) {
	// Get the member IDs of the messages in the sorted set with scores up to the current time.
	min := "-inf"
	max := "+inf" //:= strconv.FormatInt(time.Now().Unix(), 10)
	if request.PriorityRange != nil {
		min = strconv.FormatInt(request.PriorityRange.GetMin(), 10)
		max = strconv.FormatInt(request.PriorityRange.GetMax(), 10)
	}
	memberIDs, err := as.redisClient.ZRangeByScore(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  request.GetLimit(),
	}).Result()
	if err != nil {
		return &chronoqueue.PeekQueueMessagesResponse{}, err
	}

	messages := []*chronoqueue.Message{}
	// Get the messages' values using their member IDs.
	for _, memberID := range memberIDs {
		if len(memberID) == 0 {
			continue
		}
		metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), memberID), "metadata").Result()
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}
		var meta chronoqueue.Message_Metadata
		err = protojson.Unmarshal([]byte(metaResult), &meta)
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return nil, err
		}

		messages = append(messages, &chronoqueue.Message{
			MessageId: memberID,
			Priority:  0,
			Metadata:  &meta,
		})

	}
	return &chronoqueue.PeekQueueMessagesResponse{Messages: messages}, nil
}

func (as *storage) GetQueueState(ctx context.Context, request *chronoqueue.GetQueueStateRequest) (*chronoqueue.GetQueueStateResponse, error) {
	membersWithScores, err := as.redisClient.ZRangeByScoreWithScores(ctx, request.GetQueueName(), &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: 0,
	}).Result()
	if err != nil {
		return &chronoqueue.GetQueueStateResponse{}, err
	}
	// convert timefloat to time.
	// Assumes the first element of array is empty string member
	earliestDeadline := time.Unix(0, int64(membersWithScores[1].Score)*int64(time.Millisecond))

	invisibleMessagesCount := 0
	pendingMessagesCount := 0
	runningMessagesCount := 0
	completedMessagesCount := 0
	canceledMessagesCount := 0
	erroredMessagesCount := 0

	// Get the messages' values using their member IDs.
	for _, memberID := range membersWithScores {
		if len(memberID.Member.(string)) == 0 {
			continue
		}
		metaResult, err := as.redisClient.HGet(ctx, fmt.Sprintf("%s:%s:meta", request.GetQueueName(), memberID.Member), "metadata").Result()
		if err != nil {
			return &chronoqueue.GetQueueStateResponse{}, err
		}
		// Deserialize the message metadata
		var meta chronoqueue.Message_Metadata
		err = protojson.Unmarshal([]byte(metaResult), &meta)
		if err != nil {
			log.Println("Failed to serialize message's metadata", err)
			return &chronoqueue.GetQueueStateResponse{}, err
		}

		switch meta.GetState() {
		case chronoqueue.Message_Metadata_INVISIBLE:
			invisibleMessagesCount += 1
		case chronoqueue.Message_Metadata_PENDING:
			pendingMessagesCount += 1
		case chronoqueue.Message_Metadata_RUNNING:
			runningMessagesCount += 1
		case chronoqueue.Message_Metadata_COMPLETED:
			completedMessagesCount += 1
		case chronoqueue.Message_Metadata_CANCELED:
			canceledMessagesCount += 1
		case chronoqueue.Message_Metadata_ERRORED:
			erroredMessagesCount += 1
		default:
			continue
		}
	}

	log.Println("====>> Queue State: ",
		"invisibleMessagesCount: ", invisibleMessagesCount,
		"pendingMessagesCount: ", pendingMessagesCount,
		"runningMessagesCount: ", runningMessagesCount,
		"completedMessagesCount: ", completedMessagesCount,
		"canceledMessagesCount: ", canceledMessagesCount,
		"erroredMessagesCount: ", erroredMessagesCount,
		"earliestDeadline: ", earliestDeadline)
	return &chronoqueue.GetQueueStateResponse{
		InvisibleMessagesCount: int32(invisibleMessagesCount),
		PendingMessagesCount:   int32(pendingMessagesCount),
		RunningMessagesCount:   int32(runningMessagesCount),
		CompletedMessagesCount: int32(completedMessagesCount),
		CanceledMessagesCount:  int32(canceledMessagesCount),
		ErroredMessagesCount:   int32(erroredMessagesCount),
		EarliestDeadline:       timestamppb.New(earliestDeadline),
	}, nil
}

func (as *storage) RunLuaScripts(ctx context.Context) {
	// create a ticker to run the script every minute
	ticker := time.NewTicker(1 * time.Minute)

	for {
		// wait for the ticker to fire
		<-ticker.C

		// get the current time in Unix milliseconds
		now := time.Now().UnixNano() / int64(time.Millisecond)

		// run the Lua script with the current time as an argument
		err := invisibleToPending.Run(ctx, as.redisClient, nil, now).Err()
		if err != nil && err.Error() != "redis: nil" {
			log.Printf("Failed to run the script %d\n", err)
			panic(err)
		}
	}
}
