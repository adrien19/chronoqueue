package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
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
	rs          *redsync.Redsync
}

func NewQueueStorage(redisClient *redis.Client) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	storage := &storage{redisClient: redisClient, rs: rs}
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

func (as *storage) DeleteQueueMessage(ctx context.Context, queueName string, messageID string) error {
	_, err := as.redisClient.Del(ctx, messageID).Result()
	if err != nil {
		return err
	}
	return nil
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
