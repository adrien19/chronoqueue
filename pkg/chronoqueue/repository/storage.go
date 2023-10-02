package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/internal/util"
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
	SendMessageHeartBeat(ctx context.Context, request *chronoqueue.SendMessageHeartBeatRequest) (*chronoqueue.SendMessageHeartBeatResponse, error)
}

type storage struct {
	redisClient          *redis.Client
	rs                   *redsync.Redsync
	encryptionKeyManager *keymanager.EncryptionKeyManager
}

func NewQueueStorage(ctx context.Context, redisClient *redis.Client, encryptionKeyManager *keymanager.EncryptionKeyManager) Storage {
	pool := goredis.NewPool(redisClient)
	rs := redsync.New(pool)
	storage := &storage{
		redisClient:          redisClient,
		rs:                   rs,
		encryptionKeyManager: encryptionKeyManager,
	}

	// Create a buffered channel for tasks
	tasks := make(chan Task, 10)

	// Start worker goroutines
	// for i := 0; i < 5; i++ { // 5 workers
	go storage.worker(ctx, tasks)
	// }

	// Schedule the initial tasks
	tasks <- Task{Name: "invisibleToPending", Script: invisibleToPending, Interval: 2 * time.Second}
	tasks <- Task{Name: "runningToPending", Script: runningToPending, Interval: 2 * time.Second}

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

type Task struct {
	Name     string
	Script   *redis.Script
	Interval time.Duration
}

func (as *storage) worker(ctx context.Context, tasks chan Task) {
	for {
		select {
		case task := <-tasks:
			now := time.Now().UnixNano() / int64(time.Millisecond)
			err := task.Script.Run(ctx, as.redisClient, nil, now).Err()
			if err != nil && err.Error() != "redis: nil" {
				util.ErrorWithFields("Failed to run the script", map[string]interface{}{
					"task":  task.Name,
					"error": err,
				})
			}
			// Re-schedule the task
			time.AfterFunc(task.Interval, func() { tasks <- task })
		case <-ctx.Done():
			return
		}
	}
}
