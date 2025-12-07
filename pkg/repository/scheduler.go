package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
)

type Scheduler struct {
	storage  *storage
	interval time.Duration
	stopChan chan struct{}
}

func NewScheduler(storage *storage, interval time.Duration) *Scheduler {
	return &Scheduler{
		storage:  storage,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.processScheduledMessages(ctx); err != nil {
				s.storage.logger.ErrorWithFields("Scheduler error", "error", err)
			}

		case <-s.stopChan:
			return

		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) Stop() {
	close(s.stopChan)
}

func (s *Scheduler) processScheduledMessages(ctx context.Context) error {
	queues, err := s.storage.listQueueNames(ctx)
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()

	for _, queueName := range queues {
		if err := s.processQueueSchedule(ctx, queueName, now); err != nil {
			s.storage.logger.ErrorWithFields("Failed to process queue schedule",
				"queue", queueName,
				"error", err,
			)
		}
	}

	return nil
}

func (s *Scheduler) processQueueSchedule(ctx context.Context, queueName string, now int64) error {
	scheduleKey := s.storage.scheduleKey(queueName)

	messages, err := s.storage.redisClient.ZRangeByScoreWithScores(ctx, scheduleKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   fmt.Sprintf("%d", now),
		Count: 100,
	}).Result()

	if err != nil || len(messages) == 0 {
		return err
	}

	pipe := s.storage.redisClient.TxPipeline()

	for _, msg := range messages {
		messageID := msg.Member.(string)

		meta, err := s.storage.fetchMessageMetadata(ctx, queueName, messageID)
		if err != nil {
			s.storage.logger.ErrorWithFields("Failed to fetch metadata", "messageID", messageID, "error", err)
			continue
		}

		priority := meta.Priority
		if priority == 0 {
			priority = 5
		}

		streamKey := s.storage.streamKey(queueName, int32(priority))
		groupKey := s.storage.groupKey(queueName)

		if err := s.storage.ensureConsumerGroup(ctx, streamKey, groupKey); err != nil {
			s.storage.logger.ErrorWithFields("Failed to ensure consumer group", "queue", queueName, "error", err)
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]interface{}{
				"queue_name":     queueName,
				"message_id":     messageID,
				"priority":       priority,
				"scheduled_time": msg.Score,
			},
		})

		meta.State = message_pb.Message_Metadata_PENDING
		if err := s.storage.saveMessageMetadata(ctx, queueName, messageID, meta); err != nil {
			s.storage.logger.ErrorWithFields("Failed to update metadata", "messageID", messageID, "error", err)
		}

		pipe.ZRem(ctx, scheduleKey, messageID)
	}

	_, err = pipe.Exec(ctx)
	return err
}
