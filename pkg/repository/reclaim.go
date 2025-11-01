package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
)

type ReclaimService struct {
	storage  *storage
	interval time.Duration
	stopChan chan struct{}
}

func NewReclaimService(storage *storage, interval time.Duration) *ReclaimService {
	return &ReclaimService{
		storage:  storage,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

func (r *ReclaimService) Start(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.reclaimStuckMessages(ctx)
		case <-r.stopChan:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *ReclaimService) Stop() {
	close(r.stopChan)
}

func (r *ReclaimService) reclaimStuckMessages(ctx context.Context) {
	queues, err := r.storage.listQueueNames(ctx)
	if err != nil {
		r.storage.logger.ErrorWithFields("Reclaim: failed to list queues", "error", err)
		return
	}
	for _, queueName := range queues {
		// Get queue metadata to determine appropriate idle threshold
		queueMeta, err := r.storage.GetQueueMetadata(ctx, queueName)
		if err != nil {
			r.storage.logger.ErrorWithFields("Reclaim: failed to get queue metadata", "queue", queueName, "error", err)
			continue
		}

		// Calculate idle threshold as 2x the lease duration, with a minimum of 10s and maximum of 60s
		// This ensures we reclaim stuck messages shortly after their lease expires
		leaseDurationMs := queueMeta.GetLeaseDuration().AsDuration().Milliseconds()
		minIdleMs := leaseDurationMs * 2
		if minIdleMs < 10000 { // Minimum 10 seconds
			minIdleMs = 10000
		}
		if minIdleMs > 60000 { // Maximum 60 seconds
			minIdleMs = 60000
		}

		for _, priority := range []string{"high", "medium", "low"} {
			streamKey := fmt.Sprintf("stream:%s:%s", priority, queueName)
			groupKey := r.storage.groupKey(queueName)
			var startID string = "0-0"
			for {
				msgs, nextID, err := r.xAutoClaim(ctx, streamKey, groupKey, startID, minIdleMs, 10)
				if err != nil {
					r.storage.logger.ErrorWithFields("XAUTOCLAIM failed", "queue", queueName, "priority", priority, "error", err)
					break
				}
				if len(msgs) == 0 {
					break
				}
				for _, msg := range msgs {
					messageID, ok := msg.Values["message_id"].(string)
					if !ok || messageID == "" {
						continue
					}
					meta, err := r.storage.fetchMessageMetadata(ctx, queueName, messageID)
					if err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to fetch metadata", "messageID", messageID, "error", err)
						continue
					}
					oldState := meta.State
					meta.State = message_pb.Message_Metadata_PENDING
					if err := r.storage.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, oldState); err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to update metadata", "messageID", messageID, "error", err)
					}
				}
				if nextID == startID {
					break
				}
				startID = nextID
			}
		}
	}
}

func (r *ReclaimService) xAutoClaim(ctx context.Context, streamKey, groupKey, startID string, minIdle int64, count int64) ([]redis.XMessage, string, error) {
	msgs, nextID, err := r.storage.redisClient.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey,
		Group:    groupKey,
		Consumer: "reclaimer",
		MinIdle:  time.Duration(minIdle) * time.Millisecond,
		Start:    startID,
		Count:    count,
	}).Result()
	if err != nil {
		return nil, "", err
	}
	return msgs, nextID, nil
}
