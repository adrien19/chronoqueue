package repository

import (
	"context"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	"github.com/adrien19/chronoqueue/internal/lease"
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
		queueMeta, err := r.storage.GetQueueMetadata(ctx, queueName)
		if err != nil {
			r.storage.logger.ErrorWithFields("Reclaim: failed to get queue metadata", "queue", queueName, "error", err)
			continue
		}

		// Derive a MinIdle just to avoid hammering very fresh messages.
		leaseDur := queueMeta.GetLeasePolicy().GetBaseLease().AsDuration()
		if leaseDur <= 0 {
			leaseDur = queueMeta.GetLeaseDuration().AsDuration()
		}
		if leaseDur <= 0 {
			leaseDur = 3 * time.Second
		}
		minIdle := min(max(leaseDur*2, 10*time.Second), 60*time.Second)

		for _, priorityVal := range []int32{100, 50, 10} { // high, medium, low
			streamKey := r.storage.streamKey(queueName, priorityVal)
			groupKey := r.storage.groupKey(queueName)

			startID := "0-0"
			for {
				r.storage.logger.DebugWithFields(
					"Reclaim: checking queue",
					"queue", queueName,
					"priority", priorityVal,
					"minIdle", minIdle,
				)

				msgs, nextID, err := r.xAutoClaim(ctx, streamKey, groupKey, startID, int64(minIdle.Milliseconds()), 100)
				if err != nil {
					if strings.Contains(err.Error(), "NOGROUP") || strings.Contains(err.Error(), "no such key") {
						break
					}
					r.storage.logger.ErrorWithFields("XAUTOCLAIM failed", "queue", queueName, "priority", priorityVal, "error", err)
					break
				}
				if len(msgs) == 0 {
					// No more stale pending messages at this priority.
					break
				}

				now := time.Now()

				for _, xm := range msgs {
					messageID, ok := xm.Values["message_id"].(string)
					if !ok || messageID == "" {
						continue
					}

					meta, err := r.storage.fetchMessageMetadata(ctx, queueName, messageID)
					if err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to fetch metadata", "messageID", messageID, "error", err)
						continue
					}

					oldState := meta.State

					// Use lease logic as the **real** source of truth:
					st := lease.HandleReclaim(queueMeta, meta, now)
					if !st.LeaseTimedOut && !st.HeartbeatTimedOut {
						// This message is actually still "in lease" according to metadata.
						// We accidentally touched it with XAUTOCLAIM; bounce it back.
						if err := r.requeueLiveMessage(ctx, streamKey, groupKey, xm); err != nil {
							r.storage.logger.ErrorWithFields("Reclaim: failed to requeue live message", "messageID", messageID, "error", err)
						}
						continue
					}

					// At this point, we *know* the attempt is timed out by lease or heartbeat.
					// Apply retry / DLQ logic.

					// If no attempts left and max_attempts != -1 => terminal ERRORED.
					if meta.AttemptsLeft == 0 && meta.MaxAttempts != -1 {
						meta.State = message_pb.Message_Metadata_ERRORED
						r.storage.logger.InfoWithFields("Reclaim: moving to ERRORED due to timeout",
							"messageID", messageID,
							"leaseTimedOut", st.LeaseTimedOut,
							"heartbeatTimedOut", st.HeartbeatTimedOut,
							"oldState", oldState.String(),
						)

						if err := r.storage.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, oldState); err != nil {
							r.storage.logger.ErrorWithFields("Reclaim: failed to update metadata to ERRORED", "messageID", messageID, "error", err)
						} else {
							_ = r.storage.updateStateCounters(ctx, queueName, oldState, meta.State)
						}

						// DLQ
						if dlqMeta, err := r.storage.GetQueueMetadata(ctx, queueName); err == nil && dlqMeta.GetDeadLetterQueueName() != "" {
							_ = r.pushToDLQ(ctx, dlqMeta.GetDeadLetterQueueName(), queueName, messageID, "lease/heartbeat timed out", meta)
						}

						// XACK + XDEL (no requeue).
						if err := r.storage.redisClient.XAck(ctx, streamKey, groupKey, xm.ID).Err(); err != nil {
							r.storage.logger.ErrorWithFields("Reclaim: failed to XACK ERRORED message", "messageID", messageID, "error", err)
						}
						if err := r.storage.redisClient.XDel(ctx, streamKey, xm.ID).Err(); err != nil {
							r.storage.logger.ErrorWithFields("Reclaim: failed to XDEL ERRORED message", "messageID", messageID, "error", err)
						}

						continue
					}

					// Otherwise: we still want to retry.
					if meta.AttemptsLeft > 0 {
						meta.AttemptsLeft--
					}

					meta.State = message_pb.Message_Metadata_PENDING

					if err := r.storage.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, oldState); err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to update metadata on retry", "messageID", messageID, "error", err)
						continue
					}
					if err := r.storage.updateStateCounters(ctx, queueName, oldState, meta.State); err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to update state counters", "messageID", messageID, "error", err)
					}

					r.storage.logger.InfoWithFields("Reclaim: reclaimed timed-out message",
						"messageID", messageID,
						"oldState", oldState.String(),
						"newState", meta.State.String(),
						"attemptsLeft", meta.AttemptsLeft,
					)

					// Requeue (XACK + XDEL + XADD).
					if err := r.requeueAbandonedMessage(ctx, streamKey, groupKey, xm); err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to requeue timed-out message", "messageID", messageID, "error", err)
					}
				}

				if nextID == startID || nextID == "0-0" {
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

func (r *ReclaimService) requeueLiveMessage(ctx context.Context, streamKey, groupKey string, msg redis.XMessage) error {
	px := r.storage.redisClient.TxPipeline()

	px.XAck(ctx, streamKey, groupKey, msg.ID)
	px.XDel(ctx, streamKey, msg.ID)
	px.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: msg.Values,
	})

	_, err := px.Exec(ctx)
	if err != nil {
		r.storage.logger.ErrorWithFields("Reclaim: failed to bounce live message", "streamKey", streamKey, "messageID", msg.Values["message_id"], "error", err)
		return err
	}

	r.storage.logger.DebugWithFields("Reclaim: bounced live message back to stream", "messageID", msg.Values["message_id"])
	return nil
}

func (r *ReclaimService) requeueAbandonedMessage(ctx context.Context, streamKey, groupKey string, msg redis.XMessage) error {
	// Atomically: XACK old entry, XDEL it, and XADD new entry in one pipeline
	px := r.storage.redisClient.TxPipeline()

	// XACK removes message from PEL (marks as processed by consumer group)
	px.XAck(ctx, streamKey, groupKey, msg.ID)

	// XDEL removes the old stream entry
	px.XDel(ctx, streamKey, msg.ID)

	// Re-add the same message values to the stream for re-processing
	px.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: msg.Values,
	})

	_, err := px.Exec(ctx)
	if err != nil {
		r.storage.logger.ErrorWithFields("Reclaim: failed to requeue abandoned message atomically", "streamKey", streamKey, "messageID", msg.Values["message_id"], "error", err)
		return err
	}

	r.storage.logger.DebugWithFields("Reclaim: successfully requeued abandoned message", "messageID", msg.Values["message_id"])
	return nil
}

func (r *ReclaimService) pushToDLQ(
	ctx context.Context,
	dlqQueueName string,
	originalQueue string,
	messageID string,
	failureReason string,
	meta *message_pb.Message_Metadata,
) error {
	dlqStream := r.storage.dlqStreamKey(dlqQueueName)

	values := map[string]interface{}{
		"message_id":        messageID,
		"original_queue":    originalQueue,
		"failure_reason":    failureReason,
		"timestamp":         time.Now().UnixMilli(),
		"attempt_id":        meta.GetCurrentAttempt().GetAttemptId(),
		"worker_id":         meta.GetCurrentAttempt().GetWorkerId(),
		"attempts_used":     meta.MaxAttempts - meta.AttemptsLeft,
		"max_attempts":      meta.MaxAttempts,
		"last_lease_expiry": meta.GetCurrentAttempt().GetLeaseExpiry(),
	}

	_, err := r.storage.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqStream,
		Values: values,
	}).Result()
	if err != nil {
		r.storage.logger.ErrorWithFields(
			"DLQ: failed to push message",
			"messageID", messageID,
			"dlqQueue", dlqQueueName,
			"originalQueue", originalQueue,
			"error", err,
		)
		return err
	}

	r.storage.logger.InfoWithFields(
		"DLQ: message pushed",
		"messageID", messageID,
		"dlqQueue", dlqQueueName,
		"originalQueue", originalQueue,
		"reason", failureReason,
	)

	return nil
}
