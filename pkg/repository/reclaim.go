package repository

import (
	"context"
	"fmt"
	"strconv"
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

			startID := "-"
			for {
				r.storage.logger.DebugWithFields(
					"Reclaim: checking queue",
					"queue", queueName,
					"priority", priorityVal,
					"minIdle", minIdle,
				)

				// Step 1: Get list of pending entries without claiming them
				pendingEntries, err := r.xPending(ctx, streamKey, groupKey, startID, "+", 100)
				if err != nil {
					if strings.Contains(err.Error(), "NOGROUP") || strings.Contains(err.Error(), "no such key") {
						break
					}
					r.storage.logger.ErrorWithFields("XPENDING failed", "queue", queueName, "priority", priorityVal, "error", err)
					break
				}
				if len(pendingEntries) == 0 {
					r.storage.logger.DebugWithFields(
						"Reclaim: no pending messages found",
						"queue", queueName,
						"priority", priorityVal,
					)
					break
				}

				now := time.Now()

				// Step 2: For each pending entry, check metadata BEFORE claiming
				for _, entry := range pendingEntries {
					streamEntryID := entry.ID
					idleTime := entry.Idle

					// Step 2a: Fetch the stream entry to get message_id
					xmsg, err := r.xRangeSingle(ctx, streamKey, streamEntryID)
					if err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to fetch stream entry", "streamEntryID", streamEntryID, "error", err)
						continue
					}
					if xmsg == nil {
						r.storage.logger.DebugWithFields("Reclaim: stream entry not found (may have been processed)", "streamEntryID", streamEntryID)
						continue
					}

					messageID, ok := xmsg.Values["message_id"].(string)
					if !ok || messageID == "" {
						continue
					}

					// Step 2b: Fetch metadata
					meta, err := r.storage.fetchMessageMetadata(ctx, queueName, messageID)
					if err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to fetch metadata", "messageID", messageID, "error", err)
						continue
					}

					oldState := meta.State

					// Step 2c: Check if TRULY timed out using lease logic
					st := lease.HandleReclaim(queueMeta, meta, now)
					if !st.LeaseTimedOut && !st.HeartbeatTimedOut {
						// Still live! Skip this message entirely - don't touch it.
						r.storage.logger.DebugWithFields(
							"Reclaim: message still in lease, skipping",
							"messageID", messageID,
							"streamEntryID", streamEntryID,
							"idleTime", idleTime,
							"leaseExpiry", meta.GetLeaseExpiry(),
						)
						continue
					}

					// Step 2d: NOW claim it (we know it's timed out)
					xm, err := r.xClaimSingle(ctx, streamKey, groupKey, streamEntryID, minIdle)
					if err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to claim timed-out message",
							"messageID", messageID,
							"streamEntryID", streamEntryID,
							"error", err)
						continue
					}
					if xm == nil {
						r.storage.logger.DebugWithFields("Reclaim: message already claimed or deleted",
							"messageID", messageID,
							"streamEntryID", streamEntryID)
						continue
					}

					r.storage.logger.InfoWithFields(
						"Reclaim: successfully claimed timed-out message",
						"messageID", messageID,
						"leaseTimedOut", st.LeaseTimedOut,
						"heartbeatTimedOut", st.HeartbeatTimedOut,
						"idleTime", idleTime,
					)

					// Step 3: Process the reclaimed message - apply retry / DLQ logic

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
						if dlqMeta, err := r.storage.GetQueueMetadata(ctx, queueName); err == nil && (dlqMeta.GetDeadLetterQueueName() != "" || dlqMeta.AutoCreateDlq) {
							var dlqName string
							if dlqMeta.GetDeadLetterQueueName() != "" {
								dlqName = dlqMeta.GetDeadLetterQueueName()
							} else {
								dlqName = queueName + "_dlq"
							}
							_ = r.pushToDLQ(ctx, dlqName, queueName, messageID, "lease/heartbeat timed out", meta)
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
					if err := r.requeueAbandonedMessage(ctx, streamKey, groupKey, *xm); err != nil {
						r.storage.logger.ErrorWithFields("Reclaim: failed to requeue timed-out message", "messageID", messageID, "error", err)
					}
				}

				// Pagination: move to next batch
				lastEntry := pendingEntries[len(pendingEntries)-1]
				if lastEntry.ID == startID {
					// No progress, avoid infinite loop
					r.storage.logger.DebugWithFields(
						"Reclaim: no more pending messages to process",
						"queue", queueName,
						"priority", priorityVal,
					)
					break
				}

				// Increment stream ID to exclude already-processed entry
				// Redis stream IDs are "timestamp-sequence", so we increment the sequence
				// to avoid reprocessing the last entry (XPENDING Start is inclusive)
				parts := strings.Split(lastEntry.ID, "-")
				if len(parts) == 2 {
					seq, err := strconv.ParseInt(parts[1], 10, 64)
					if err == nil {
						startID = fmt.Sprintf("%s-%d", parts[0], seq+1)
					} else {
						// Fallback: use original ID (will hit no-progress guard if stuck)
						startID = lastEntry.ID
					}
				} else {
					// Fallback for unexpected ID format
					startID = lastEntry.ID
				}
			}
		}
	}
}

// xPending gets a list of pending entries without claiming them.
// Returns pending entries with their ID, consumer, idle time, and delivery count.
func (r *ReclaimService) xPending(ctx context.Context, streamKey, groupKey, start, end string, count int64) ([]redis.XPendingExt, error) {
	pending, err := r.storage.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupKey,
		Start:  start,
		End:    end,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, err
	}
	return pending, nil
}

// xRangeSingle fetches a single stream entry by its ID.
// Used to get the message_id and other values from a pending entry.
func (r *ReclaimService) xRangeSingle(ctx context.Context, streamKey, entryID string) (*redis.XMessage, error) {
	msgs, err := r.storage.redisClient.XRange(ctx, streamKey, entryID, entryID).Result()
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &msgs[0], nil
}

// xClaimSingle claims a specific stream entry by ID.
// Only call this after verifying the message is truly timed out.
func (r *ReclaimService) xClaimSingle(ctx context.Context, streamKey, groupKey, entryID string, minIdle time.Duration) (*redis.XMessage, error) {
	msgs, err := r.storage.redisClient.XClaim(ctx, &redis.XClaimArgs{
		Stream:   streamKey,
		Group:    groupKey,
		Consumer: "reclaimer",
		MinIdle:  minIdle,
		Messages: []string{entryID},
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &msgs[0], nil
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
