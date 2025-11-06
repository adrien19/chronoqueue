package repository

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

func (as *storage) GetQueueState(ctx context.Context, request *queueservice_pb.GetQueueStateRequest) (*queueservice_pb.GetQueueStateResponse, error) {
	queueName := request.GetQueueName()
	stateCounts := make(map[message_pb.Message_Metadata_State]int32)
	var earliestDeadline time.Time

	// Count scheduled messages (INVISIBLE state)
	scheduleKey := as.scheduleKey(queueName)
	scheduledCount, err := as.redisClient.ZCard(ctx, scheduleKey).Result()
	if err != nil && err != redis.Nil {
		chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, "Failed to count scheduled messages")
		return nil, chronoErr.GRPCStatus()
	}
	if scheduledCount > 0 {
		stateCounts[message_pb.Message_Metadata_INVISIBLE] = int32(scheduledCount)
	}

	// Query all priority streams for this queue
	priorities := []int32{100, 50, 10} // high, medium, low
	groupKey := as.groupKey(queueName)

	for _, priority := range priorities {
		streamKey := as.streamKey(queueName, priority)

		// Get stream length (PENDING messages in stream)
		streamLen, err := as.redisClient.XLen(ctx, streamKey).Result()
		if err != nil && err != redis.Nil {
			continue
		}

		// Get consumer group info if it exists
		groupInfo, err := as.redisClient.XInfoGroups(ctx, streamKey).Result()
		if err != nil || len(groupInfo) == 0 {
			// No consumer group or stream doesn't exist yet
			if streamLen > 0 {
				stateCounts[message_pb.Message_Metadata_PENDING] += int32(streamLen)
			}
			continue
		}

		// Use XPENDING to get messages being processed (RUNNING state)
		pendingInfo, err := as.redisClient.XPending(ctx, streamKey, groupKey).Result()
		if err != nil && err != redis.Nil {
			continue
		}

		if pendingInfo != nil {
			// Messages in PEL are RUNNING
			runningCount := pendingInfo.Count
			stateCounts[message_pb.Message_Metadata_RUNNING] += int32(runningCount)

			// Messages in stream but not in PEL are PENDING
			pendingCount := streamLen - runningCount
			if pendingCount > 0 {
				stateCounts[message_pb.Message_Metadata_PENDING] += int32(pendingCount)
			}

			// Get earliest deadline from PEL
			if runningCount > 0 {
				// Get detailed pending info for earliest deadline
				pendingExt, err := as.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
					Stream: streamKey,
					Group:  groupKey,
					Start:  "-",
					End:    "+",
					Count:  1,
				}).Result()
				if err == nil && len(pendingExt) > 0 {
					// Idle time is how long the message has been pending
					// We need to calculate deadline from message metadata
					messageID := pendingExt[0].ID
					// Extract actual message ID from stream entry
					entries, err := as.redisClient.XRange(ctx, streamKey, messageID, messageID).Result()
					if err == nil && len(entries) > 0 {
						if msgID, ok := entries[0].Values["message_id"].(string); ok {
							metadata, err := as.fetchMessageMetadata(ctx, queueName, msgID)
							if err == nil && metadata.LeaseExpiry > 0 {
								leaseDeadline := time.Unix(0, metadata.LeaseExpiry*int64(time.Millisecond))
								if earliestDeadline.IsZero() || leaseDeadline.Before(earliestDeadline) {
									earliestDeadline = leaseDeadline
								}
							}
						}
					}
				}
			}
		} else if streamLen > 0 {
			// No PEL info but stream has messages - all are PENDING
			stateCounts[message_pb.Message_Metadata_PENDING] += int32(streamLen)
		}
	}

	// Read terminal state counts from Redis hash (O(1) lookup)
	// These counters are maintained by AcknowledgeMessage for COMPLETED, CANCELED, ERRORED states
	statsKey := as.statsKey(queueName)
	terminalStates := []string{"COMPLETED", "CANCELED", "ERRORED"}

	for _, stateStr := range terminalStates {
		count, err := as.redisClient.HGet(ctx, statsKey, stateStr).Int64()
		if err != nil && err != redis.Nil {
			as.logger.ErrorWithFields("Error reading state counter", "state", stateStr, "error", err)
			continue
		}
		if count > 0 {
			switch stateStr {
			case "COMPLETED":
				stateCounts[message_pb.Message_Metadata_COMPLETED] = int32(count)
			case "CANCELED":
				stateCounts[message_pb.Message_Metadata_CANCELED] = int32(count)
			case "ERRORED":
				stateCounts[message_pb.Message_Metadata_ERRORED] = int32(count)
			}
		}
	}

	return &queueservice_pb.GetQueueStateResponse{
		StateCounts: map[string]int32{
			"INVISIBLE": stateCounts[message_pb.Message_Metadata_INVISIBLE],
			"PENDING":   stateCounts[message_pb.Message_Metadata_PENDING],
			"RUNNING":   stateCounts[message_pb.Message_Metadata_RUNNING],
			"COMPLETED": stateCounts[message_pb.Message_Metadata_COMPLETED],
			"CANCELED":  stateCounts[message_pb.Message_Metadata_CANCELED],
			"ERRORED":   stateCounts[message_pb.Message_Metadata_ERRORED],
		},
		EarliestDeadline: timestamppb.New(earliestDeadline),
	}, nil
}
