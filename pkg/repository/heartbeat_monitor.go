package repository

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	message_pb "github.com/adrien19/chronoqueue/api/message/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/lease"
	"github.com/adrien19/chronoqueue/internal/util"
)

func (as *storage) SendMessageHeartBeat(
	ctx context.Context,
	request *queueservice_pb.SendMessageHeartBeatRequest,
) (*queueservice_pb.SendMessageHeartBeatResponse, error) {
	as.logger.InfoWithFields("Processing heartbeat for message",
		"queue_name", request.GetQueueName(),
		"message_id", request.GetMessageId(),
		"stream_entry_id", request.GetStreamEntryId(),
	)

	queueName := request.GetQueueName()
	messageID := request.GetMessageId()
	streamEntryID := request.GetStreamEntryId()
	attemptID := request.GetAttemptId()

	// 1) Load message metadata
	meta, err := as.fetchMessageMetadata(ctx, queueName, messageID)
	if err != nil {
		return nil, util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.Internal,
			err,
			"Failed to fetch metadata for heartbeat.",
		).GRPCStatus()
	}
	if meta == nil {
		// Message not found; treat as gone.
		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         message_pb.Message_Metadata_COMPLETED,
		}, nil
	}

	// 2) Only RUNNING messages can be heartbeated
	if meta.State != message_pb.Message_Metadata_RUNNING {
		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}

	// 3) Validate attempt id to avoid stale heartbeats
	currentAttempt := meta.GetCurrentAttempt()
	if currentAttempt == nil || currentAttempt.GetAttemptId() == "" {
		// No current attempt => stale / invalid heartbeat
		as.logger.WarnWithFields("Heartbeat received for message without current attempt",
			"queue_name", queueName,
			"message_id", messageID,
			"attempt_id", attemptID,
		)
		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}
	if attemptID != currentAttempt.GetAttemptId() {
		// Heartbeat belongs to an old attempt; ignore
		as.logger.WarnWithFields("Heartbeat attempt_id mismatch; ignoring stale heartbeat",
			"queue_name", queueName,
			"message_id", messageID,
			"expected_attempt_id", currentAttempt.GetAttemptId(),
			"got_attempt_id", attemptID,
		)
		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}

	// 4) Get queue metadata for effective lease policy
	queueMeta, err := as.GetQueueMetadata(ctx, queueName)
	if err != nil {
		return nil, util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.Internal,
			err,
			"Failed to get queue metadata for heartbeat.",
		).GRPCStatus()
	}

	now := time.Now()

	// 5) Apply heartbeat at lease model level
	res := lease.HandleHeartbeat(queueMeta, meta, now)
	if res.ShouldFail {
		// Lease or heartbeat already timed out at this instant.
		oldState := meta.State
		// You *may* want to do retry/DLQ here instead of raw ERRORED,
		// but this matches your original ERRORED behavior on failure.
		meta.State = message_pb.Message_Metadata_ERRORED

		if err := as.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, oldState); err != nil {
			return nil, util.NewChronoError(
				util.ERROR_LEVEL_ERROR,
				codes.Internal,
				err,
				"Failed to save metadata for heartbeat timeout.",
			).GRPCStatus()
		}

		return &queueservice_pb.SendMessageHeartBeatResponse{
			RemainingTime: nil,
			State:         meta.State,
		}, nil
	}

	// 6) Always persist updated runtime (even if lease wasn't extended)
	// HandleHeartbeat has already updated metadata.current_attempt.* in-place.
	if err := as.saveMessageMetadataWithOldState(ctx, queueName, messageID, meta, meta.State); err != nil {
		return nil, util.NewChronoError(
			util.ERROR_LEVEL_ERROR,
			codes.Internal,
			err,
			"Failed to persist heartbeat runtime updates.",
		).GRPCStatus()
	}

	// 7) Reset stream idle time for this delivery via XCLAIM (optional but recommended)
	workerID := request.GetWorkerId()
	if workerID == "" && currentAttempt != nil {
		workerID = currentAttempt.GetWorkerId()
	}
	if workerID == "" {
		workerID = as.generateWorkerID()
	}
	consumerName := as.generateConsumerName(workerID)

	if streamEntryID != "" {
		if err := as.sendHeartbeat(ctx, queueName, consumerName, streamEntryID); err != nil {
			as.logger.ErrorWithFields("Failed to send heartbeat via XCLAIM",
				"queue_name", queueName,
				"message_id", messageID,
				"stream_entry_id", streamEntryID,
				"error", err,
			)
		}
	}

	// 8) Compute remaining time until lease expiry from runtime, not base lease
	currentAttempt = meta.GetCurrentAttempt()
	rt := lease.RuntimeFromProto(currentAttempt)
	remaining := max(rt.LeaseExpiry.Sub(now), 0)

	return &queueservice_pb.SendMessageHeartBeatResponse{
		RemainingTime: durationpb.New(remaining),
		State:         meta.State,
	}, nil
}
