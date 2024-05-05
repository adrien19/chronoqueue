package repository

import (
	"context"
	"fmt"
	"strings"

	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"google.golang.org/grpc/codes"
)

func (as *storage) ListQueues(ctx context.Context, request *queueservice_pb.ListQueuesRequest) (*queueservice_pb.ListQueuesResponse, error) {
	queueMetadataIDs, err := as.listMetadataIDs(ctx, "queue", request.GetPrefix(), 10)
	if err != nil {
		return nil, err
	}

	queues := make([]*queue_pb.Queue, len(queueMetadataIDs))
	for i, queueMetadataID := range queueMetadataIDs {
		queueID := strings.Split(queueMetadataID, ":")[0]
		metadata, err := as.getQueueMetadata(ctx, queueID)
		if err != nil {
			msg := fmt.Sprintf("error fetching metadata for queue %s", queueID)
			chronoErr := util.NewChronoError(util.ERROR_LEVEL_ERROR, codes.Internal, err, msg)
			return nil, chronoErr.GRPCStatus()
		}

		queues[i] = &queue_pb.Queue{
			Name:     queueID,
			Metadata: metadata,
		}
	}

	return &queueservice_pb.ListQueuesResponse{
		Queues: queues,
	}, nil
}
