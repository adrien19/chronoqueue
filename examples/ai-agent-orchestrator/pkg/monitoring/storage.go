package monitoring

import (
	"context"

	"github.com/adrien19/chronoqueue/client"
)

// StorageMonitor provides ChronoQueue API-based monitoring
type StorageMonitor struct {
	chronoClient *client.ChronoQueueClient
}

// NewStorageMonitor creates a new monitor using ChronoQueue client
func NewStorageMonitor(chronoClient *client.ChronoQueueClient) *StorageMonitor {
	return &StorageMonitor{
		chronoClient: chronoClient,
	}
}

// QueueStateCounts represents message counts by state for a queue
type QueueStateCounts struct {
	Invisible int32
	Pending   int32
	Running   int32
	Completed int32
	Canceled  int32
	Errored   int32
}

// GetQueueState returns comprehensive queue state using ChronoQueue API
func (rm *StorageMonitor) GetQueueState(ctx context.Context, queueName string) (*QueueStateCounts, error) {
	resp, err := rm.chronoClient.GetQueueState(ctx, queueName)
	if err != nil {
		return nil, err
	}

	return &QueueStateCounts{
		Invisible: resp.StateCounts["INVISIBLE"],
		Pending:   resp.StateCounts["PENDING"],
		Running:   resp.StateCounts["RUNNING"],
		Completed: resp.StateCounts["COMPLETED"],
		Canceled:  resp.StateCounts["CANCELED"],
		Errored:   resp.StateCounts["ERRORED"],
	}, nil
}

// GetDLQSize returns the number of errored messages (using queue state)
func (rm *StorageMonitor) GetDLQSize(ctx context.Context, queueName string) (int64, error) {
	state, err := rm.GetQueueState(ctx, queueName)
	if err != nil {
		return 0, err
	}
	return int64(state.Errored), nil
}

// Close closes the ChronoQueue client connection
func (rm *StorageMonitor) Close() error {
	if rm.chronoClient != nil {
		rm.chronoClient.Close()
	}
	return nil
}
