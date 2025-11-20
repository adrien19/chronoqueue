package monitoring

import (
	"context"

	"github.com/adrien19/chronoqueue/client"
)

// RedisMonitor provides ChronoQueue API-based monitoring
type RedisMonitor struct {
	chronoClient *client.ChronoQueueClient
}

// NewRedisMonitor creates a new monitor using ChronoQueue client
func NewRedisMonitor(chronoClient *client.ChronoQueueClient) *RedisMonitor {
	return &RedisMonitor{
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
func (rm *RedisMonitor) GetQueueState(ctx context.Context, queueName string) (*QueueStateCounts, error) {
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
func (rm *RedisMonitor) GetDLQSize(ctx context.Context, queueName string) (int64, error) {
	state, err := rm.GetQueueState(ctx, queueName)
	if err != nil {
		return 0, err
	}
	return int64(state.Errored), nil
}

// Close closes the ChronoQueue client connection
func (rm *RedisMonitor) Close() error {
	if rm.chronoClient != nil {
		rm.chronoClient.Close()
	}
	return nil
}
