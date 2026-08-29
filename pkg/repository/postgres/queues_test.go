package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
)

func TestValidateQueueDLQPair(t *testing.T) {
	tests := []struct {
		name   string
		source *queuepb.Queue
		dlq    *queuepb.Queue
		valid  bool
	}{
		{
			name:   "matching relationship",
			source: &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}},
			dlq:    &queuepb.Queue{Name: "source-dlq"},
			valid:  true,
		},
		{name: "nil source", dlq: &queuepb.Queue{Name: "source-dlq"}},
		{name: "nil DLQ", source: &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}},
		{
			name:   "empty target",
			source: &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{}},
			dlq:    &queuepb.Queue{Name: "source-dlq"},
		},
		{
			name:   "empty DLQ name",
			source: &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}},
			dlq:    &queuepb.Queue{},
		},
		{
			name:   "mismatched target",
			source: &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "missing-dlq"}},
			dlq:    &queuepb.Queue{Name: "created-dlq"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateQueueDLQPair(test.source, test.dlq)
			if test.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
		})
	}
}
