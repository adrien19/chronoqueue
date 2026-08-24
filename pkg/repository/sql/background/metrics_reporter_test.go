package background

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

type stubRowIterator struct {
	rows    [][]any
	next    int
	rowErr  error
	scanErr error
}

func (s *stubRowIterator) Next() bool {
	if s.next >= len(s.rows) {
		return false
	}
	s.next++
	return true
}

func (s *stubRowIterator) Scan(dest ...any) error {
	if s.scanErr != nil {
		return s.scanErr
	}
	row := s.rows[s.next-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan destination count %d does not match row length %d", len(dest), len(row))
	}
	for i, value := range row {
		switch target := dest[i].(type) {
		case *string:
			*target = value.(string)
		case *int32:
			*target = value.(int32)
		case *int64:
			*target = value.(int64)
		default:
			return fmt.Errorf("unsupported scan destination %T", dest[i])
		}
	}
	return nil
}

func (s *stubRowIterator) Err() error {
	return s.rowErr
}

func TestCollectQueueNames(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		names, err := collectQueueNames(&stubRowIterator{rows: [][]any{{"queue-a"}, {"queue-b"}}})
		require.NoError(t, err)
		require.Equal(t, []string{"queue-a", "queue-b"}, names)
	})

	t.Run("iteration failure", func(t *testing.T) {
		iterationErr := errors.New("iteration failed")
		_, err := collectQueueNames(&stubRowIterator{rowErr: iterationErr})
		require.ErrorIs(t, err, iterationErr)
	})

	t.Run("scan failure", func(t *testing.T) {
		scanErr := errors.New("scan failed")
		_, err := collectQueueNames(&stubRowIterator{
			rows:    [][]any{{"queue-a"}},
			scanErr: scanErr,
		})
		require.ErrorIs(t, err, scanErr)
	})
}

func TestUpdateMessagesByStateMetrics(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		err := updateMessagesByStateMetrics("queue", &stubRowIterator{rows: [][]any{
			{int32(messagepb.Message_Metadata_PENDING), int64(2)},
			{int32(messagepb.Message_Metadata_RUNNING), int64(1)},
		}})
		require.NoError(t, err)
	})

	t.Run("iteration failure", func(t *testing.T) {
		iterationErr := errors.New("iteration failed")
		err := updateMessagesByStateMetrics("queue", &stubRowIterator{rowErr: iterationErr})
		require.ErrorIs(t, err, iterationErr)
	})

	t.Run("scan failure", func(t *testing.T) {
		scanErr := errors.New("scan failed")
		err := updateMessagesByStateMetrics("queue", &stubRowIterator{
			rows:    [][]any{{int32(messagepb.Message_Metadata_PENDING), int64(2)}},
			scanErr: scanErr,
		})
		require.ErrorIs(t, err, scanErr)
	})
}
