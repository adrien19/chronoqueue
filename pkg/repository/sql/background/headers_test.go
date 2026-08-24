package background

import (
	"testing"

	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func TestCloneHeadersPreservesOrderAndCopiesValues(t *testing.T) {
	value := []byte{0x00, 0xff}
	original := []*messagepb.Message_Metadata_Header{
		{Key: "trace-id", Value: value},
		{Key: "trace-id", Value: []byte("second")},
	}

	cloned := cloneHeaders(original)
	require.Len(t, cloned, 2)
	require.Equal(t, "trace-id", cloned[0].GetKey())
	require.Equal(t, []byte{0x00, 0xff}, cloned[0].GetValue())
	require.Equal(t, []byte("second"), cloned[1].GetValue())

	value[0] = 0x7f
	require.Equal(t, []byte{0x00, 0xff}, cloned[0].GetValue())
	require.Nil(t, cloneHeaders(nil))
}
