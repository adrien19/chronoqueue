package common

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func TestProtoSerializerPreservesOrderedBinaryMessageHeaders(t *testing.T) {
	original := &messagepb.Message{
		MessageId: "message-with-headers",
		Metadata: &messagepb.Message_Metadata{
			Headers: []*messagepb.Message_Metadata_Header{
				{Key: "trace-id", Value: []byte{0x00, 0xff}},
				{Key: "trace-id", Value: []byte("second")},
			},
		},
	}
	serializer := NewProtoSerializer()

	encoded, err := serializer.MarshalMessage(original)
	require.NoError(t, err)
	decoded, err := serializer.UnmarshalMessage(encoded)
	require.NoError(t, err)

	require.True(t, proto.Equal(original, decoded))
}
