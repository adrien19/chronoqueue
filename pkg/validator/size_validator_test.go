package validator

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func TestSizeValidatorCountsHeadersInTotalMessageSizeWithoutChargingPayloadLimit(t *testing.T) {
	t.Run("headers count toward total message size without a payload", func(t *testing.T) {
		t.Setenv("CHRONOQUEUE_MAX_MESSAGE_SIZE", "16")
		message := headerMessage(&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte(strings.Repeat("x", 32))})

		result := NewSizeValidator(nil).Validate(context.Background(), message)

		require.False(t, result.Valid)
		assert.Equal(t, "message", result.Errors[0].Field)
	})

	t.Run("headers do not count toward payload data limit", func(t *testing.T) {
		t.Setenv("CHRONOQUEUE_MAX_MESSAGE_SIZE", "1024")
		t.Setenv("CHRONOQUEUE_MAX_PAYLOAD_SIZE", "2")
		message := headerMessage(&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte(strings.Repeat("x", 32))})
		message.Metadata.Payload = &commonpb.Payload{Data: &structpb.Struct{}}

		result := NewSizeValidator(nil).Validate(context.Background(), message)

		require.True(t, result.Valid, "errors: %v", result.Errors)
	})
}
