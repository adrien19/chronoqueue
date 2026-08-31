package validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

func TestContentTypeValidatorAcceptsJSONStruct(t *testing.T) {
	message := &messagepb.Message{Metadata: &messagepb.Message_Metadata{Payload: &commonpb.Payload{Data: &structpb.Struct{}}}}
	result := NewContentTypeValidator(&queuepb.QueueMetadata{}).Validate(context.Background(), message)
	require.True(t, result.Valid)
	require.Equal(t, "application/json", message.GetMetadata().GetPayload().GetContentType())
}

func TestContentTypeValidatorRejectsNonJSONFormatClaims(t *testing.T) {
	for _, contentType := range []string{"application/xml", "text/plain", "application/octet-stream"} {
		t.Run(contentType, func(t *testing.T) {
			message := &messagepb.Message{Metadata: &messagepb.Message_Metadata{Payload: &commonpb.Payload{Data: &structpb.Struct{}, ContentType: contentType}}}
			result := NewContentTypeValidator(&queuepb.QueueMetadata{}).Validate(context.Background(), message)
			require.False(t, result.Valid)
			require.Len(t, result.Errors, 1)
			require.Equal(t, "payload.content_type", result.Errors[0].Field)
		})
	}
}
