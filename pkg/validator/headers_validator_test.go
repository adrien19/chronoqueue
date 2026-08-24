package validator

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
)

func TestHeadersValidatorAcceptsOptionalOrderedBinaryHeaders(t *testing.T) {
	tests := []struct {
		name    string
		message *messagepb.Message
	}{
		{name: "nil message"},
		{name: "nil metadata", message: &messagepb.Message{}},
		{name: "empty headers", message: headerMessage()},
		{
			name: "ordered duplicate keys and binary values",
			message: headerMessage(
				&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte{0x00, 0xff}},
				&messagepb.Message_Metadata_Header{Key: "trace-id", Value: []byte("second")},
			),
		},
	}

	validator := NewHeadersValidator()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validator.Validate(context.Background(), tt.message)
			require.True(t, result.Valid, "errors: %v", result.Errors)
			assert.Empty(t, result.Errors)
		})
	}
}

func TestHeadersValidatorRejectsInvalidHeaders(t *testing.T) {
	tests := []struct {
		name      string
		headers   []*messagepb.Message_Metadata_Header
		wantField string
		wantCode  schemapb.ErrorCode
	}{
		{
			name:      "nil header",
			headers:   []*messagepb.Message_Metadata_Header{nil},
			wantField: "metadata.headers[0]",
			wantCode:  schemapb.ErrorCode_REQUIRED_FIELD_MISSING,
		},
		{
			name:      "invalid key",
			headers:   []*messagepb.Message_Metadata_Header{{Key: "Trace_ID"}},
			wantField: "metadata.headers[0].key",
			wantCode:  schemapb.ErrorCode_INVALID_FORMAT,
		},
		{
			name:      "reserved prefix",
			headers:   []*messagepb.Message_Metadata_Header{{Key: "x-system-token"}},
			wantField: "metadata.headers[0].key",
			wantCode:  schemapb.ErrorCode_INVALID_FORMAT,
		},
		{
			name:      "value too large",
			headers:   []*messagepb.Message_Metadata_Header{{Key: "key", Value: []byte(strings.Repeat("v", MaxHeaderValueSize+1))}},
			wantField: "metadata.headers[0].value",
			wantCode:  schemapb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
		},
		{
			name: "aggregate too large",
			headers: func() []*messagepb.Message_Metadata_Header {
				headers := make([]*messagepb.Message_Metadata_Header, 9)
				for i := range headers {
					headers[i] = &messagepb.Message_Metadata_Header{Key: "key", Value: []byte(strings.Repeat("v", MaxHeaderValueSize))}
				}
				return headers
			}(),
			wantField: "metadata.headers",
			wantCode:  schemapb.ErrorCode_PAYLOAD_SIZE_EXCEEDED,
		},
	}

	validator := NewHeadersValidator()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validator.Validate(context.Background(), headerMessage(tt.headers...))
			require.False(t, result.Valid)
			require.NotEmpty(t, result.Errors)
			assert.Equal(t, tt.wantField, result.Errors[len(result.Errors)-1].Field)
			assert.Equal(t, tt.wantCode, result.Errors[len(result.Errors)-1].ErrorCode)
		})
	}
}

func headerMessage(headers ...*messagepb.Message_Metadata_Header) *messagepb.Message {
	return &messagepb.Message{Metadata: &messagepb.Message_Metadata{Headers: headers}}
}
