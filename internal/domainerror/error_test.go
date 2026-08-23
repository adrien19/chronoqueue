package domainerror

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPC(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCode    codes.Code
		wantMessage string
	}{
		{name: "invalid argument", err: New(InvalidArgument, "invalid queue", errors.New("private detail")), wantCode: codes.InvalidArgument, wantMessage: "invalid queue"},
		{name: "not found", err: New(NotFound, "queue not found", nil), wantCode: codes.NotFound, wantMessage: "queue not found"},
		{name: "already exists", err: New(AlreadyExists, "queue already exists", nil), wantCode: codes.AlreadyExists, wantMessage: "queue already exists"},
		{name: "failed precondition", err: New(FailedPrecondition, "message is not running", nil), wantCode: codes.FailedPrecondition, wantMessage: "message is not running"},
		{name: "domain deadline", err: New(DeadlineExceeded, "lease expired", nil), wantCode: codes.DeadlineExceeded, wantMessage: "lease expired"},
		{name: "context deadline", err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded, wantMessage: "request deadline exceeded"},
		{name: "context canceled", err: context.Canceled, wantCode: codes.Canceled, wantMessage: "request canceled"},
		{name: "existing status", err: status.Error(codes.ResourceExhausted, "rate limited"), wantCode: codes.ResourceExhausted, wantMessage: "rate limited"},
		{name: "unclassified", err: errors.New("database password leaked"), wantCode: codes.Internal, wantMessage: "internal server error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converted := status.Convert(ToGRPC(tt.err))
			assert.Equal(t, tt.wantCode, converted.Code())
			assert.Equal(t, tt.wantMessage, converted.Message())
		})
	}
}

func TestToGRPCIncludesValidationDetails(t *testing.T) {
	err := InvalidWithFields("invalid message", []FieldViolation{{Field: "metadata.priority", Description: "must be between 0 and 4"}}, nil)
	grpcStatus := status.Convert(ToGRPC(err))
	require.Len(t, grpcStatus.Details(), 1)
	detail, ok := grpcStatus.Details()[0].(*errdetails.BadRequest)
	require.True(t, ok)
	require.Len(t, detail.GetFieldViolations(), 1)
	assert.Equal(t, "metadata.priority", detail.GetFieldViolations()[0].GetField())
}

func TestPrefixMessagePreservesDomainContract(t *testing.T) {
	cause := errors.New("private detail")
	err := PrefixMessage(New(NotFound, "queue not found", cause), "get queue metadata")

	grpcStatus := status.Convert(ToGRPC(err))
	assert.Equal(t, codes.NotFound, grpcStatus.Code())
	assert.Equal(t, "get queue metadata: queue not found", grpcStatus.Message())
	assert.ErrorIs(t, err, cause)
}

func TestPrefixMessageDoesNotClassifyUnknownError(t *testing.T) {
	err := PrefixMessage(errors.New("database password leaked"), "get queue metadata")

	grpcStatus := status.Convert(ToGRPC(err))
	assert.Equal(t, codes.Internal, grpcStatus.Code())
	assert.Equal(t, "internal server error", grpcStatus.Message())
}
