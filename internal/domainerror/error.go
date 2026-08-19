package domainerror

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Kind uint8

const (
	InvalidArgument Kind = iota
	NotFound
	AlreadyExists
	FailedPrecondition
	DeadlineExceeded
)

type Error struct {
	kind    Kind
	message string
	cause   error
	fields  []FieldViolation
}

type FieldViolation struct {
	Field       string
	Description string
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) Unwrap() error {
	return e.cause
}

func New(kind Kind, message string, cause error) error {
	return &Error{kind: kind, message: message, cause: cause}
}

func InvalidWithFields(message string, fields []FieldViolation, cause error) error {
	return &Error{kind: InvalidArgument, message: message, cause: cause, fields: fields}
}

func PrefixMessage(err error, prefix string) error {
	var domainErr *Error
	if !errors.As(err, &domainErr) {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return &Error{
		kind:    domainErr.kind,
		message: fmt.Sprintf("%s: %s", prefix, domainErr.message),
		cause:   err,
		fields:  domainErr.fields,
	}
}

func ToGRPC(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	if grpcStatus, ok := status.FromError(err); ok && grpcStatus.Code() != codes.Unknown {
		return err
	}

	var domainErr *Error
	if !errors.As(err, &domainErr) {
		return status.Error(codes.Internal, "internal server error")
	}

	grpcStatus := status.New(codeForKind(domainErr.kind), domainErr.message)
	if domainErr.kind == InvalidArgument && len(domainErr.fields) > 0 {
		violations := make([]*errdetails.BadRequest_FieldViolation, 0, len(domainErr.fields))
		for _, field := range domainErr.fields {
			violations = append(violations, &errdetails.BadRequest_FieldViolation{Field: field.Field, Description: field.Description})
		}
		withDetails, detailErr := grpcStatus.WithDetails(&errdetails.BadRequest{FieldViolations: violations})
		if detailErr == nil {
			grpcStatus = withDetails
		}
	}
	return grpcStatus.Err()
}

func codeForKind(kind Kind) codes.Code {
	switch kind {
	case InvalidArgument:
		return codes.InvalidArgument
	case NotFound:
		return codes.NotFound
	case AlreadyExists:
		return codes.AlreadyExists
	case FailedPrecondition:
		return codes.FailedPrecondition
	case DeadlineExceeded:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}
