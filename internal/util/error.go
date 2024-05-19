package util

import (
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrUnknown = errors.New("unknown argument passed")

	ErrInvalidArgument = errors.New("invalid argument passed")
	log                *logrus.Logger
)

type ErrorLevel int

const (
	ERROR_LEVEL_INFO ErrorLevel = iota
	ERROR_LEVEL_ERROR
	ERROR_LEVEL_DEBUG
)

type ChronoError struct {
	Level   ErrorLevel
	Code    codes.Code // gRPC status code
	Message string
	Err     error
}

func (ce *ChronoError) Error() string {
	return ce.Err.Error()
}

// Convert the ChronoError to a gRPC status error
func (e *ChronoError) GRPCStatus() error {
	errMsg := e.Message
	if e.Err != nil {
		errMsg = fmt.Sprintf("%s: %s", e.Message, e.Err.Error())
	}
	return status.Errorf(e.Code, errMsg)
}

func NewChronoError(level ErrorLevel, code codes.Code, err error, msg string) *ChronoError {
	return &ChronoError{
		Level:   level,
		Code:    code,
		Err:     err,
		Message: msg,
	}
}
