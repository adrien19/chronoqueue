package util

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

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

// Define a custom error type
// type ChronoError struct {
// 	Level   ErrorLevel
// 	Message string
// 	Err     error
// }

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

// func NewChronoError(level ErrorLevel, err error, msg string) *ChronoError {
// 	return &ChronoError{
// 		Level:   level,
// 		Err:     err,
// 		Message: msg,
// 	}
// }

func NewChronoError(level ErrorLevel, code codes.Code, err error, msg string) *ChronoError {
	return &ChronoError{
		Level:   level,
		Code:    code,
		Err:     err,
		Message: msg,
	}
}

type ChronoHandlerFunc func(ctx context.Context, req interface{}) (interface{}, error)

// ErrorHandler to handle ChronoError
func ErrorHandler(handler ChronoHandlerFunc, defaultResp interface{}) ChronoHandlerFunc {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			// Check if it's a ChronoError
			if ce, ok := err.(*ChronoError); ok {
				switch ce.Level {
				case ERROR_LEVEL_INFO:
					log.Info(ce.Message)
				case ERROR_LEVEL_ERROR:
					log.WithError(ce.Err).Error(ce.Message)
				case ERROR_LEVEL_DEBUG:
					log.Debug(ce.Message)
				default:
					log.WithError(ce.Err).Warn(ce.Message)
				}
			} else {
				log.Error(err)
			}
			return defaultResp, err
		}
		return resp, nil
	}
}

func HttpLoggerHandler(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		inner.ServeHTTP(w, r)

		log.Printf(
			"%s\t%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			name,
			time.Since(start),
		)
	})
}

func init() {
	log = logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.DebugLevel)
}

func init() {
	log = logrus.New()
	// log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.DebugLevel)
}

func Debug(args ...interface{}) {
	log.Debug(args...)
}

func DebugWithFields(message string, fields map[string]interface{}) {
	log.WithFields(fields).Debug(message)
}

func Info(args ...interface{}) {
	log.Info(args...)
}

func InfoWithFields(message string, fields map[string]interface{}) {
	log.WithFields(fields).Info(message)
}

func Warn(args ...interface{}) {
	log.Warn(args...)
}

func WarnWithFields(message string, fields map[string]interface{}) {
	log.WithFields(fields).Warn(message)
}

func Error(args ...interface{}) {
	log.Error(args...)
}

func ErrorWithFields(message string, fields map[string]interface{}) {
	log.WithFields(fields).Error(message)
}

func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

func FatalWithFields(message string, fields map[string]interface{}) {
	log.WithFields(fields).Fatal(message)
}

func WithFields(fields map[string]interface{}) *logrus.Entry {
	return log.WithFields(fields)
}
