package log

import (
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
)

var errMissingValue = errors.New("(MISSING)")

type Logger struct {
	logger *logrus.Logger
}

type Option func(*Logger)

// NewLogger creates a new logger with the specified log level and log format.
// The log level is determined by the LOG_LEVEL environment variable, defaulting to "info".
// The log format is determined by the LOG_FORMAT environment variable, defaulting to "text".
func NewLogger(options ...Option) *Logger {
	log := &Logger{
		logger: logrus.New(),
	}
	log.logger.SetLevel(logrus.InfoLevel)
	log.logger.SetFormatter(&logrus.TextFormatter{})

	for _, optFunc := range options {
		optFunc(log)
	}

	return log
}

// WithLevel configures a logrus logger with the specified level
func WithLevel(level logrus.Level) Option {
	return func(log *Logger) {
		log.logger.SetLevel(level)
	}
}

// WithFormatter configures a logrus logger with the specified formatter
func WithFormatter(formatter logrus.Formatter) Option {
	return func(log *Logger) {
		log.logger.SetFormatter(formatter)
	}
}

func (log *Logger) Debug(args ...interface{}) {
	log.logger.Debug(args...)
}

func (log *Logger) DebugWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Debug(message)
}

func (log *Logger) Info(args ...interface{}) {
	log.logger.Info(args...)
}

func (log *Logger) InfoWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Info(message)
}

func (log *Logger) Warn(args ...interface{}) {
	log.logger.Warn(args...)
}

func (log *Logger) WarnWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Warn(message)
}

func (log *Logger) Error(args ...interface{}) {
	log.logger.Error(args...)
}

func (log *Logger) ErrorWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Error(message)
}

func (log *Logger) Fatal(args ...interface{}) {
	log.logger.Fatal(args...)
}

func (log *Logger) FatalWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Fatal(message)
}

func (log *Logger) Panic(args ...interface{}) {
	log.logger.Panic(args...)
}

func (log *Logger) PanicWithFields(message string, fields ...interface{}) {
	parsedFields := parseLogrusFields(fields)
	log.logger.WithFields(parsedFields).Panic(message)
}

// func parseLogrusFields(keyvals ...interface{}) logrus.Fields {
// 	fields := logrus.Fields{}
// 	for i := 0; i < len(keyvals); i += 2 {
// 		if i+1 < len(keyvals) {
// 			fields[fmt.Sprint(keyvals[i])] = keyvals[i+1]
// 		} else {
// 			fields[fmt.Sprint(keyvals[i])] = errMissingValue
// 		}
// 	}
// 	return fields
// }

func parseLogrusFields(keyvals ...interface{}) logrus.Fields {
	fields := logrus.Fields{}
	for i := 0; i < len(keyvals); i++ {
		switch v := keyvals[i].(type) {
		case []interface{}:
			for j := 0; j < len(v); j += 2 {
				if j+1 < len(v) {
					fields[fmt.Sprint(v[j])] = v[j+1]
				} else {
					fields[fmt.Sprint(v[j])] = errMissingValue
				}
			}
		default:
			if i+1 < len(keyvals) {
				fields[fmt.Sprint(keyvals[i])] = keyvals[i+1]
			} else {
				fields[fmt.Sprint(keyvals[i])] = errMissingValue
			}
			i++ // Skip next value since it's already used
		}
	}
	return fields
}
