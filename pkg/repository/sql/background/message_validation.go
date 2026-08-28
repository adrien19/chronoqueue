package background

import (
	"context"
	"errors"
	"fmt"
	"strings"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
	"github.com/adrien19/chronoqueue/pkg/validator"
)

type scheduledMessageValidationError struct {
	message string
}

func (e *scheduledMessageValidationError) Error() string {
	return e.message
}

func isScheduledMessageValidationError(err error) bool {
	var validationError *scheduledMessageValidationError
	return errors.As(err, &validationError)
}

func validateScheduledMessage(ctx context.Context, base *repositorysql.BaseSQL, queueMeta *queuepb.QueueMetadata, message *messagepb.Message) error {
	result := validator.NewPayloadValidator(queueMeta, base.SchemaRegistry()).Validate(ctx, message)
	if result.Valid {
		return nil
	}

	violations := make([]string, 0, len(result.Errors))
	for _, validationError := range result.Errors {
		violations = append(violations, fmt.Sprintf("%s: %s", validationError.Field, validationError.Message))
	}
	return &scheduledMessageValidationError{message: fmt.Sprintf("scheduled message validation failed: %s", strings.Join(violations, "; "))}
}
