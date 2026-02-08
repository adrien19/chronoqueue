package common

import "errors"

// ErrDuplicateMessageID is returned when a message with the same ID already exists.
var ErrDuplicateMessageID = errors.New("duplicate message_id")
