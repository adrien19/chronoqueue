package pagination

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	DefaultPageSize int32 = 100
	MaxPageSize     int32 = 1000
)

type cursor struct {
	Version  int    `json:"v"`
	Scope    string `json:"s"`
	Filter   string `json:"f"`
	Offset   int64  `json:"o,omitempty"`
	Position string `json:"p,omitempty"`
	Priority *int64 `json:"priority,omitempty"`
	RowID    *int64 `json:"row_id,omitempty"`
}

func DecodePeek(token, scope, filter string) (int64, int64, error) {
	if token == "" {
		return 0, 0, nil
	}
	value, err := decode(token)
	if err != nil {
		return 0, 0, err
	}
	if value.Version != 2 || value.Scope != scope || value.Filter != filter || value.Priority == nil || value.RowID == nil || *value.RowID <= 0 || value.Offset != 0 || value.Position != "" {
		return 0, 0, errors.New("page token does not match this request")
	}
	return *value.Priority, *value.RowID, nil
}

func EncodePeek(scope, filter string, priority, rowID int64) (string, error) {
	if rowID <= 0 {
		return "", errors.New("page token row id must be positive")
	}
	return encode(cursor{Version: 2, Scope: scope, Filter: filter, Priority: &priority, RowID: &rowID})
}

func DecodePosition(token, scope, filter string) (string, error) {
	if token == "" {
		return "", nil
	}
	value, err := decode(token)
	if err != nil {
		return "", err
	}
	if value.Version != 1 || value.Scope != scope || value.Filter != filter || value.Position == "" || value.Offset != 0 || value.Priority != nil || value.RowID != nil {
		return "", errors.New("page token does not match this request")
	}
	return value.Position, nil
}

func EncodePosition(scope, filter, position string) (string, error) {
	if position == "" {
		return "", errors.New("page token position must not be empty")
	}
	return encode(cursor{Version: 1, Scope: scope, Filter: filter, Position: position})
}

func PageSize(requested int32) (int32, error) {
	if requested < 0 || requested > MaxPageSize {
		return 0, fmt.Errorf("page size must be between 0 and %d", MaxPageSize)
	}
	if requested == 0 {
		return DefaultPageSize, nil
	}
	return requested, nil
}

func Decode(token, scope, filter string) (int64, error) {
	if token == "" {
		return 0, nil
	}
	value, err := decode(token)
	if err != nil {
		return 0, err
	}
	if value.Version != 1 || value.Scope != scope || value.Filter != filter || value.Offset < 0 || value.Position != "" || value.Priority != nil || value.RowID != nil {
		return 0, errors.New("page token does not match this request")
	}
	return value.Offset, nil
}

func Encode(scope, filter string, offset int64) (string, error) {
	if offset < 0 {
		return "", errors.New("page token offset must be non-negative")
	}
	return encode(cursor{Version: 1, Scope: scope, Filter: filter, Offset: offset})
}

func decode(token string) (cursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor{}, errors.New("page token is malformed")
	}
	var value cursor
	if err := json.Unmarshal(data, &value); err != nil {
		return cursor{}, errors.New("page token is malformed")
	}
	return value, nil
}

func encode(value cursor) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}
