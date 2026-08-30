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
	Version int    `json:"v"`
	Scope   string `json:"s"`
	Filter  string `json:"f"`
	Offset  int64  `json:"o"`
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
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, errors.New("page token is malformed")
	}
	var value cursor
	if err := json.Unmarshal(data, &value); err != nil {
		return 0, errors.New("page token is malformed")
	}
	if value.Version != 1 || value.Scope != scope || value.Filter != filter || value.Offset < 0 {
		return 0, errors.New("page token does not match this request")
	}
	return value.Offset, nil
}

func Encode(scope, filter string, offset int64) (string, error) {
	if offset < 0 {
		return "", errors.New("page token offset must be non-negative")
	}
	data, err := json.Marshal(cursor{Version: 1, Scope: scope, Filter: filter, Offset: offset})
	if err != nil {
		return "", fmt.Errorf("marshal page token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}
