package sql

import (
	"fmt"
	"strconv"
	"strings"
)

func EncodeHistoryCursor(executedAt, id int64) string {
	return fmt.Sprintf("%d:%d", executedAt, id)
}

func DecodeHistoryCursor(cursor string) (int64, int64, error) {
	if cursor == "" {
		return 0, 0, nil
	}
	parts := strings.Split(cursor, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid history cursor")
	}
	executedAt, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse history cursor time: %w", err)
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse history cursor id: %w", err)
	}
	return executedAt, id, nil
}
