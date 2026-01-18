package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
)

func (s *Storage) CreateSchedule(ctx context.Context, schedule *schedulepb.Schedule) error {
	if schedule == nil {
		return fmt.Errorf("schedule is nil")
	}

	if schedule.Metadata == nil {
		schedule.Metadata = &schedulepb.Schedule_Metadata{}
	}

	now := s.Clock.NowMs()
	if schedule.Metadata.CreatedAt == nil {
		schedule.Metadata.CreatedAt = timestamppb.New(time.UnixMilli(now))
	}
	schedule.Metadata.UpdatedAt = timestamppb.New(time.UnixMilli(now))

	var cronExpr any
	if schedule.Metadata.GetCronSchedule() != "" {
		cronExpr = schedule.Metadata.GetCronSchedule()
	}

	if err := repositorycommon.EncryptSchedulePayload(schedule, s.KeyManager); err != nil {
		return fmt.Errorf("encrypt schedule payload: %w", err)
	}

	scheduleBytes, err := s.Serializer.MarshalSchedule(schedule)
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}

	var nextRunMs any
	if schedule.Metadata.GetNextRun() != nil {
		nextRunMs = schedule.Metadata.GetNextRun().AsTime().UnixMilli()
	}

	var lastRunMs any
	if schedule.Metadata.GetLastRun() != nil {
		lastRunMs = schedule.Metadata.GetLastRun().AsTime().UnixMilli()
	}

	query := `INSERT INTO cq_schedules (id, queue_name, metadata_pb, state, cron_schedule, next_run, last_run, execution_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = s.DB.ExecContext(ctx, query, schedule.GetScheduleId(), schedule.GetMetadata().GetQueueName(), scheduleBytes, schedule.GetMetadata().GetState(), cronExpr, nextRunMs, lastRunMs, 0, now, now)
	if err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}

	return nil
}

// GetSchedule retrieves a schedule by ID
func (s *Storage) GetSchedule(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error) {
	query := `SELECT metadata_pb FROM cq_schedules WHERE id = ?`
	var scheduleBytes []byte
	err := s.DB.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("schedule not found: %s", scheduleId)
	}
	if err != nil {
		return nil, fmt.Errorf("query schedule: %w", err)
	}

	schedule, err := s.Serializer.UnmarshalSchedule(scheduleBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal schedule: %w", err)
	}

	if err := repositorycommon.DecryptSchedulePayload(schedule, s.KeyManager); err != nil {
		return nil, fmt.Errorf("decrypt schedule payload: %w", err)
	}

	return schedule, nil
}

// ListSchedules returns schedules for a queue
func (s *Storage) ListSchedules(ctx context.Context, queueName string) ([]*schedulepb.Schedule, error) {
	var query string
	var rows *sql.Rows
	var err error

	if queueName == "" {
		// List all schedules
		query = `SELECT metadata_pb FROM cq_schedules ORDER BY id`
		rows, err = s.DB.QueryContext(ctx, query)
	} else {
		// Filter by queue name
		query = `SELECT metadata_pb FROM cq_schedules WHERE queue_name = ? ORDER BY id`
		rows, err = s.DB.QueryContext(ctx, query, queueName)
	}

	if err != nil {
		return nil, fmt.Errorf("query schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var schedules []*schedulepb.Schedule
	for rows.Next() {
		var scheduleBytes []byte
		if err := rows.Scan(&scheduleBytes); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}

		schedule, err := s.Serializer.UnmarshalSchedule(scheduleBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal schedule: %w", err)
		}

		if err := repositorycommon.DecryptSchedulePayload(schedule, s.KeyManager); err != nil {
			return nil, fmt.Errorf("decrypt schedule payload: %w", err)
		}

		schedules = append(schedules, schedule)
	}

	return schedules, rows.Err()
}

// DeleteSchedule deletes a schedule
func (s *Storage) DeleteSchedule(ctx context.Context, scheduleId string) error {
	query := `DELETE FROM cq_schedules WHERE id = ?`
	result, err := s.DB.ExecContext(ctx, query, scheduleId)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("schedule not found: %s", scheduleId)
	}

	return nil
}

// PauseSchedule pauses a schedule
func (s *Storage) PauseSchedule(ctx context.Context, scheduleId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get current schedule to update state in metadata
		var scheduleBytes []byte
		var currentState int32
		query := `SELECT metadata_pb, state FROM cq_schedules WHERE id = ?`
		err := tx.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes, &currentState)
		if err == sql.ErrNoRows {
			return fmt.Errorf("schedule not found: %s", scheduleId)
		}
		if err != nil {
			return fmt.Errorf("query schedule: %w", err)
		}

		// Unmarshal schedule to update metadata
		schedule, err := s.Serializer.UnmarshalSchedule(scheduleBytes)
		if err != nil {
			return fmt.Errorf("unmarshal schedule: %w", err)
		}

		// Update state to PAUSED
		if schedule.Metadata == nil {
			schedule.Metadata = &schedulepb.Schedule_Metadata{}
		}
		schedule.Metadata.State = schedulepb.Schedule_Metadata_PAUSED

		// Marshal updated schedule
		updatedBytes, err := s.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		// Update database
		updateQuery := `UPDATE cq_schedules SET state = ?, metadata_pb = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`
		result, err := tx.ExecContext(ctx, updateQuery, schedulepb.Schedule_Metadata_PAUSED, updatedBytes, scheduleId)
		if err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return fmt.Errorf("schedule not found: %s", scheduleId)
		}

		return nil
	})
}

// ResumeSchedule resumes a paused schedule
func (s *Storage) ResumeSchedule(ctx context.Context, scheduleId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get current schedule to update state in metadata
		var scheduleBytes []byte
		var currentState int32
		query := `SELECT metadata_pb, state FROM cq_schedules WHERE id = ?`
		err := tx.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes, &currentState)
		if err == sql.ErrNoRows {
			return fmt.Errorf("schedule not found: %s", scheduleId)
		}
		if err != nil {
			return fmt.Errorf("query schedule: %w", err)
		}

		// Unmarshal schedule to update metadata
		schedule, err := s.Serializer.UnmarshalSchedule(scheduleBytes)
		if err != nil {
			return fmt.Errorf("unmarshal schedule: %w", err)
		}

		// Update state to SCHEDULED
		if schedule.Metadata == nil {
			schedule.Metadata = &schedulepb.Schedule_Metadata{}
		}
		schedule.Metadata.State = schedulepb.Schedule_Metadata_SCHEDULED

		// Marshal updated schedule
		updatedBytes, err := s.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		// Update database
		updateQuery := `UPDATE cq_schedules SET state = ?, metadata_pb = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`
		result, err := tx.ExecContext(ctx, updateQuery, schedulepb.Schedule_Metadata_SCHEDULED, updatedBytes, scheduleId)
		if err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return fmt.Errorf("schedule not found: %s", scheduleId)
		}

		return nil
	})
}

// RecordScheduleExecution records a schedule execution
func (s *Storage) RecordScheduleExecution(ctx context.Context, scheduleId string, messageId string, executionTime int64) error {
	query := `INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success) VALUES (?, ?, ?, ?)`
	_, err := s.DB.ExecContext(ctx, query, scheduleId, messageId, executionTime, 1)
	if err != nil {
		return fmt.Errorf("insert schedule history: %w", err)
	}

	return nil
}

// GetScheduleHistory returns the execution history for a schedule
func (s *Storage) GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*schedulepb.ScheduleHistory, error) {
	// Get schedule metadata
	schedule, err := s.GetSchedule(ctx, scheduleId)
	if err != nil {
		return nil, fmt.Errorf("get schedule: %w", err)
	}

	// Query messages from history
	query := `
		SELECT m.metadata_pb
		FROM cq_schedule_history h
		JOIN cq_messages m ON h.message_id = m.id
		WHERE h.schedule_id = ?
		ORDER BY h.executed_at DESC
		LIMIT ?
	`

	rows, err := s.DB.QueryContext(ctx, query, scheduleId, limit)
	if err != nil {
		return nil, fmt.Errorf("query schedule history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*messagepb.Message
	for rows.Next() {
		var messageBytes []byte
		if err := rows.Scan(&messageBytes); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}

		msg, err := s.Serializer.UnmarshalMessage(messageBytes)
		if err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}

		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}

	history := &schedulepb.ScheduleHistory{
		Messages:   messages,
		ScheduleId: scheduleId,
	}

	// Populate schedule metadata if available
	if schedule.Metadata != nil {
		history.NextRun = schedule.Metadata.NextRun
		history.LastRun = schedule.Metadata.LastRun
		history.CreatedAt = schedule.Metadata.CreatedAt
		history.UpdatedAt = schedule.Metadata.UpdatedAt
	}

	return history, nil
}

// GetDLQMessages returns messages in the dead letter queue
