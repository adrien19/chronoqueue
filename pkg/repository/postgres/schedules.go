package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

func (s *Storage) CreateSchedule(ctx context.Context, schedule *schedulepb.Schedule) error {
	if schedule == nil {
		return fmt.Errorf("schedule is nil")
	}

	if schedule.Metadata == nil {
		schedule.Metadata = &schedulepb.Schedule_Metadata{}
	}

	now := s.nowMs()
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

	query := s.ph(`INSERT INTO cq_schedules (id, queue_name, metadata_pb, state, cron_schedule, next_run, last_run, execution_count, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	_, err = s.DB.ExecContext(ctx, query, schedule.GetScheduleId(), schedule.GetMetadata().GetQueueName(), scheduleBytes, schedule.GetMetadata().GetState(), cronExpr, nextRunMs, lastRunMs, 0, now, now)
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return domainerror.New(domainerror.AlreadyExists, fmt.Sprintf("schedule %q already exists", schedule.GetScheduleId()), err)
		}
		return fmt.Errorf("insert schedule: %w", err)
	}

	return nil
}

// GetSchedule retrieves a schedule by ID.
func (s *Storage) GetSchedule(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error) {
	query := s.ph(`SELECT metadata_pb FROM cq_schedules WHERE id = ?`)
	var scheduleBytes []byte
	err := s.DB.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes)
	if err == sql.ErrNoRows {
		return nil, domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), err)
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

// ListSchedules returns schedules for a queue.
func (s *Storage) ListSchedules(ctx context.Context, queueName string) ([]*schedulepb.Schedule, error) {
	var query string
	var rows *sql.Rows
	var err error

	if queueName == "" {
		// List all schedules
		query = s.ph(`SELECT metadata_pb FROM cq_schedules ORDER BY id`)
		rows, err = s.DB.QueryContext(ctx, query)
	} else {
		// Filter by queue name
		query = s.ph(`SELECT metadata_pb FROM cq_schedules WHERE queue_name = ? ORDER BY id`)
		rows, err = s.DB.QueryContext(ctx, query, queueName)
	}

	if err != nil {
		return nil, fmt.Errorf("query schedules: %w", err)
	}
	schedules, scanErr := s.scanSchedules(rows)
	closeErr := rows.Close()
	if scanErr != nil {
		return nil, scanErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close schedule rows: %w", closeErr)
	}
	return schedules, nil
}

func (s *Storage) scanSchedules(rows *sql.Rows) ([]*schedulepb.Schedule, error) {
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

// ListSchedulesWithPrefix returns schedules whose IDs start with prefix.
func (s *Storage) ListSchedulesWithPrefix(ctx context.Context, prefix string) ([]*schedulepb.Schedule, error) {
	schedules, _, err := s.ListSchedulesPage(ctx, prefix, int32(^uint32(0)>>1), "")
	return schedules, err
}

func (s *Storage) ListSchedulesPage(ctx context.Context, prefix string, limit int32, cursor string) ([]*schedulepb.Schedule, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT metadata_pb FROM cq_schedules WHERE STRPOS(id, $1) = 1 AND id > $2 ORDER BY id LIMIT $3`, prefix, cursor, int64(limit)+1)
	if err != nil {
		return nil, "", fmt.Errorf("query schedules: %w", err)
	}

	schedules, scanErr := s.scanSchedules(rows)
	closeErr := rows.Close()
	if scanErr != nil {
		return nil, "", scanErr
	}
	if closeErr != nil {
		return nil, "", fmt.Errorf("close schedule rows: %w", closeErr)
	}
	if len(schedules) <= int(limit) {
		return schedules, "", nil
	}
	schedules = schedules[:limit]
	return schedules, schedules[len(schedules)-1].GetScheduleId(), nil
}

// DeleteSchedule deletes a schedule.
func (s *Storage) DeleteSchedule(ctx context.Context, scheduleId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		var scheduleBytes []byte
		if err := tx.QueryRowContext(ctx, s.ph(`SELECT metadata_pb FROM cq_schedules WHERE id = ? FOR UPDATE`), scheduleId).Scan(&scheduleBytes); err != nil {
			if err == sql.ErrNoRows {
				return domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), err)
			}
			return fmt.Errorf("query schedule for deletion: %w", err)
		}
		if _, err := tx.ExecContext(ctx, s.ph(`INSERT INTO cq_schedule_archive (schedule_id, metadata_pb, deleted_at) VALUES (?, ?, ?) ON CONFLICT (schedule_id) DO UPDATE SET metadata_pb = EXCLUDED.metadata_pb, deleted_at = EXCLUDED.deleted_at`), scheduleId, scheduleBytes, s.nowMs()); err != nil {
			return fmt.Errorf("archive schedule: %w", err)
		}
		if _, err := tx.ExecContext(ctx, s.ph(`DELETE FROM cq_schedules WHERE id = ?`), scheduleId); err != nil {
			return fmt.Errorf("delete schedule: %w", err)
		}
		return nil
	})
}

// PauseSchedule pauses a schedule.
func (s *Storage) PauseSchedule(ctx context.Context, scheduleId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get current schedule to update state in metadata
		var scheduleBytes []byte
		var currentState int32
		query := s.ph(`SELECT metadata_pb, state FROM cq_schedules WHERE id = ? FOR UPDATE`)
		err := tx.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes, &currentState)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), err)
		}
		if err != nil {
			return fmt.Errorf("query schedule: %w", err)
		}
		if schedulepb.Schedule_Metadata_State(currentState) != schedulepb.Schedule_Metadata_SCHEDULED {
			return domainerror.New(domainerror.FailedPrecondition, "schedule is not scheduled", nil)
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
		nowMs := s.nowMs()
		schedule.Metadata.UpdatedAt = timestamppb.New(time.UnixMilli(nowMs))

		// Marshal updated schedule
		updatedBytes, err := s.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		// Update database
		updateQuery := s.ph(`UPDATE cq_schedules SET state = ?, metadata_pb = ?, updated_at = ? WHERE id = ?`)
		result, err := tx.ExecContext(ctx, updateQuery, schedulepb.Schedule_Metadata_PAUSED, updatedBytes, nowMs, scheduleId)
		if err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), nil)
		}

		return nil
	})
}

// ResumeSchedule resumes a paused schedule.
func (s *Storage) ResumeSchedule(ctx context.Context, scheduleId string) error {
	return s.WithTransaction(ctx, nil, func(tx *sql.Tx) error {
		// Get current schedule to update state in metadata
		var scheduleBytes []byte
		var currentState int32
		query := s.ph(`SELECT metadata_pb, state FROM cq_schedules WHERE id = ? FOR UPDATE`)
		err := tx.QueryRowContext(ctx, query, scheduleId).Scan(&scheduleBytes, &currentState)
		if err == sql.ErrNoRows {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), err)
		}
		if err != nil {
			return fmt.Errorf("query schedule: %w", err)
		}
		if schedulepb.Schedule_Metadata_State(currentState) != schedulepb.Schedule_Metadata_PAUSED {
			return domainerror.New(domainerror.FailedPrecondition, "schedule is not paused", nil)
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
		schedule.Metadata.StateMessage = ""
		nowMs := s.nowMs()
		schedule.Metadata.UpdatedAt = timestamppb.New(time.UnixMilli(nowMs))

		// Marshal updated schedule
		updatedBytes, err := s.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		// Update database
		updateQuery := s.ph(`UPDATE cq_schedules SET state = ?, metadata_pb = ?, updated_at = ?, execution_count = 0 WHERE id = ?`)
		result, err := tx.ExecContext(ctx, updateQuery, schedulepb.Schedule_Metadata_SCHEDULED, updatedBytes, nowMs, scheduleId)
		if err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("get rows affected: %w", err)
		}
		if rows == 0 {
			return domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), nil)
		}

		return nil
	})
}

// RecordScheduleExecution records a schedule execution.
func (s *Storage) RecordScheduleExecution(ctx context.Context, scheduleId string, messageId string, executionTime int64) error {
	query := s.ph(`INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success, message_pb) SELECT ?, ?, ?, ?, m.metadata_pb FROM cq_messages m JOIN cq_schedules sc ON sc.queue_name = m.queue_name WHERE sc.id = ? AND m.message_id = ?`)
	result, err := s.DB.ExecContext(ctx, query, scheduleId, messageId, executionTime, 1, scheduleId, messageId)
	if err != nil {
		return fmt.Errorf("insert schedule history: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get inserted history rows: %w", err)
	}
	if rows == 0 {
		return domainerror.New(domainerror.NotFound, fmt.Sprintf("scheduled message %q not found", messageId), nil)
	}

	return nil
}

// GetScheduleHistory returns the execution history for a schedule
func (s *Storage) GetScheduleHistory(ctx context.Context, scheduleId string, limit int64) (*schedulepb.ScheduleHistory, error) {
	if limit < 0 || limit > math.MaxInt32 {
		return nil, domainerror.New(domainerror.InvalidArgument, "schedule history limit must fit within int32", nil)
	}
	history, _, err := s.GetScheduleHistoryPage(ctx, scheduleId, int32(limit), "")
	return history, err
}

func (s *Storage) GetScheduleHistoryPage(ctx context.Context, scheduleId string, limit int32, cursor string) (*schedulepb.ScheduleHistory, string, error) {
	schedule, err := s.getScheduleForHistory(ctx, scheduleId)
	if err != nil {
		return nil, "", err
	}
	cursorTime, cursorID, err := repositorysql.DecodeHistoryCursor(cursor)
	if err != nil {
		return nil, "", domainerror.New(domainerror.InvalidArgument, "invalid schedule history cursor", err)
	}

	query := s.ph(`
		SELECT id, message_id, executed_at, success, error_message, message_pb
		FROM cq_schedule_history
		WHERE schedule_id = ? AND (? = '' OR executed_at < ? OR (executed_at = ? AND id < ?))
		ORDER BY executed_at DESC, id DESC
		LIMIT ?
	`)

	rows, err := s.DB.QueryContext(ctx, query, scheduleId, cursor, cursorTime, cursorTime, cursorID, int64(limit)+1)
	if err != nil {
		return nil, "", fmt.Errorf("query schedule history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*messagepb.Message
	var executions []*schedulepb.ScheduleHistory_Execution
	var rowIDs []int64
	for rows.Next() {
		var (
			rowID        int64
			messageID    string
			executedAt   int64
			success      int32
			errorMessage sql.NullString
			messageBytes []byte
		)
		if err := rows.Scan(&rowID, &messageID, &executedAt, &success, &errorMessage, &messageBytes); err != nil {
			return nil, "", fmt.Errorf("scan schedule execution: %w", err)
		}

		execution := &schedulepb.ScheduleHistory_Execution{
			MessageId:    messageID,
			ExecutedAt:   timestamppb.New(time.UnixMilli(executedAt)),
			Success:      success != 0,
			ErrorMessage: errorMessage.String,
		}
		if len(messageBytes) > 0 {
			msg, err := s.Serializer.UnmarshalMessage(messageBytes)
			if err != nil {
				return nil, "", fmt.Errorf("unmarshal history message: %w", err)
			}
			if err := repositorycommon.DecryptMessagePayload(msg, s.KeyManager); err != nil {
				return nil, "", fmt.Errorf("decrypt history message: %w", err)
			}
			execution.Message = msg
			messages = append(messages, msg)
		}
		executions = append(executions, execution)
		rowIDs = append(rowIDs, rowID)
	}

	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate messages: %w", err)
	}
	var nextCursor string
	if limit == 0 {
		executions = nil
		messages = nil
	} else if len(executions) > int(limit) {
		executions = executions[:limit]
		nextCursor = repositorysql.EncodeHistoryCursor(executions[len(executions)-1].GetExecutedAt().AsTime().UnixMilli(), rowIDs[limit-1])
		messages = messages[:0]
		for _, execution := range executions {
			if execution.GetMessage() != nil {
				messages = append(messages, execution.GetMessage())
			}
		}
	}

	history := &schedulepb.ScheduleHistory{
		Messages:   messages,
		ScheduleId: scheduleId,
		Executions: executions,
	}

	// Populate schedule metadata if available
	if schedule.Metadata != nil {
		history.NextRun = schedule.Metadata.NextRun
		history.LastRun = schedule.Metadata.LastRun
		history.CreatedAt = schedule.Metadata.CreatedAt
		history.UpdatedAt = schedule.Metadata.UpdatedAt
	}

	return history, nextCursor, nil
}

func (s *Storage) getScheduleForHistory(ctx context.Context, scheduleId string) (*schedulepb.Schedule, error) {
	query := s.ph(`
   SELECT metadata_pb FROM cq_schedules WHERE id = ?
   UNION ALL
   SELECT metadata_pb
   FROM cq_schedule_archive
   WHERE schedule_id = ?
     AND NOT EXISTS (SELECT 1 FROM cq_schedules WHERE id = ?)
   LIMIT 1
 `)
	var scheduleBytes []byte
	if err := s.DB.QueryRowContext(ctx, query, scheduleId, scheduleId, scheduleId).Scan(&scheduleBytes); err != nil {
		if err == sql.ErrNoRows {
			return nil, domainerror.New(domainerror.NotFound, fmt.Sprintf("schedule %q not found", scheduleId), err)
		}
		return nil, fmt.Errorf("query schedule history metadata: %w", err)
	}
	schedule, err := s.Serializer.UnmarshalSchedule(scheduleBytes)
	if err != nil {
		return nil, fmt.Errorf("unmarshal schedule history metadata: %w", err)
	}
	if err := repositorycommon.DecryptSchedulePayload(schedule, s.KeyManager); err != nil {
		return nil, fmt.Errorf("decrypt schedule history metadata: %w", err)
	}
	return schedule, nil
}

// GetDLQMessages returns messages in the dead letter queue.
