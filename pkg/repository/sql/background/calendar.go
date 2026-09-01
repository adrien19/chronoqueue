package background

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/calendar"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// CalendarService triggers calendar-based schedules when their next_run arrives.
type CalendarService struct {
	base             *sqlbase.BaseSQL
	engine           calendar.Engine
	interval         time.Duration
	stopChan         chan struct{}
	doneChan         chan struct{}
	generateID       func() (string, error)
	batchSize        int
	maxDrainBatches  int
	maxCycleDuration time.Duration
}

// NewCalendarService creates a new calendar background processor.
func NewCalendarService(base *sqlbase.BaseSQL, engine calendar.Engine, interval time.Duration) *CalendarService {
	return &CalendarService{
		base:             base,
		engine:           engine,
		interval:         interval,
		stopChan:         make(chan struct{}),
		doneChan:         make(chan struct{}),
		generateID:       util.GenerateID,
		batchSize:        defaultBackgroundBatchSize,
		maxDrainBatches:  defaultBackgroundMaxDrainBatches,
		maxCycleDuration: defaultBackgroundCycleDuration,
	}
}

// Start begins the processing loop.
func (c *CalendarService) Start(ctx context.Context) {
	defer close(c.doneChan)
	c.base.Logger.Info("Starting SQL calendar service", "interval", c.interval)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.processDueSchedules(ctx); err != nil {
				c.base.Logger.ErrorWithFields("Calendar service cycle failed", "error", err)
			}
		case <-c.stopChan:
			c.base.Logger.Info("Stopping SQL calendar service")
			return
		case <-ctx.Done():
			c.base.Logger.Info("SQL calendar service stopped due to context cancellation")
			return
		}
	}
}

// StopGracefully stops the service and waits for shutdown.
func (c *CalendarService) StopGracefully(ctx context.Context) error {
	close(c.stopChan)
	select {
	case <-c.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("calendar service shutdown timeout: %w", ctx.Err())
	}
}

// RunOnce processes due schedules once (used in tests).
func (c *CalendarService) RunOnce(ctx context.Context) error {
	return c.processDueSchedules(ctx)
}

type dueSchedule struct {
	id string
}

func (c *CalendarService) processDueSchedules(ctx context.Context) error {
	start := time.Now()
	status := "success"
	batches := 0
	handled := 0
	defer func() {
		metrics.IncrementBackgroundServiceIterations("calendar", status)
		metrics.ObserveBackgroundServiceIterationDuration("calendar", time.Since(start).Seconds())
		metrics.ObserveBackgroundServiceCycle("calendar", batches, int64(handled))
	}()

	budgetExhausted := true
	for batch := 0; batch < c.maxDrainBatches && time.Since(start) < c.maxCycleDuration; batch++ {
		if err := ctx.Err(); err != nil {
			status = "error"
			return fmt.Errorf("calendar cycle canceled: %w", err)
		}
		nowMs := c.base.Clock.NowMs()
		schedules, err := c.collectDueSchedules(ctx, nowMs, c.batchSize)
		if err != nil {
			status = "error"
			return fmt.Errorf("collect due schedules: %w", err)
		}
		if len(schedules) == 0 {
			budgetExhausted = false
			break
		}
		batches++
		handled += len(schedules)
		metrics.ObserveBackgroundServiceBatchSize("calendar", len(schedules))
		progressed := 0
		for _, sched := range schedules {
			processed, err := c.processSchedule(ctx, sched.id, nowMs)
			if err != nil {
				status = "error"
				c.base.Logger.ErrorWithFields("Failed to process schedule", "schedule_id", sched.id, "error", err)
				continue
			}
			if processed {
				progressed++
			}
		}
		if progressed == 0 {
			budgetExhausted = false
			break
		}
		yieldBetweenBatches(c.base.Dialect.SupportsSkipLocked())
	}
	if budgetExhausted {
		metrics.IncrementBackgroundServiceBudgetExhausted("calendar")
	}

	return nil
}

func (c *CalendarService) collectDueSchedules(ctx context.Context, nowMs int64, limit int) ([]dueSchedule, error) {
	query := fmt.Sprintf(`
		SELECT id
		FROM cq_schedules
		WHERE state = %s
		  AND next_run IS NOT NULL
		  AND next_run <= %s
		  AND (cron_schedule IS NULL OR cron_schedule = '')
		ORDER BY next_run ASC
		LIMIT %d
	`, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), limit)

	rows, err := c.base.DB.QueryContext(ctx, query, int(schedulepb.Schedule_Metadata_SCHEDULED), nowMs)
	if err != nil {
		return nil, fmt.Errorf("query due schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []dueSchedule
	for rows.Next() {
		var sched dueSchedule
		if err := rows.Scan(&sched.id); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		out = append(out, sched)
	}

	return out, rows.Err()
}

func (c *CalendarService) processSchedule(ctx context.Context, scheduleID string, nowMs int64) (bool, error) {
	processed := false
	err := c.base.WithTransaction(ctx, &sqlbase.TxOptions{Timeout: 5 * time.Second}, func(tx *sql.Tx) error {
		query := fmt.Sprintf(`
			SELECT metadata_pb, state, queue_name, next_run, execution_count
			FROM cq_schedules
			WHERE id = %s
		`, c.base.Dialect.Placeholder(1))

		if c.base.Dialect.SupportsSkipLocked() {
			query = strings.TrimSpace(query) + " FOR UPDATE SKIP LOCKED"
		}

		var (
			scheduleBytes []byte
			scheduleState int32
			queueName     string
			nextRun       sql.NullInt64
			execCount     int64
		)

		if err := tx.QueryRowContext(ctx, query, scheduleID).Scan(&scheduleBytes, &scheduleState, &queueName, &nextRun, &execCount); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return fmt.Errorf("load schedule: %w", err)
		}

		if scheduleState != int32(schedulepb.Schedule_Metadata_SCHEDULED) {
			return nil
		}
		if !nextRun.Valid || nextRun.Int64 > nowMs {
			return nil
		}

		schedule, err := c.base.Serializer.UnmarshalSchedule(scheduleBytes)
		if err != nil {
			return fmt.Errorf("unmarshal schedule: %w", err)
		}

		if err := repositorycommon.DecryptSchedulePayload(schedule, c.base.KeyManager); err != nil {
			return fmt.Errorf("decrypt schedule payload: %w", err)
		}

		if schedule.Metadata == nil {
			schedule.Metadata = &schedulepb.Schedule_Metadata{}
		}
		markError := func(message string) error {
			if err := c.markScheduleError(ctx, tx, schedule, scheduleID, message); err != nil {
				return err
			}
			processed = true
			return nil
		}
		if schedule.Metadata.GetHasMaxMessages() && execCount >= schedule.Metadata.GetMaxMessages() {
			if err := c.pauseAtMessageLimit(ctx, tx, schedule, scheduleID, nowMs, execCount); err != nil {
				return err
			}
			processed = true
			return nil
		}

		// Skip cron-only schedules; cron processor owns them and sets next_run
		if schedule.Metadata.GetCalendarSchedule() == nil {
			if schedule.Metadata.GetCronSchedule() != "" {
				return nil
			}
			return markError("missing calendar schedule")
		}

		runTime := time.UnixMilli(nextRun.Int64)
		if schedule.Metadata.NextRun == nil {
			schedule.Metadata.NextRun = timestamppb.New(runTime)
		}

		if err := c.engine.ValidateSchedule(ctx, schedule.Metadata.GetCalendarSchedule()); err != nil {
			return markError(fmt.Sprintf("invalid calendar schedule: %v", err))
		}

		_, err = c.createScheduledMessage(ctx, tx, queueName, schedule, runTime)
		if err != nil {
			return markError(err.Error())
		}

		repositorycommon.ClearLegacyScheduleMessageIDs(schedule.Metadata)
		schedule.Metadata.LastRun = timestamppb.New(runTime)
		execCount++

		var nextRunMs any
		if schedule.Metadata.GetHasMaxMessages() && execCount >= schedule.Metadata.GetMaxMessages() {
			nextRunTime, err := c.engine.CalculateNextRun(ctx, schedule.Metadata.GetCalendarSchedule(), runTime)
			if err != nil {
				return markError(err.Error())
			}
			schedule.Metadata.State = schedulepb.Schedule_Metadata_PAUSED
			schedule.Metadata.StateMessage = "maximum message count reached"
			if nextRunTime == nil {
				schedule.Metadata.NextRun = nil
			} else {
				schedule.Metadata.NextRun = timestamppb.New(*nextRunTime)
				nextRunMs = nextRunTime.UnixMilli()
			}
		} else {
			nextRunTime, err := c.engine.CalculateNextRun(ctx, schedule.Metadata.GetCalendarSchedule(), runTime)
			if err != nil {
				return markError(err.Error())
			}
			if nextRunTime == nil {
				schedule.Metadata.State = schedulepb.Schedule_Metadata_PAUSED
				schedule.Metadata.StateMessage = "no future runs"
				schedule.Metadata.NextRun = nil
			} else {
				schedule.Metadata.NextRun = timestamppb.New(*nextRunTime)
				nextRunMs = nextRunTime.UnixMilli()
			}
		}

		schedule.Metadata.UpdatedAt = timestamppb.New(time.UnixMilli(nowMs))

		if err := repositorycommon.EncryptSchedulePayload(schedule, c.base.KeyManager); err != nil {
			return fmt.Errorf("encrypt schedule payload: %w", err)
		}

		updatedBytes, err := c.base.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		updateQuery := fmt.Sprintf(`
			UPDATE cq_schedules
			SET metadata_pb = %s,
			    state = %s,
			    next_run = %s,
			    last_run = %s,
			    updated_at = %s,
			    execution_count = %s
			WHERE id = %s
		`, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4), c.base.Dialect.Placeholder(5), c.base.Dialect.Placeholder(6), c.base.Dialect.Placeholder(7))

		if _, err := tx.ExecContext(ctx, updateQuery, updatedBytes, schedule.Metadata.GetState(), nextRunMs, runTime.UnixMilli(), nowMs, execCount, scheduleID); err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		metrics.IncrementScheduleExecutions(scheduleID, queueName, "success")
		metrics.IncrementBackgroundServiceProcessedMessages("calendar", queueName)
		processed = true

		return nil
	})
	return processed, err
}

func (c *CalendarService) pauseAtMessageLimit(ctx context.Context, tx *sql.Tx, schedule *schedulepb.Schedule, scheduleID string, nowMs, execCount int64) error {
	schedule.Metadata.State = schedulepb.Schedule_Metadata_PAUSED
	schedule.Metadata.StateMessage = "maximum message count reached"
	schedule.Metadata.UpdatedAt = timestamppb.New(time.UnixMilli(nowMs))
	if err := repositorycommon.EncryptSchedulePayload(schedule, c.base.KeyManager); err != nil {
		return fmt.Errorf("encrypt schedule payload: %w", err)
	}
	updatedBytes, err := c.base.Serializer.MarshalSchedule(schedule)
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}
	query := fmt.Sprintf(`UPDATE cq_schedules SET metadata_pb = %s, state = %s, updated_at = %s, execution_count = %s WHERE id = %s`,
		c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4), c.base.Dialect.Placeholder(5))
	_, err = tx.ExecContext(ctx, query, updatedBytes, schedule.Metadata.GetState(), nowMs, execCount, scheduleID)
	return err
}

func (c *CalendarService) markScheduleError(ctx context.Context, tx *sql.Tx, schedule *schedulepb.Schedule, scheduleID string, msg string) error {
	schedule.Metadata.State = schedulepb.Schedule_Metadata_ERRORED
	schedule.Metadata.StateMessage = msg
	schedule.Metadata.NextRun = nil
	schedule.Metadata.UpdatedAt = timestamppb.Now()

	if err := repositorycommon.EncryptSchedulePayload(schedule, c.base.KeyManager); err != nil {
		return fmt.Errorf("encrypt schedule payload: %w", err)
	}

	updatedBytes, err := c.base.Serializer.MarshalSchedule(schedule)
	if err != nil {
		return fmt.Errorf("marshal schedule: %w", err)
	}

	updateQuery := fmt.Sprintf(`
		UPDATE cq_schedules
		SET metadata_pb = %s,
		    state = %s,
		    next_run = NULL,
		    updated_at = %s
		WHERE id = %s
	`, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4))

	if _, err := tx.ExecContext(ctx, updateQuery, updatedBytes, schedule.Metadata.GetState(), c.base.Clock.NowMs(), scheduleID); err != nil {
		return fmt.Errorf("update schedule state: %w", err)
	}
	historyQuery := fmt.Sprintf(`INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success, error_message) VALUES (%s, %s, %s, %s, %s)`,
		c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4), c.base.Dialect.Placeholder(5))
	if _, err := tx.ExecContext(ctx, historyQuery, scheduleID, "", c.base.Clock.NowMs(), 0, msg); err != nil {
		return fmt.Errorf("record failed schedule execution: %w", err)
	}

	metrics.IncrementScheduleExecutions(scheduleID, schedule.Metadata.GetQueueName(), "error")

	return nil
}

func (c *CalendarService) createScheduledMessage(ctx context.Context, tx *sql.Tx, queueName string, schedule *schedulepb.Schedule, runTime time.Time) (string, error) {
	queueQuery := fmt.Sprintf(`SELECT metadata_pb FROM cq_queues WHERE name = %s`, c.base.Dialect.Placeholder(1))
	var queueBytes []byte
	if err := tx.QueryRowContext(ctx, queueQuery, queueName).Scan(&queueBytes); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("queue not found")
		}
		return "", fmt.Errorf("load queue metadata: %w", err)
	}

	queue, err := c.base.Serializer.UnmarshalQueue(queueBytes)
	if err != nil {
		return "", fmt.Errorf("unmarshal queue: %w", err)
	}

	queueMeta := queue.GetMetadata()
	if queueMeta == nil {
		queueMeta = &queuepb.QueueMetadata{}
	}

	meta := schedule.GetMetadata()
	priority := meta.GetPriority()
	// Priority 0 is valid (lowest priority), no defaulting needed

	maxAttempts := queueMeta.GetDefaultMaxAttempts()
	if maxAttempts == 0 {
		maxAttempts = 3
	}

	messageID, err := c.generateID()
	if err != nil {
		return "", fmt.Errorf("generate message id: %w", err)
	}

	leasePolicy := queueMeta.GetLeasePolicy()
	if leasePolicy != nil {
		leasePolicy = proto.Clone(leasePolicy).(*commonpb.LeasePolicy)
	}
	if meta.GetLeaseDuration() != nil {
		if leasePolicy == nil {
			leasePolicy = &commonpb.LeasePolicy{}
		}
		leasePolicy.BaseLease = meta.GetLeaseDuration()
	}

	message := &messagepb.Message{
		MessageId: messageID,
		Metadata: &messagepb.Message_Metadata{
			Headers:       cloneHeaders(meta.GetHeaders()),
			Payload:       clonePayload(meta.GetPayload()),
			State:         messagepb.Message_Metadata_INVISIBLE,
			AttemptsLeft:  int32(maxAttempts),
			MaxAttempts:   int32(maxAttempts),
			Priority:      priority,
			ScheduledTime: timestamppb.New(runTime),
			LeasePolicy:   leasePolicy,
			LeaseDuration: meta.GetLeaseDuration(),
		},
	}
	if err := validateScheduledMessage(ctx, c.base, queueMeta, message); err != nil {
		return "", err
	}

	if err := repositorycommon.EncryptMessagePayload(message, c.base.KeyManager); err != nil {
		return "", fmt.Errorf("encrypt message payload: %w", err)
	}

	messageBytes, err := c.base.Serializer.MarshalMessage(message)
	if err != nil {
		return "", fmt.Errorf("marshal message: %w", err)
	}

	placeholders := make([]string, 10)
	for i := range placeholders {
		placeholders[i] = c.base.Dialect.Placeholder(i + 1)
	}
	insertQuery := fmt.Sprintf(`
		INSERT INTO cq_messages (
			message_id, queue_name, state, attempts_left, max_attempts,
			priority, scheduled_at, metadata_pb, created_at, updated_at
		) VALUES (%s)
	`, strings.Join(placeholders, ", "))

	now := c.base.Clock.NowMs()
	if _, err := tx.ExecContext(
		ctx, insertQuery,
		message.MessageId,
		queueName,
		message.Metadata.State,
		message.Metadata.AttemptsLeft,
		message.Metadata.MaxAttempts,
		message.Metadata.Priority,
		runTime.UnixMilli(),
		messageBytes,
		now,
		now,
	); err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}

	if err := c.base.StateManager.InsertCounter(ctx, tx, queueName, messagepb.Message_Metadata_INVISIBLE); err != nil {
		return "", fmt.Errorf("update counters: %w", err)
	}

	historyQuery := fmt.Sprintf(`
		INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success, message_pb)
		VALUES (%s, %s, %s, %s, %s)
	`, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4), c.base.Dialect.Placeholder(5))

	if _, err := tx.ExecContext(ctx, historyQuery, schedule.GetScheduleId(), messageID, runTime.UnixMilli(), 1, messageBytes); err != nil {
		return "", fmt.Errorf("insert schedule history: %w", err)
	}

	return messageID, nil
}

func clonePayload(payload *commonpb.Payload) *commonpb.Payload {
	if payload == nil {
		return nil
	}
	return proto.Clone(payload).(*commonpb.Payload)
}
