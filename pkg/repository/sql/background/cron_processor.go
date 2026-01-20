package background

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/util"
	"github.com/adrien19/chronoqueue/pkg/metrics"
	repositorycommon "github.com/adrien19/chronoqueue/pkg/repository/common"
	sqlbase "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

// CronProcessorService triggers cron-based schedules when their cron expressions are due.
type CronProcessorService struct {
	base     *sqlbase.BaseSQL
	interval time.Duration
	stopChan chan struct{}
	doneChan chan struct{}
	nowFn    func() time.Time
}

// NewCronProcessorService creates a new cron background processor.
func NewCronProcessorService(base *sqlbase.BaseSQL, interval time.Duration) *CronProcessorService {
	return &CronProcessorService{
		base:     base,
		interval: interval,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		nowFn: func() time.Time {
			return base.Clock.Now().UTC()
		},
	}
}

// Start begins the processing loop.
func (c *CronProcessorService) Start(ctx context.Context) {
	defer close(c.doneChan)
	c.base.Logger.Info("Starting SQL cron processor", "interval", c.interval)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.processCronSchedules(ctx); err != nil {
				c.base.Logger.ErrorWithFields("Cron processor cycle failed", "error", err)
			}
		case <-c.stopChan:
			c.base.Logger.Info("Stopping SQL cron processor")
			return
		case <-ctx.Done():
			c.base.Logger.Info("SQL cron processor stopped due to context cancellation")
			return
		}
	}
}

// StopGracefully stops the service and waits for shutdown.
func (c *CronProcessorService) StopGracefully(ctx context.Context) error {
	close(c.stopChan)
	select {
	case <-c.doneChan:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cron processor shutdown timeout: %w", ctx.Err())
	}
}

// RunOnce processes cron schedules once (useful for tests).
func (c *CronProcessorService) RunOnce(ctx context.Context) error {
	return c.processCronSchedules(ctx)
}

type cronCandidate struct {
	id string
}

func (c *CronProcessorService) processCronSchedules(ctx context.Context) error {
	start := time.Now()
	status := "success"
	defer func() {
		metrics.IncrementBackgroundServiceIterations("cron_processor", status)
		metrics.ObserveBackgroundServiceIterationDuration("cron_processor", time.Since(start).Seconds())
	}()

	nowMs := c.base.Clock.NowMs()

	schedules, err := c.collectDueCronSchedules(ctx, nowMs)
	if err != nil {
		status = "error"
		return fmt.Errorf("collect cron schedules: %w", err)
	}

	for _, sched := range schedules {
		if err := c.processSchedule(ctx, sched.id); err != nil {
			status = "error"
			c.base.Logger.ErrorWithFields("Failed to process cron schedule", "schedule_id", sched.id, "error", err)
		}
	}

	return nil
}

func (c *CronProcessorService) collectDueCronSchedules(ctx context.Context, nowMs int64) ([]cronCandidate, error) {
	query := fmt.Sprintf(`
        SELECT id
        FROM cq_schedules
        WHERE state = %s
          AND cron_schedule IS NOT NULL
          AND cron_schedule <> ''
          AND (next_run IS NULL OR next_run <= %s)
        ORDER BY next_run ASC
        LIMIT 100
    `, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2))

	rows, err := c.base.DB.QueryContext(ctx, query, int(schedulepb.Schedule_Metadata_SCHEDULED), nowMs)
	if err != nil {
		return nil, fmt.Errorf("query cron schedules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []cronCandidate
	for rows.Next() {
		var cand cronCandidate
		if err := rows.Scan(&cand.id); err != nil {
			return nil, fmt.Errorf("scan cron schedule: %w", err)
		}
		out = append(out, cand)
	}

	return out, rows.Err()
}

func (c *CronProcessorService) processSchedule(ctx context.Context, scheduleID string) error {
	return c.base.WithTransaction(ctx, &sqlbase.TxOptions{Timeout: 5 * time.Second}, func(tx *sql.Tx) error {
		query := fmt.Sprintf(`
            SELECT metadata_pb, state, queue_name, cron_schedule, next_run, last_run, execution_count
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
			cronExpr      sql.NullString
			nextRun       sql.NullInt64
			lastRun       sql.NullInt64
			execCount     sql.NullInt64
		)

		if err := tx.QueryRowContext(ctx, query, scheduleID).Scan(&scheduleBytes, &scheduleState, &queueName, &cronExpr, &nextRun, &lastRun, &execCount); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return fmt.Errorf("load schedule: %w", err)
		}

		if scheduleState != int32(schedulepb.Schedule_Metadata_SCHEDULED) {
			return nil
		}
		if !cronExpr.Valid || cronExpr.String == "" {
			return c.markScheduleError(ctx, tx, nil, scheduleID, "missing cron schedule")
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

		cronSchedule, err := c.parseCronExpression(cronExpr.String)
		if err != nil {
			return c.markScheduleError(ctx, tx, schedule, scheduleID, err.Error())
		}

		now := c.nowFn()

		var nextRunTime time.Time
		if nextRun.Valid {
			nextRunTime = time.UnixMilli(nextRun.Int64).UTC()
		} else {
			nextRunTime = c.calculateNextCronRun(cronSchedule, now)
		}

		if schedule.Metadata.NextRun == nil || schedule.Metadata.GetNextRun().AsTime() != nextRunTime {
			schedule.Metadata.NextRun = timestamppb.New(nextRunTime)
		}

		due := nextRun.Valid && !nextRunTime.After(now)
		if !due && !nextRun.Valid {
			return c.updateNextRun(ctx, tx, schedule, scheduleID, nextRunTime, lastRun)
		}

		if !due {
			return nil
		}

		messageID, err := c.createCronMessage(ctx, tx, queueName, schedule, now)
		if err != nil {
			if err.Error() == "queue not found" {
				c.base.Logger.ErrorWithFields("Queue for cron schedule not found", "schedule_id", scheduleID, "queue", queueName)
				return nil
			}
			return fmt.Errorf("create message: %w", err)
		}

		count := int64(0)
		if execCount.Valid {
			count = execCount.Int64
		}
		count++

		schedule.Metadata.MessageIds = append(schedule.Metadata.MessageIds, messageID)
		schedule.Metadata.LastRun = timestamppb.New(now)
		schedule.Metadata.NextRun = timestamppb.New(c.calculateNextCronRun(cronSchedule, now))
		schedule.Metadata.StateMessage = ""
		schedule.Metadata.UpdatedAt = timestamppb.New(now)

		if err := repositorycommon.EncryptSchedulePayload(schedule, c.base.KeyManager); err != nil {
			return fmt.Errorf("encrypt schedule payload: %w", err)
		}

		updatedBytes, err := c.base.Serializer.MarshalSchedule(schedule)
		if err != nil {
			return fmt.Errorf("marshal schedule: %w", err)
		}

		nextRunMs := schedule.Metadata.GetNextRun().AsTime().UnixMilli()
		updateQuery := fmt.Sprintf(`
            UPDATE cq_schedules
            SET metadata_pb = %s,
                state = %s,
                next_run = %s,
                last_run = %s,
                execution_count = %s,
                updated_at = %s
            WHERE id = %s
        `,
			c.base.Dialect.Placeholder(1),
			c.base.Dialect.Placeholder(2),
			c.base.Dialect.Placeholder(3),
			c.base.Dialect.Placeholder(4),
			c.base.Dialect.Placeholder(5),
			c.base.Dialect.Placeholder(6),
			c.base.Dialect.Placeholder(7),
		)

		if _, err := tx.ExecContext(ctx, updateQuery, updatedBytes, schedule.Metadata.GetState(), nextRunMs, now.UnixMilli(), count, now.UnixMilli(), scheduleID); err != nil {
			return fmt.Errorf("update schedule: %w", err)
		}

		metrics.IncrementCronScheduleExecutions(scheduleID, queueName)
		metrics.IncrementBackgroundServiceProcessedMessages("cron_processor", queueName)

		return nil
	})
}

func (c *CronProcessorService) updateNextRun(ctx context.Context, tx *sql.Tx, schedule *schedulepb.Schedule, scheduleID string, nextRun time.Time, lastRun sql.NullInt64) error {
	schedule.Metadata.NextRun = timestamppb.New(nextRun)
	schedule.Metadata.UpdatedAt = timestamppb.New(c.nowFn())

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
            next_run = %s,
            updated_at = %s
        WHERE id = %s
    `, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4))

	_, err = tx.ExecContext(ctx, updateQuery, updatedBytes, nextRun.UnixMilli(), c.base.Clock.NowMs(), scheduleID)
	return err
}

func (c *CronProcessorService) markScheduleError(ctx context.Context, tx *sql.Tx, schedule *schedulepb.Schedule, scheduleID string, msg string) error {
	now := c.base.Clock.Now()
	if schedule == nil {
		schedule = &schedulepb.Schedule{Metadata: &schedulepb.Schedule_Metadata{}}
	}

	schedule.Metadata.State = schedulepb.Schedule_Metadata_ERRORED
	schedule.Metadata.StateMessage = msg
	schedule.Metadata.NextRun = nil
	schedule.Metadata.UpdatedAt = timestamppb.New(now)

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

	if _, err := tx.ExecContext(ctx, updateQuery, updatedBytes, schedule.Metadata.GetState(), now.UnixMilli(), scheduleID); err != nil {
		return fmt.Errorf("update schedule state: %w", err)
	}

	metrics.IncrementCronScheduleFailures(scheduleID, schedule.Metadata.GetQueueName())

	return nil
}

func (c *CronProcessorService) createCronMessage(ctx context.Context, tx *sql.Tx, queueName string, schedule *schedulepb.Schedule, runTime time.Time) (string, error) {
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
	if priority == 0 {
		priority = 5
	}

	maxAttempts := queueMeta.GetDefaultMaxAttempts()
	if maxAttempts == 0 {
		maxAttempts = 3
	}

	messageID, err := util.GenerateID()
	if err != nil {
		return "", fmt.Errorf("generate message id: %w", err)
	}

	leasePolicy := queueMeta.GetLeasePolicy()
	if meta.GetLeaseDuration() != nil {
		if leasePolicy == nil {
			leasePolicy = &commonpb.LeasePolicy{}
		}
		leasePolicy.BaseLease = meta.GetLeaseDuration()
	}

	message := &messagepb.Message{
		MessageId: messageID,
		Metadata: &messagepb.Message_Metadata{
			Payload:       meta.GetPayload(),
			State:         messagepb.Message_Metadata_INVISIBLE,
			AttemptsLeft:  int32(maxAttempts),
			MaxAttempts:   int32(maxAttempts),
			Priority:      priority,
			ScheduledTime: timestamppb.New(runTime),
			LeasePolicy:   leasePolicy,
			LeaseDuration: meta.GetLeaseDuration(),
		},
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
        ON CONFLICT(message_id) DO NOTHING
    `, strings.Join(placeholders, ", "))

	nowMs := c.base.Clock.NowMs()
	if _, err := tx.ExecContext(ctx, insertQuery,
		message.MessageId,
		queueName,
		message.Metadata.State,
		message.Metadata.AttemptsLeft,
		message.Metadata.MaxAttempts,
		message.Metadata.Priority,
		runTime.UnixMilli(),
		messageBytes,
		nowMs,
		nowMs,
	); err != nil {
		return "", fmt.Errorf("insert message: %w", err)
	}

	if err := c.base.StateManager.UpdateCounters(ctx, tx, queueName, 0, messagepb.Message_Metadata_INVISIBLE); err != nil {
		return "", fmt.Errorf("update counters: %w", err)
	}

	historyQuery := fmt.Sprintf(`
        INSERT INTO cq_schedule_history (schedule_id, message_id, executed_at, success)
        VALUES (%s, %s, %s, %s)
    `, c.base.Dialect.Placeholder(1), c.base.Dialect.Placeholder(2), c.base.Dialect.Placeholder(3), c.base.Dialect.Placeholder(4))

	if _, err := tx.ExecContext(ctx, historyQuery, schedule.GetScheduleId(), messageID, runTime.UnixMilli(), 1); err != nil {
		return "", fmt.Errorf("insert schedule history: %w", err)
	}

	return messageID, nil
}

func (c *CronProcessorService) parseCronExpression(expression string) (cron.Schedule, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronSchedule, err := parser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	return cronSchedule, nil
}

func (c *CronProcessorService) calculateNextCronRun(cronSchedule cron.Schedule, fromTime time.Time) time.Time {
	return cronSchedule.Next(fromTime.UTC())
}
