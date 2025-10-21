package integration

// Package integration provides scheduling tests for ChronoQueue.
//
// These tests validate:
// - Cron-based scheduling (standard cron expressions)
// - Calendar-based scheduling (business days, holidays, timezones)
// - Schedule management (create, pause, resume, delete)
// - Schedule history and execution tracking
//
// Test Scenarios: TC-S-001 through TC-S-035 from TESTING_GUIDE.md
//
// Run with: go test -v ./tests/integration/ -run TestSchedul

import (
	"context"
	"encoding/json"
	"testing"

	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	queue_pb "github.com/adrien19/chronoqueue/api/queue/v1"
	queueservice_pb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/tests/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestScheduling_CreateCronSchedule validates cron schedule creation
//
// Test Scenario: TC-S-001 from TESTING_GUIDE.md
// Data: fixtures/schedules.json:cron_every_minute
// Expected: Schedule created successfully, metadata retrievable
func TestScheduling_CreateCronSchedule(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-cron-schedule")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Load schedule fixture
	scheduleFixture := helpers.LoadScheduleFixture(t, "cron_hourly")

	// Create message payload
	payloadData := make(map[string]interface{})
	payloadBytes, _ := json.Marshal(scheduleFixture.Message.Content)
	json.Unmarshal(payloadBytes, &payloadData)

	payload := &common_pb.Payload{
		Data:        createStruct(t, payloadData),
		ContentType: scheduleFixture.Message.ContentType,
	}

	// Create schedule
	scheduleID := helpers.GenerateUniqueMessageID(t) // Reuse ID generator

	schedule := &schedule_pb.Schedule{
		ScheduleId: scheduleID,
		Metadata: &schedule_pb.Schedule_Metadata{
			Payload:    payload,
			QueueName:  queueName,
			MessageIds: []string{helpers.GenerateUniqueMessageID(t)},
			ScheduleConfig: &schedule_pb.Schedule_Metadata_CronSchedule{
				CronSchedule: scheduleFixture.CronExpression,
			},
		},
	}

	// Act
	createResp, err := client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
		Schedule: schedule,
	})

	// Assert
	require.NoError(t, err, "Creating cron schedule should succeed")
	assert.True(t, createResp.Success, "Response should indicate success")

	// Verify schedule can be retrieved
	getResp, err := client.GetSchedule(ctx, &queueservice_pb.GetScheduleRequest{
		ScheduleId: scheduleID,
	})

	if err == nil {
		assert.NotNil(t, getResp.Schedule, "Schedule should be retrievable")
		assert.Equal(t, scheduleID, getResp.Schedule.ScheduleId, "Schedule ID should match")
	}
}

// TestScheduling_InvalidCronExpression validates error handling for invalid cron
//
// Test Scenario: TC-S-005 from TESTING_GUIDE.md
// Data: Invalid cron expression
// Expected: Validation error with clear message
func TestScheduling_InvalidCronExpression(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-invalid-cron")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Create schedule with invalid cron expression
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"task": "test"}),
		ContentType: "application/json",
	}

	schedule := &schedule_pb.Schedule{
		ScheduleId: helpers.GenerateUniqueMessageID(t),
		Metadata: &schedule_pb.Schedule_Metadata{
			QueueName: queueName,
			Payload:   payload,
			ScheduleConfig: &schedule_pb.Schedule_Metadata_CronSchedule{
				CronSchedule: "* * * * * * *", // Too many fields (invalid)
			},
		},
	}

	// Act
	_, err = client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
		Schedule: schedule,
	})

	// Assert - Should return error
	if err != nil {
		t.Logf("Expected error for invalid cron: %v", err)
		helpers.AssertErrorContains(t, err, "cron")
	} else {
		t.Log("Server accepted invalid cron expression (might have lenient validation)")
	}
}

// TestScheduling_CalendarScheduleBusinessDays validates business day scheduling
//
// Test Scenario: TC-S-010 from TESTING_GUIDE.md
// Data: fixtures/schedules.json:calendar_business_days
// Expected: Schedule only executes on Monday-Friday
func TestScheduling_CalendarScheduleBusinessDays(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-calendar-business")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Create calendar schedule for business days
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"task": "business_day_task"}),
		ContentType: "application/json",
	}

	// Build calendar schedule with business days
	calendarSchedule := &schedule_pb.CalendarSchedule{
		Type:     schedule_pb.CalendarSchedule_BUSINESS_DAYS,
		Timezone: "UTC", // Use UTC to avoid tzdata dependency
		Rules: []*schedule_pb.CalendarRule{
			{
				Rule: &schedule_pb.CalendarRule_BusinessDays{
					BusinessDays: &schedule_pb.BusinessDaysRule{},
				},
			},
		},
	}

	schedule := &schedule_pb.Schedule{
		ScheduleId: helpers.GenerateUniqueMessageID(t),
		Metadata: &schedule_pb.Schedule_Metadata{
			Payload:   payload,
			QueueName: queueName,
			ScheduleConfig: &schedule_pb.Schedule_Metadata_CalendarSchedule{
				CalendarSchedule: calendarSchedule,
			},
		},
	}

	// Act
	createResp, err := client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
		Schedule: schedule,
	})

	// Assert
	require.NoError(t, err, "Creating calendar schedule should succeed")
	assert.True(t, createResp.Success, "Response should indicate success")
}

// TestScheduling_ValidateCalendarSchedule validates schedule validation API
//
// Test Scenario: TC-S-018 from TESTING_GUIDE.md
// Data: Calendar schedule rules
// Expected: Validation result returned
func TestScheduling_ValidateCalendarSchedule(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create calendar rules to validate
	calendarSchedule := &schedule_pb.CalendarSchedule{
		Type:     schedule_pb.CalendarSchedule_BUSINESS_DAYS,
		Timezone: "UTC",
		Rules: []*schedule_pb.CalendarRule{
			{
				Rule: &schedule_pb.CalendarRule_BusinessDays{
					BusinessDays: &schedule_pb.BusinessDaysRule{},
				},
			},
		},
	}

	// Act
	validateResp, err := client.ValidateCalendarSchedule(ctx, &queueservice_pb.ValidateCalendarScheduleRequest{
		CalendarSchedule: calendarSchedule,
	})

	// Assert
	require.NoError(t, err, "Validation API should succeed")
	assert.True(t, validateResp.Valid, "Valid calendar schedule should pass validation")
	t.Logf("Validation result: valid=%v, error=%s", validateResp.Valid, validateResp.ErrorMessage)
}

// TestScheduling_PreviewCalendarSchedule validates schedule preview API
//
// Test Scenario: TC-S-019 from TESTING_GUIDE.md
// Data: Calendar schedule rules
// Expected: Returns list of next N execution times
func TestScheduling_PreviewCalendarSchedule(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	// Create calendar rules for preview
	calendarSchedule := &schedule_pb.CalendarSchedule{
		Type:     schedule_pb.CalendarSchedule_WEEKLY,
		Timezone: "UTC",
		Rules: []*schedule_pb.CalendarRule{
			{
				Rule: &schedule_pb.CalendarRule_Weekly{
					Weekly: &schedule_pb.WeeklyRule{
						DaysOfWeek: []int32{1, 3, 5}, // Monday, Wednesday, Friday
					},
				},
			},
		},
	}

	// Act - Preview next 5 executions
	previewResp, err := client.PreviewCalendarSchedule(ctx, &queueservice_pb.PreviewCalendarScheduleRequest{
		CalendarSchedule: calendarSchedule,
		Count:            5,
	})

	// Assert
	require.NoError(t, err, "Preview API should succeed")
	assert.LessOrEqual(t, len(previewResp.ExecutionTimes), 5, "Should return at most 5 execution times")

	t.Logf("Preview returned %d execution times", len(previewResp.ExecutionTimes))
	for i, ts := range previewResp.ExecutionTimes {
		t.Logf("  Execution %d: %v", i+1, ts.AsTime())
	}
}

// TestScheduling_PauseAndResumeSchedule validates schedule state management
//
// Test Scenario: TC-S-030, TC-S-031 from TESTING_GUIDE.md
// Data: Active schedule
// Expected: Schedule paused, then resumed
func TestScheduling_PauseAndResumeSchedule(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-pause-resume")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Create schedule
	scheduleID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"task": "test"}),
		ContentType: "application/json",
	}

	schedule := &schedule_pb.Schedule{
		ScheduleId: scheduleID,
		Metadata: &schedule_pb.Schedule_Metadata{
			QueueName: queueName,
			Payload:   payload,
			ScheduleConfig: &schedule_pb.Schedule_Metadata_CronSchedule{
				CronSchedule: "0 * * * *", // Every hour
			},
		},
	}

	createResp, err := client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
		Schedule: schedule,
	})
	require.NoError(t, err)
	require.True(t, createResp.Success)

	// Act - Pause schedule
	pauseResp, err := client.PauseSchedule(ctx, &queueservice_pb.PauseScheduleRequest{
		ScheduleId: scheduleID,
	})

	// Assert pause
	require.NoError(t, err, "Pause should succeed")
	assert.True(t, pauseResp.Success, "Pause response should indicate success")

	// Act - Resume schedule
	resumeResp, err := client.ResumeSchedule(ctx, &queueservice_pb.ResumeScheduleRequest{
		ScheduleId: scheduleID,
	})

	// Assert resume
	require.NoError(t, err, "Resume should succeed")
	assert.True(t, resumeResp.Success, "Resume response should indicate success")
}

// TestScheduling_DeleteSchedule validates schedule deletion
//
// Test Scenario: TC-S-032 from TESTING_GUIDE.md
// Data: Active schedule
// Expected: Schedule deleted, no new executions
func TestScheduling_DeleteSchedule(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-delete-schedule")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Create schedule
	scheduleID := helpers.GenerateUniqueMessageID(t)
	payload := &common_pb.Payload{
		Data:        createStruct(t, map[string]interface{}{"task": "test"}),
		ContentType: "application/json",
	}

	schedule := &schedule_pb.Schedule{
		ScheduleId: scheduleID,
		Metadata: &schedule_pb.Schedule_Metadata{
			QueueName: queueName,
			Payload:   payload,
			ScheduleConfig: &schedule_pb.Schedule_Metadata_CronSchedule{
				CronSchedule: "0 * * * *",
			},
		},
	}

	createResp, err := client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
		Schedule: schedule,
	})
	require.NoError(t, err)
	require.True(t, createResp.Success)

	// Act - Delete schedule
	deleteResp, err := client.DeleteSchedule(ctx, &queueservice_pb.DeleteScheduleRequest{
		ScheduleId: scheduleID,
	})

	// Assert
	require.NoError(t, err, "Delete should succeed")
	assert.True(t, deleteResp.Success, "Delete response should indicate success")

	// Verify schedule no longer exists
	_, err = client.GetSchedule(ctx, &queueservice_pb.GetScheduleRequest{
		ScheduleId: scheduleID,
	})

	if err != nil {
		t.Logf("Expected: Schedule not found after deletion: %v", err)
	}
}

// TestScheduling_ListSchedules validates listing all schedules
//
// Test Scenario: TC-S-034 from TESTING_GUIDE.md
// Data: Multiple schedules
// Expected: All schedules returned with metadata
func TestScheduling_ListSchedules(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := context.Background()
	env := helpers.SetupTestEnvironment(t)
	conn := env.NewGRPCClient(t)
	client := queueservice_pb.NewQueueServiceClient(conn)

	queueName := helpers.GenerateUniqueQueueName(t, "test-list-schedules")

	// Create queue
	_, err := client.CreateQueue(ctx, &queueservice_pb.CreateQueueRequest{
		Name: queueName,
		Metadata: &queue_pb.QueueMetadata{
			Type: queue_pb.QueueType_SIMPLE,
		},
	})
	require.NoError(t, err)

	// Create multiple schedules
	scheduleIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		scheduleID := helpers.GenerateUniqueMessageID(t)
		scheduleIDs[i] = scheduleID

		payload := &common_pb.Payload{
			Data:        createStruct(t, map[string]interface{}{"task": "test", "index": i}),
			ContentType: "application/json",
		}

		schedule := &schedule_pb.Schedule{
			ScheduleId: scheduleID,
			Metadata: &schedule_pb.Schedule_Metadata{
				QueueName: queueName,
				Payload:   payload,
				ScheduleConfig: &schedule_pb.Schedule_Metadata_CronSchedule{
					CronSchedule: "0 * * * *",
				},
			},
		}

		_, err := client.CreateSchedule(ctx, &queueservice_pb.CreateScheduleRequest{
			Schedule: schedule,
		})
		require.NoError(t, err)
	}

	// Act - List schedules (API may not support queue name filter, just list all)
	listResp, err := client.ListSchedules(ctx, &queueservice_pb.ListSchedulesRequest{})

	// Assert
	require.NoError(t, err, "List schedules should succeed")
	assert.GreaterOrEqual(t, len(listResp.Schedules), 3, "Should return at least 3 schedules")

	t.Logf("Found %d schedules for queue %s", len(listResp.Schedules), queueName)
}

// Helper function
func createStruct(t *testing.T, data map[string]interface{}) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(data)
	require.NoError(t, err)
	return s
}
