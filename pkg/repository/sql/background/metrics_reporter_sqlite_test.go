//go:build sqlite && cgo

package background

import (
	"bufio"
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

func TestMetricsReporterEmitsDatabaseAndDLQGauges(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "source-dlq", Metadata: &queuepb.QueueMetadata{}}))

	registry := metrics.NewMetricsRegistry()
	reporter := NewMetricsReporterService(storage.BaseSQL, time.Second)
	reporter.reportMetrics(ctx)
	require.False(t, reporter.LastReportedAt().IsZero())

	recorder := httptest.NewRecorder()
	registry.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	require.True(t, strings.Contains(body, `chronoqueue_db_connections_active{backend="sqlite"}`))
	require.True(t, strings.Contains(body, `chronoqueue_dlq_messages_total{dlq_name="source-dlq"} 0`))
}

func TestMetricsReporterRecordsQueryFailureWithoutAdvancingLastReportedAt(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	registry := metrics.NewMetricsRegistry()
	reporter := NewMetricsReporterService(storage.BaseSQL, time.Second)
	before := metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="error"}`)
	_, err := storage.DB.ExecContext(ctx, "DROP TABLE cq_queues")
	require.NoError(t, err)

	reporter.reportMetrics(ctx)

	require.True(t, reporter.LastReportedAt().IsZero())
	after := metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="error"}`)
	require.Equal(t, before+1, after)
}

func TestMetricsReporterRecordsMetadataFailureWithoutAdvancingLastReportedAt(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}))
	_, err := storage.DB.ExecContext(ctx, `UPDATE cq_queues SET metadata_pb = ? WHERE name = ?`, []byte("invalid"), "source")
	require.NoError(t, err)
	registry := metrics.NewMetricsRegistry()
	reporter := NewMetricsReporterService(storage.BaseSQL, time.Second)
	before := metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="error"}`)
	successBefore := metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="success"}`)

	reporter.reportMetrics(ctx)

	require.True(t, reporter.LastReportedAt().IsZero())
	require.Equal(t, before+1, metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="error"}`))
	require.Equal(t, successBefore, metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="success"}`))
}

func TestMetricsReporterReportsSharedDLQOnce(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	for _, source := range []string{"source-a", "source-b"} {
		require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: source, Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "shared-dlq"}}))
	}
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "shared-dlq", Metadata: &queuepb.QueueMetadata{}}))

	registry := metrics.NewMetricsRegistry()
	NewMetricsReporterService(storage.BaseSQL, time.Second).reportMetrics(ctx)
	recorder := httptest.NewRecorder()
	registry.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))

	lines := 0
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if strings.HasPrefix(line, `chronoqueue_dlq_messages_total{dlq_name="shared-dlq"}`) {
			lines++
		}
	}
	require.Equal(t, 1, lines)
}

func TestMetricsReporterDLQCountFailureDoesNotRecordSuccess(t *testing.T) {
	ctx := context.Background()
	storage := newTestStorage(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "source", Metadata: &queuepb.QueueMetadata{DeadLetterQueueName: "source-dlq"}}))
	require.NoError(t, storage.CreateQueue(ctx, &queuepb.Queue{Name: "source-dlq", Metadata: &queuepb.QueueMetadata{}}))
	_, err := storage.DB.ExecContext(ctx, "DROP TABLE cq_messages")
	require.NoError(t, err)
	registry := metrics.NewMetricsRegistry()
	reporter := NewMetricsReporterService(storage.BaseSQL, time.Second)
	successBefore := metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="success"}`)

	err = reporter.updateDLQMetrics(ctx, "source", make(map[string]struct{}))
	require.ErrorContains(t, err, "count DLQ messages")
	require.True(t, reporter.LastReportedAt().IsZero())
	require.Equal(t, successBefore, metricValue(t, registry, `chronoqueue_background_service_iterations_total{service="metrics_reporter",status="success"}`))
}

func metricValue(t *testing.T, registry *metrics.MetricsRegistry, metric string) float64 {
	t.Helper()
	recorder := httptest.NewRecorder()
	registry.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	scanner := bufio.NewScanner(strings.NewReader(recorder.Body.String()))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, metric+" ") {
			value, err := strconv.ParseFloat(strings.TrimPrefix(line, metric+" "), 64)
			require.NoError(t, err)
			return value
		}
	}
	require.NoError(t, scanner.Err())
	return 0
}
