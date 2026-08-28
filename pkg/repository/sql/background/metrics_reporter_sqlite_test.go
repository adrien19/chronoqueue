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
	require.True(t, strings.Contains(body, `chronoqueue_dlq_messages_total{dlq_name="source-dlq",source_queue="source"} 0`))
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
