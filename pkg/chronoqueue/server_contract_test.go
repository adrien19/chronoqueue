package chronoqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	queueservicepb "github.com/adrien19/chronoqueue/api/queueservice/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/internal/domainerror"
	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/repository"
	"github.com/adrien19/chronoqueue/pkg/schema"
)

type contractStorage struct {
	repository.Storage
	validationErr  error
	validationCall *schedulepb.CalendarSchedule
	previewCalled  bool
	previewCount   int
}

type contractRegistry struct {
	schema.Registry
	registerCalled bool
	registered     *schemapb.Schema
	metadata       schema.SchemaMetadata
}

func (r *contractRegistry) Register(ctx context.Context, value *schemapb.Schema) (schema.SchemaMetadata, error) {
	r.registerCalled = true
	r.registered = value
	return r.metadata, nil
}

func (s *contractStorage) CreateSchedule(ctx context.Context, request *queueservicepb.CreateScheduleRequest) (*queueservicepb.CreateScheduleResponse, error) {
	return nil, domainerror.New(domainerror.InvalidArgument, "schedule is required", nil)
}

func (s *contractStorage) ValidateCalendarSchedule(ctx context.Context, schedule *schedulepb.CalendarSchedule) error {
	s.validationCall = schedule
	return s.validationErr
}

func (s *contractStorage) GetCalendarSchedulePreview(ctx context.Context, schedule *schedulepb.CalendarSchedule, count int) (*queueservicepb.PreviewCalendarScheduleResponse, error) {
	s.previewCalled = true
	s.previewCount = count
	return &queueservicepb.PreviewCalendarScheduleResponse{}, nil
}

func TestCreateScheduleNilRequestDoesNotPanic(t *testing.T) {
	server := NewChronoQueueServer(&contractStorage{}, nil, log.NewLogger())
	_, err := server.CreateSchedule(context.Background(), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
}

func TestValidateCalendarScheduleReturnsStructuredIssue(t *testing.T) {
	server := NewChronoQueueServer(&contractStorage{validationErr: errors.New("rule 2 validation failed")}, nil, log.NewLogger())
	response, err := server.ValidateCalendarSchedule(context.Background(), &queueservicepb.ValidateCalendarScheduleRequest{})
	require.NoError(t, err)
	require.False(t, response.GetValid())
	require.Len(t, response.GetValidationIssues(), 1)
	require.Equal(t, int32(2), response.GetValidationIssues()[0].GetRuleIndex())
	require.Equal(t, "rules", response.GetValidationIssues()[0].GetField())
}

func TestValidateCalendarScheduleAcceptsValidSchedule(t *testing.T) {
	storage := &contractStorage{}
	server := NewChronoQueueServer(storage, nil, log.NewLogger())
	schedule := &schedulepb.CalendarSchedule{}

	response, err := server.ValidateCalendarSchedule(context.Background(), &queueservicepb.ValidateCalendarScheduleRequest{CalendarSchedule: schedule})

	require.NoError(t, err)
	require.True(t, response.GetValid())
	require.Same(t, schedule, storage.validationCall)
}

func TestPreviewCalendarScheduleRejectsNegativeCount(t *testing.T) {
	storage := &contractStorage{}
	server := NewChronoQueueServer(storage, nil, log.NewLogger())
	_, err := server.PreviewCalendarSchedule(context.Background(), &queueservicepb.PreviewCalendarScheduleRequest{Count: -1})
	require.Equal(t, codes.InvalidArgument, status.Code(domainerror.ToGRPC(err)))
	require.False(t, storage.previewCalled)
}

func TestPreviewCalendarScheduleForwardsDefaultCount(t *testing.T) {
	storage := &contractStorage{}
	server := NewChronoQueueServer(storage, nil, log.NewLogger())

	_, err := server.PreviewCalendarSchedule(context.Background(), &queueservicepb.PreviewCalendarScheduleRequest{})

	require.NoError(t, err)
	require.True(t, storage.previewCalled)
	require.Equal(t, 10, storage.previewCount)
}

func TestRegisterSchemaRejectsUnsupportedContentType(t *testing.T) {
	registry := &contractRegistry{}
	server := NewChronoQueueServer(&contractStorage{}, registry, log.NewLogger())
	_, err := server.RegisterSchema(context.Background(), &queueservicepb.RegisterSchemaRequest{
		SchemaId: "events", Name: "Events", Content: `{"type":"record"}`, ContentType: "avro",
	})
	converted := status.Convert(domainerror.ToGRPC(err))
	require.Equal(t, codes.InvalidArgument, converted.Code())
	require.False(t, registry.registerCalled)
}

func TestRegisterSchemaAcceptsJSONSchema(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0)
	registry := &contractRegistry{metadata: schema.SchemaMetadata{SchemaID: "events", LatestVersion: 1, CreatedAt: createdAt}}
	server := NewChronoQueueServer(&contractStorage{}, registry, log.NewLogger())

	response, err := server.RegisterSchema(context.Background(), &queueservicepb.RegisterSchemaRequest{
		SchemaId: "events", Name: "Events", Content: `{"type":"object"}`, ContentType: "json-schema",
	})

	require.NoError(t, err)
	require.True(t, registry.registerCalled)
	require.Equal(t, "json-schema", registry.registered.GetContentType())
	require.Equal(t, "events", response.GetSchemaId())
	require.Equal(t, int32(1), response.GetVersion())
	require.Equal(t, createdAt.UnixMilli(), response.GetCreatedAt())
}
