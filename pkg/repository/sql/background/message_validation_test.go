package background

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
	schemas "github.com/adrien19/chronoqueue/pkg/schema"
)

type rejectingSchemaRegistry struct{}

type acceptingSchemaRegistry struct{ rejectingSchemaRegistry }

func (acceptingSchemaRegistry) Validate(context.Context, string, int32, []byte) (*schemapb.ValidationResult, error) {
	return &schemapb.ValidationResult{Valid: true}, nil
}

func (rejectingSchemaRegistry) Register(context.Context, *schemapb.Schema) (schemas.SchemaMetadata, error) {
	return schemas.SchemaMetadata{}, nil
}

func (rejectingSchemaRegistry) Get(context.Context, string, int32) (*schemapb.Schema, error) {
	return nil, nil
}

func (rejectingSchemaRegistry) GetLatest(context.Context, string) (*schemapb.Schema, error) {
	return nil, nil
}
func (rejectingSchemaRegistry) List(context.Context) ([]*schemapb.Schema, error) { return nil, nil }
func (rejectingSchemaRegistry) ListWithOptions(context.Context, schemas.ListOptions) (schemas.ListResult, error) {
	return schemas.ListResult{}, nil
}

func (rejectingSchemaRegistry) Deactivate(context.Context, string, int32) (int32, error) {
	return 0, nil
}

func (rejectingSchemaRegistry) Validate(context.Context, string, int32, []byte) (*schemapb.ValidationResult, error) {
	return &schemapb.ValidationResult{Valid: false, Errors: []*schemapb.ValidationError{{Field: "payload", Message: "schema rejected payload"}}}, nil
}

func (rejectingSchemaRegistry) IsCompatible(context.Context, string, string) (bool, error) {
	return false, nil
}

func TestValidateScheduledMessageUsesConfiguredSchemaRegistry(t *testing.T) {
	base := &repositorysql.BaseSQL{}
	base.SetSchemaRegistry(rejectingSchemaRegistry{})
	payload, err := structpb.NewStruct(map[string]any{"value": "invalid"})
	require.NoError(t, err)

	err = validateScheduledMessage(context.Background(), base, &queuepb.QueueMetadata{
		SchemaRequired: true,
		SchemaId:       "required-schema",
	}, &messagepb.Message{
		MessageId: "scheduled-message",
		Metadata: &messagepb.Message_Metadata{Payload: &commonpb.Payload{
			Data:        payload,
			ContentType: "application/json",
		}},
	})
	require.ErrorContains(t, err, "schema rejected payload")
}

func TestValidateScheduledMessageAcceptsValidRequiredSchema(t *testing.T) {
	base := &repositorysql.BaseSQL{}
	base.SetSchemaRegistry(acceptingSchemaRegistry{})
	payload, err := structpb.NewStruct(map[string]any{"value": "valid"})
	require.NoError(t, err)

	err = validateScheduledMessage(context.Background(), base, &queuepb.QueueMetadata{
		SchemaRequired: true,
		SchemaId:       "required-schema",
	}, &messagepb.Message{
		MessageId: "scheduled-message",
		Metadata: &messagepb.Message_Metadata{Payload: &commonpb.Payload{
			Data:        payload,
			ContentType: "application/json",
		}},
	})
	require.NoError(t, err)
}
