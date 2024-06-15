package schedule

import (
	"encoding/json"
	"errors"
	"time"

	schedule_pb "github.com/adrien19/chronoqueue/api/schedule/v1"
	"github.com/adrien19/chronoqueue/internal/encryption"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/common"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type State int32

const (
	SCHEDULED State = 0
	CANCELED  State = 1
	ERRORED   State = 2
	PAUSED    State = 3
)

type Metadata struct {
	Payload        common.Payload
	State          State
	CronSchedule   string
	QueueName      string
	MessageIds     []string
	NextRun        time.Time
	LastRun        time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ExclusivityKey string
	StateMessage   string
	Priority       int64
	HasMaxMessages bool
	MaxMessages    int64
	LeaseDuration  time.Duration
}

func durationToProto(d time.Duration) *durationpb.Duration {
	seconds := int64(d.Seconds())
	nanos := int32(d.Nanoseconds() - seconds*1e9)
	return &durationpb.Duration{Seconds: seconds, Nanos: nanos}
}

func NewMetadataFromProto(metadata *schedule_pb.Schedule_Metadata) Metadata {
	return Metadata{
		Payload:        *common.NewPayloadFromProto(metadata.Payload),
		State:          State(metadata.State),
		CronSchedule:   metadata.CronSchedule,
		QueueName:      metadata.QueueName,
		MessageIds:     metadata.MessageIds,
		NextRun:        time.Unix(metadata.NextRun.Seconds, int64(metadata.NextRun.Nanos)),
		LastRun:        time.Unix(metadata.LastRun.Seconds, int64(metadata.LastRun.Nanos)),
		CreatedAt:      time.Unix(metadata.CreatedAt.Seconds, int64(metadata.CreatedAt.Nanos)),
		UpdatedAt:      time.Unix(metadata.UpdatedAt.Seconds, int64(metadata.UpdatedAt.Nanos)),
		ExclusivityKey: metadata.ExclusivityKey,
		StateMessage:   metadata.StateMessage,
		Priority:       metadata.Priority,
		HasMaxMessages: metadata.HasMaxMessages,
		MaxMessages:    metadata.MaxMessages,
		LeaseDuration:  time.Duration(metadata.LeaseDuration.AsDuration()),
	}
}

func (meta *Metadata) ToProto() *schedule_pb.Schedule_Metadata {
	return &schedule_pb.Schedule_Metadata{
		Payload:        meta.Payload.ToProto(),
		State:          schedule_pb.Schedule_Metadata_State(meta.State),
		CronSchedule:   meta.CronSchedule,
		QueueName:      meta.QueueName,
		MessageIds:     meta.MessageIds,
		NextRun:        &timestamppb.Timestamp{Seconds: meta.NextRun.Unix(), Nanos: int32(meta.NextRun.Nanosecond())},
		LastRun:        &timestamppb.Timestamp{Seconds: meta.LastRun.Unix(), Nanos: int32(meta.LastRun.Nanosecond())},
		CreatedAt:      &timestamppb.Timestamp{Seconds: meta.CreatedAt.Unix(), Nanos: int32(meta.CreatedAt.Nanosecond())},
		UpdatedAt:      &timestamppb.Timestamp{Seconds: meta.UpdatedAt.Unix(), Nanos: int32(meta.UpdatedAt.Nanosecond())},
		ExclusivityKey: meta.ExclusivityKey,
		StateMessage:   meta.StateMessage,
		Priority:       meta.Priority,
		HasMaxMessages: meta.HasMaxMessages,
		MaxMessages:    meta.MaxMessages,
		LeaseDuration:  durationToProto(meta.LeaseDuration),
	}
}

func (meta *Metadata) ToBytes() ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(meta.ToProto())
}

func (meta *Metadata) EncryptPayload(keyManager *keymanager.EncryptionKeyManager) error {
	if ok := meta.Payload.IsPayloadEncrypted(); ok {
		return nil
	}

	payloadData, err := json.Marshal(meta.Payload)
	if err != nil {
		return err
	}

	// Encrypt the payload data
	encryptedPayload, nonce, err := encryption.EncryptPayload(payloadData, keyManager)
	if err != nil {
		return err
	}

	if encryptedPayload != "" && nonce != "" {
		meta.Payload = common.Payload{}
		meta.Payload.Metadata = make(map[string]*structpb.Value)
	}

	// Update to the metadata field of Payload
	meta.Payload.Metadata["encryptedPayload"] = structpb.NewStringValue(encryptedPayload)
	meta.Payload.Metadata["nonce"] = structpb.NewStringValue(nonce)

	if ok := meta.Payload.IsPayloadEncrypted(); !ok {
		return errors.New("failed to updated encryptedPayload or nonce in metadata")
	}
	return nil
}

type Schedule struct {
	ScheduleId string
	Metadata   Metadata
}

func NewScheduleFromProto(schedule *schedule_pb.Schedule) (*Schedule, error) {

	return &Schedule{
		ScheduleId: schedule.ScheduleId,
		Metadata:   NewMetadataFromProto(schedule.Metadata),
	}, nil
}

func (s *Schedule) ToProto() (*schedule_pb.Schedule, error) {
	return &schedule_pb.Schedule{
		ScheduleId: s.ScheduleId,
		Metadata:   s.Metadata.ToProto(),
	}, nil
}

type Execution struct {
	ID            string
	ScheduleID    string
	ExecutionTime time.Time
	Status        string
	Message       string
	WorkerID      string
}
