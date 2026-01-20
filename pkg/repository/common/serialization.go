package common

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

// ProtoSerializer handles protobuf serialization/deserialization for ChronoQueue entities.
// This provides a centralized way to marshal/unmarshal protobufs across all storage backends.
type ProtoSerializer struct{}

// NewProtoSerializer creates a new ProtoSerializer instance
func NewProtoSerializer() *ProtoSerializer {
	return &ProtoSerializer{}
}

// Queue Metadata

func (s *ProtoSerializer) MarshalQueueMetadata(meta *queuepb.QueueMetadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("queue metadata is nil")
	}
	data, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal queue metadata: %w", err)
	}
	return data, nil
}

func (s *ProtoSerializer) UnmarshalQueueMetadata(data []byte) (*queuepb.QueueMetadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty queue metadata data")
	}
	meta := &queuepb.QueueMetadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, fmt.Errorf("unmarshal queue metadata: %w", err)
	}
	return meta, nil
}

// Message Metadata

func (s *ProtoSerializer) MarshalMessageMetadata(meta *messagepb.Message_Metadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("message metadata is nil")
	}
	data, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal message metadata: %w", err)
	}
	return data, nil
}

func (s *ProtoSerializer) UnmarshalMessageMetadata(data []byte) (*messagepb.Message_Metadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty message metadata data")
	}
	meta := &messagepb.Message_Metadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, fmt.Errorf("unmarshal message metadata: %w", err)
	}
	return meta, nil
}

// Schedule Metadata

func (s *ProtoSerializer) MarshalScheduleMetadata(meta *schedulepb.Schedule_Metadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("schedule metadata is nil")
	}
	data, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal schedule metadata: %w", err)
	}
	return data, nil
}

func (s *ProtoSerializer) UnmarshalScheduleMetadata(data []byte) (*schedulepb.Schedule_Metadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty schedule metadata data")
	}
	meta := &schedulepb.Schedule_Metadata{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, fmt.Errorf("unmarshal schedule metadata: %w", err)
	}
	return meta, nil
}

// Full Object Serialization (for storing complete entities)

// MarshalMessage marshals a complete Message object
func (s *ProtoSerializer) MarshalMessage(msg *messagepb.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("message is nil")
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}
	return data, nil
}

// UnmarshalMessage unmarshals a complete Message object
func (s *ProtoSerializer) UnmarshalMessage(data []byte) (*messagepb.Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty message data")
	}
	msg := &messagepb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

// MarshalQueue marshals a complete Queue object
func (s *ProtoSerializer) MarshalQueue(queue *queuepb.Queue) ([]byte, error) {
	if queue == nil {
		return nil, fmt.Errorf("queue is nil")
	}
	data, err := proto.Marshal(queue)
	if err != nil {
		return nil, fmt.Errorf("marshal queue: %w", err)
	}
	return data, nil
}

// UnmarshalQueue unmarshals a complete Queue object
func (s *ProtoSerializer) UnmarshalQueue(data []byte) (*queuepb.Queue, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty queue data")
	}
	queue := &queuepb.Queue{}
	if err := proto.Unmarshal(data, queue); err != nil {
		return nil, fmt.Errorf("unmarshal queue: %w", err)
	}
	return queue, nil
}

// MarshalSchedule marshals a complete Schedule object
func (s *ProtoSerializer) MarshalSchedule(schedule *schedulepb.Schedule) ([]byte, error) {
	if schedule == nil {
		return nil, fmt.Errorf("schedule is nil")
	}
	data, err := proto.Marshal(schedule)
	if err != nil {
		return nil, fmt.Errorf("marshal schedule: %w", err)
	}
	return data, nil
}

// UnmarshalSchedule unmarshals a complete Schedule object
func (s *ProtoSerializer) UnmarshalSchedule(data []byte) (*schedulepb.Schedule, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty schedule data")
	}
	schedule := &schedulepb.Schedule{}
	if err := proto.Unmarshal(data, schedule); err != nil {
		return nil, fmt.Errorf("unmarshal schedule: %w", err)
	}
	return schedule, nil
}
