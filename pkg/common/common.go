package common

import (
	common_pb "github.com/adrien19/chronoqueue/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

type Payload struct {
	Metadata map[string]*structpb.Value
	Data     *structpb.Struct
}

func NewPayloadFromProto(payload *common_pb.Payload) *Payload {
	return &Payload{
		Metadata: payload.Metadata,
		Data:     payload.Data,
	}
}

func (p *Payload) ToProto() *common_pb.Payload {
	return &common_pb.Payload{
		Metadata: p.Metadata,
		Data:     p.Data,
	}
}

func (p *Payload) ToBytes() ([]byte, error) {
	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return m.Marshal(p.ToProto())
}

func (p *Payload) IsPayloadEncrypted() bool {
	if p.Metadata["encryptedPayload"].GetStringValue() == "" || p.Metadata["nonce"].GetStringValue() == "" {
		return false
	}
	return true
}
