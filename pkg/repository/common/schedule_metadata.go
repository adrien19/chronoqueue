package common

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	schedulepb "github.com/adrien19/chronoqueue/api/schedule/v1"
)

const legacyMessageIDsField protoreflect.Name = "message_ids"

// HasLegacyScheduleMessageIDs reports whether deprecated generated-message metadata is present.
func HasLegacyScheduleMessageIDs(metadata *schedulepb.Schedule_Metadata) bool {
	if metadata == nil {
		return false
	}
	message := metadata.ProtoReflect()
	field := message.Descriptor().Fields().ByName(legacyMessageIDsField)
	return field != nil && message.Get(field).List().Len() > 0
}

// ClearLegacyScheduleMessageIDs compacts deprecated generated-message metadata.
func ClearLegacyScheduleMessageIDs(metadata *schedulepb.Schedule_Metadata) {
	if metadata == nil {
		return
	}
	message := metadata.ProtoReflect()
	field := message.Descriptor().Fields().ByName(legacyMessageIDsField)
	if field != nil {
		message.Clear(field)
	}
}
