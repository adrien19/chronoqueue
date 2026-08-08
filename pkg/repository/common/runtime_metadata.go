package common

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

// RuntimeMetadata carries runtime-only fields that are stored in SQL columns
// rather than inside serialized metadata_pb.
type RuntimeMetadata struct {
	State messagepb.Message_Metadata_State

	CurrentAttemptID    string
	HasCurrentAttemptID bool
	CurrentWorkerID     string
	HasCurrentWorkerID  bool

	LeaseStartedAtMs    int64
	HasLeaseStartedAtMs bool
	LeaseExpiryMs       int64
	HasLeaseExpiryMs    bool

	LeaseExtensionUsedMs    int64
	HasLeaseExtensionUsedMs bool
	LeaseRenewalCount       int32
	HasLeaseRenewalCount    bool

	LastHeartbeatAtMs    int64
	HasLastHeartbeatAtMs bool
	HeartbeatExpiryMs    int64
	HasHeartbeatExpiryMs bool
}

// ApplyRuntimeMetadata overlays runtime SQL column values onto message metadata.
// This keeps read paths (such as peek) consistent with queue state counters.
func ApplyRuntimeMetadata(msg *messagepb.Message, runtime RuntimeMetadata) {
	if msg == nil {
		return
	}

	if msg.Metadata == nil {
		msg.Metadata = &messagepb.Message_Metadata{}
	}

	msg.Metadata.State = runtime.State

	if runtime.HasLeaseExpiryMs {
		msg.Metadata.LeaseExpiry = runtime.LeaseExpiryMs
	}
	if runtime.HasLeaseRenewalCount {
		msg.Metadata.LeaseRenewalCount = runtime.LeaseRenewalCount
	}

	attempt := msg.Metadata.GetCurrentAttempt()
	hasAttemptRuntime := runtime.HasCurrentAttemptID || runtime.HasCurrentWorkerID || runtime.HasLeaseStartedAtMs ||
		runtime.HasLeaseExpiryMs || runtime.HasLastHeartbeatAtMs || runtime.HasHeartbeatExpiryMs
	hasLeaseRuntime := runtime.HasLeaseExtensionUsedMs || runtime.HasLeaseRenewalCount
	if !hasAttemptRuntime && (!hasLeaseRuntime || attempt == nil) {
		return
	}

	if attempt == nil {
		attempt = &messagepb.Message_Metadata_AttemptRuntime{}
		msg.Metadata.CurrentAttempt = attempt
	}

	if runtime.HasCurrentAttemptID {
		attempt.AttemptId = runtime.CurrentAttemptID
	}
	if runtime.HasCurrentWorkerID {
		attempt.WorkerId = runtime.CurrentWorkerID
	}
	if runtime.HasLeaseStartedAtMs {
		attempt.LeaseStartedAt = timestamppb.New(time.UnixMilli(runtime.LeaseStartedAtMs))
	}
	if runtime.HasLeaseExpiryMs {
		attempt.LeaseExpiry = runtime.LeaseExpiryMs
	}
	if runtime.HasLeaseExtensionUsedMs {
		attempt.LeaseExtensionUsed = durationpb.New(time.Duration(runtime.LeaseExtensionUsedMs) * time.Millisecond)
	}
	if runtime.HasLeaseRenewalCount {
		attempt.LeaseRenewalCount = runtime.LeaseRenewalCount
	}
	if runtime.HasLastHeartbeatAtMs {
		attempt.LastHeartbeatAt = timestamppb.New(time.UnixMilli(runtime.LastHeartbeatAtMs))
	}
	if runtime.HasHeartbeatExpiryMs {
		attempt.HeartbeatExpiry = runtime.HeartbeatExpiryMs
	}
}
