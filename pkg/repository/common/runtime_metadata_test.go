package common

import (
	"testing"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

func TestApplyRuntimeMetadata_AllowsNilMessage(t *testing.T) {
	ApplyRuntimeMetadata(nil, RuntimeMetadata{
		State: messagepb.Message_Metadata_RUNNING,
	})
}

func TestApplyRuntimeMetadata_UpdatesStateAndAttemptRuntime(t *testing.T) {
	msg := &messagepb.Message{
		MessageId: "msg-1",
		Metadata: &messagepb.Message_Metadata{
			State: messagepb.Message_Metadata_PENDING,
		},
	}

	ApplyRuntimeMetadata(msg, RuntimeMetadata{
		State:                   messagepb.Message_Metadata_RUNNING,
		AttemptsLeft:            2,
		HasAttemptsLeft:         true,
		MaxAttempts:             4,
		HasMaxAttempts:          true,
		CurrentAttemptID:        "attempt-1",
		HasCurrentAttemptID:     true,
		CurrentWorkerID:         "worker-A",
		HasCurrentWorkerID:      true,
		LeaseStartedAtMs:        1_000,
		HasLeaseStartedAtMs:     true,
		LeaseExpiryMs:           61_000,
		HasLeaseExpiryMs:        true,
		LeaseExtensionUsedMs:    15_000,
		HasLeaseExtensionUsedMs: true,
		LeaseRenewalCount:       2,
		HasLeaseRenewalCount:    true,
		LastHeartbeatAtMs:       30_000,
		HasLastHeartbeatAtMs:    true,
		HeartbeatExpiryMs:       90_000,
		HasHeartbeatExpiryMs:    true,
	})

	if got := msg.GetMetadata().GetState(); got != messagepb.Message_Metadata_RUNNING {
		t.Fatalf("expected state RUNNING, got %v", got)
	}
	if got := msg.GetMetadata().GetAttemptsLeft(); got != 2 {
		t.Fatalf("expected 2 attempts left, got %d", got)
	}
	if got := msg.GetMetadata().GetMaxAttempts(); got != 4 {
		t.Fatalf("expected 4 max attempts, got %d", got)
	}

	attempt := msg.GetMetadata().GetCurrentAttempt()
	if attempt == nil {
		t.Fatalf("expected current attempt runtime to be populated")
	}
	if attempt.GetAttemptId() != "attempt-1" {
		t.Fatalf("expected attempt id attempt-1, got %q", attempt.GetAttemptId())
	}
	if attempt.GetWorkerId() != "worker-A" {
		t.Fatalf("expected worker id worker-A, got %q", attempt.GetWorkerId())
	}
	if attempt.GetLeaseStartedAt() == nil || attempt.GetLeaseStartedAt().AsTime().UnixMilli() != 1_000 {
		t.Fatalf("expected lease started at 1000ms, got %v", attempt.GetLeaseStartedAt())
	}
	if attempt.GetLeaseExpiry() != 61_000 {
		t.Fatalf("expected attempt lease expiry 61000, got %d", attempt.GetLeaseExpiry())
	}
	if attempt.GetLeaseExtensionUsed() == nil || attempt.GetLeaseExtensionUsed().AsDuration().Milliseconds() != 15_000 {
		t.Fatalf("expected lease extension used 15000ms, got %v", attempt.GetLeaseExtensionUsed())
	}
	if attempt.GetLeaseRenewalCount() != 2 {
		t.Fatalf("expected attempt lease renewal count 2, got %d", attempt.GetLeaseRenewalCount())
	}
	if attempt.GetLastHeartbeatAt() == nil || attempt.GetLastHeartbeatAt().AsTime().UnixMilli() != 30_000 {
		t.Fatalf("expected last heartbeat at 30000ms, got %v", attempt.GetLastHeartbeatAt())
	}
	if attempt.GetHeartbeatExpiry() != 90_000 {
		t.Fatalf("expected heartbeat expiry 90000, got %d", attempt.GetHeartbeatExpiry())
	}

	if msg.GetMetadata().GetLeaseExpiry() != 61_000 {
		t.Fatalf("expected top-level lease expiry 61000, got %d", msg.GetMetadata().GetLeaseExpiry())
	}
	if msg.GetMetadata().GetLeaseRenewalCount() != 2 {
		t.Fatalf("expected top-level lease renewal count 2, got %d", msg.GetMetadata().GetLeaseRenewalCount())
	}
}

func TestApplyRuntimeMetadata_PreservesAttemptWhenNoRuntimeColumns(t *testing.T) {
	msg := &messagepb.Message{
		MessageId: "msg-2",
		Metadata: &messagepb.Message_Metadata{
			State: messagepb.Message_Metadata_PENDING,
			CurrentAttempt: &messagepb.Message_Metadata_AttemptRuntime{
				AttemptId: "existing-attempt",
			},
		},
	}

	ApplyRuntimeMetadata(msg, RuntimeMetadata{
		State: messagepb.Message_Metadata_PENDING,
	})

	if msg.GetMetadata().GetCurrentAttempt() == nil {
		t.Fatalf("expected current attempt to remain present")
	}
	if msg.GetMetadata().GetCurrentAttempt().GetAttemptId() != "existing-attempt" {
		t.Fatalf("expected existing attempt to remain unchanged")
	}
}

func TestApplyRuntimeMetadata_DoesNotCreateAttemptForZeroOnlyRuntimeColumns(t *testing.T) {
	msg := &messagepb.Message{
		MessageId: "msg-3",
		Metadata: &messagepb.Message_Metadata{
			State: messagepb.Message_Metadata_PENDING,
		},
	}

	ApplyRuntimeMetadata(msg, RuntimeMetadata{
		State:                   messagepb.Message_Metadata_PENDING,
		LeaseExtensionUsedMs:    0,
		HasLeaseExtensionUsedMs: true,
		LeaseRenewalCount:       0,
		HasLeaseRenewalCount:    true,
		HeartbeatExpiryMs:       0,
		HasHeartbeatExpiryMs:    false,
	})

	if msg.GetMetadata().GetCurrentAttempt() != nil {
		t.Fatalf("expected no current attempt runtime for zero-only columns")
	}
}
