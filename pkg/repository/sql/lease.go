package sql

import (
	"fmt"
	"time"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
)

// LeaseRuntimeCalculator calculates lease expiry times and manages lease extensions.
// This provides a centralized way to compute lease-related timestamps.
type LeaseRuntimeCalculator struct {
	clock *Clock
}

// NewLeaseRuntimeCalculator creates a new lease runtime calculator
func NewLeaseRuntimeCalculator(clock *Clock) *LeaseRuntimeCalculator {
	return &LeaseRuntimeCalculator{
		clock: clock,
	}
}

// LeaseRuntime represents the calculated runtime values for a message lease.
// All times are Unix milliseconds for consistency with database storage.
type LeaseRuntime struct {
	LeaseStartedAt     int64
	LeaseExpiry        int64
	LastHeartbeatAt    int64
	HeartbeatExpiry    int64
	LeaseExtensionUsed int64
}

// CalculateLeaseRuntime computes all lease-related timestamps from a lease policy.
// This is called when a message transitions to RUNNING state.
func (lrc *LeaseRuntimeCalculator) CalculateLeaseRuntime(policy *commonpb.LeasePolicy) *LeaseRuntime {
	return lrc.CalculateLeaseRuntimeWithDuration(policy, policy.GetBaseLease().AsDuration())
}

// CalculateLeaseRuntimeWithDuration computes lease timestamps with a per-claim duration.
func (lrc *LeaseRuntimeCalculator) CalculateLeaseRuntimeWithDuration(policy *commonpb.LeasePolicy, leaseDuration time.Duration) *LeaseRuntime {
	nowMs := lrc.clock.NowMs()

	baseLeaseDurationMs := leaseDuration.Milliseconds()
	heartbeatTimeoutMs := int64(0)
	if policy.GetHeartbeatTimeout() != nil {
		heartbeatTimeoutMs = policy.GetHeartbeatTimeout().AsDuration().Milliseconds()
	}

	runtime := &LeaseRuntime{
		LeaseStartedAt:     nowMs,
		LeaseExpiry:        nowMs + baseLeaseDurationMs,
		LastHeartbeatAt:    nowMs,
		LeaseExtensionUsed: 0,
	}

	// Set heartbeat expiry if heartbeat is enabled
	if heartbeatTimeoutMs > 0 {
		runtime.HeartbeatExpiry = nowMs + heartbeatTimeoutMs
	}

	return runtime
}

// ExtendLease calculates a new lease expiry when extending a lease.
// Returns updated lease runtime or error if max extension reached.
func (lrc *LeaseRuntimeCalculator) ExtendLease(
	policy *commonpb.LeasePolicy,
	currentLeaseExpiry int64,
	currentExtensionUsed int64,
	requestedExtensionMs int64,
) (*LeaseRuntime, error) {
	maxExtensionMs := policy.GetMaxExtension().AsDuration().Milliseconds()
	extendStepMs := policy.GetExtendStep().AsDuration().Milliseconds()

	// Check if we can extend
	if currentExtensionUsed >= maxExtensionMs {
		return nil, ErrMaxExtensionReached
	}

	// Calculate actual extension (use extend_step if not specified, cap by remaining)
	actualExtensionMs := extendStepMs
	if requestedExtensionMs > 0 {
		actualExtensionMs = requestedExtensionMs
	}

	remainingExtensionMs := maxExtensionMs - currentExtensionUsed
	if actualExtensionMs > remainingExtensionMs {
		actualExtensionMs = remainingExtensionMs
	}
	if actualExtensionMs <= 0 {
		return nil, ErrInvalidLeaseExtension
	}

	newExtensionUsed := currentExtensionUsed + actualExtensionMs
	newLeaseExpiry := currentLeaseExpiry + actualExtensionMs
	if newLeaseExpiry <= currentLeaseExpiry {
		return nil, ErrInvalidLeaseExtension
	}

	return &LeaseRuntime{
		LeaseExpiry:        newLeaseExpiry,
		LeaseExtensionUsed: newExtensionUsed,
	}, nil
}

// Heartbeat updates the heartbeat expiry for an active lease.
// Returns updated runtime with new heartbeat timestamp.
func (lrc *LeaseRuntimeCalculator) Heartbeat() *LeaseRuntime {
	nowMs := lrc.clock.NowMs()

	// Default heartbeat timeout if not specified (30 seconds)
	heartbeatTimeoutMs := int64(30000)

	return &LeaseRuntime{
		LastHeartbeatAt: nowMs,
		HeartbeatExpiry: nowMs + heartbeatTimeoutMs,
	}
}

// Common errors
var (
	ErrMaxExtensionReached   = fmt.Errorf("maximum lease extension reached")
	ErrInvalidLeaseExtension = fmt.Errorf("lease extension must advance expiry by at least 1 millisecond")
)
