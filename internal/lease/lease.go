// internal/lease/lease.go
package lease

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
)

// LeasePolicy is the internal, computed policy for a single attempt.
// It merges queue-level and message-level LeasePolicy protos.
type LeasePolicy struct {
	// BaseTimeout is the initial lease (L_base).
	BaseTimeout time.Duration

	// MaxExtension is the total extra lease time allowed (L_maxExt).
	// Effective hard cap per attempt is BaseTimeout + MaxExtension.
	MaxExtension time.Duration

	// HeartbeatTimeout is the max gap between heartbeats (H).
	// If 0, heartbeat timeout is disabled.
	HeartbeatTimeout time.Duration

	// ExtendStep is the amount to extend the lease by on each heartbeat (Δ),
	// until MaxExtension is exhausted.
	ExtendStep time.Duration
}

// LeaseRuntime is the internal runtime state for the current attempt.
type LeaseRuntime struct {
	AttemptID string
	WorkerID  string

	// When this attempt acquired the lease.
	LeaseStart time.Time

	// Current lease deadline for this attempt.
	LeaseExpiry time.Time

	// Total extension used so far beyond BaseTimeout.
	ExtensionUsed time.Duration

	// How many times we successfully extended the lease.
	RenewalCount int32

	// Last successful heartbeat.
	LastHeartbeat time.Time

	// Deadline for heartbeat timeout (LastHeartbeat + HeartbeatTimeout).
	// Zero if heartbeat timeout disabled.
	HeartbeatExpiry time.Time
}

// HeartbeatResult describes what happened when applying a heartbeat.
type HeartbeatResult struct {
	// True if this heartbeat extended the lease.
	Extended bool

	// Timeouts observed at the moment heartbeat was processed.
	LeaseTimedOut     bool
	HeartbeatTimedOut bool

	// If true, caller should treat this attempt as failed / timed out
	// (e.g., trigger retry/failure logic).
	ShouldFail bool
}

// TimeoutStatus describes timeout checks at a given instant.
type TimeoutStatus struct {
	LeaseTimedOut     bool
	HeartbeatTimedOut bool
	ExpiredAt         time.Time
}

// -----------------------------------------------------------------------------
// Helpers: proto duration / timestamp
// -----------------------------------------------------------------------------

func durOrZero(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func tsOrZero(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// -----------------------------------------------------------------------------
// Policy merging
// -----------------------------------------------------------------------------

// MergeLeasePolicy builds an effective LeasePolicy by merging a queue-level
// LeasePolicy and an optional per-message LeasePolicy override.
//
// Semantics:
//   - Start from queue.metadata.lease_policy.
//   - For each field set in message.metadata.lease_policy, override only that field.
//   - Apply internal defaults if still zero (e.g., BaseTimeout, ExtendStep).
func MergeLeasePolicy(
	q *queuepb.QueueMetadata,
	m *messagepb.Message_Metadata,
	workerBaseHint time.Duration, // from GetNextMessageRequest.LeaseDuration
) LeasePolicy {
	var base, maxExt, hb, step time.Duration

	// 1) queue defaults
	if lp := q.GetLeasePolicy(); lp != nil {
		base = durOrZero(lp.BaseLease)
		maxExt = durOrZero(lp.MaxExtension)
		hb = durOrZero(lp.HeartbeatTimeout)
		step = durOrZero(lp.ExtendStep)
	}

	// 2) worker hint, if message doesn’t override it
	if workerBaseHint > 0 {
		base = workerBaseHint
	}

	// 3) message overrides take precedence over worker hints
	if mlp := m.GetLeasePolicy(); mlp != nil {
		if mlp.BaseLease != nil {
			base = durOrZero(mlp.BaseLease)
		}
		if mlp.MaxExtension != nil {
			maxExt = durOrZero(mlp.MaxExtension)
		}
		if mlp.HeartbeatTimeout != nil {
			hb = durOrZero(mlp.HeartbeatTimeout)
		}
		if mlp.ExtendStep != nil {
			step = durOrZero(mlp.ExtendStep)
		}
	}

	// caps & defaults
	if base <= 0 {
		base = 3 * time.Second
	}
	// TODO: Should we enforce queue-level max_base_lease here? --- IGNORE ---
	if step <= 0 {
		step = base / 5
		if step <= 0 {
			step = 500 * time.Millisecond
		}
	}
	if maxExt < 0 {
		maxExt = 0
	}

	return LeasePolicy{
		BaseTimeout:      base,
		MaxExtension:     maxExt,
		HeartbeatTimeout: hb,
		ExtendStep:       step,
	}
}

// -----------------------------------------------------------------------------
// Runtime <-> proto conversions
// -----------------------------------------------------------------------------

// RuntimeFromProto builds a LeaseRuntime from the nested AttemptRuntime proto.
// If ar is nil, returns a zero-valued runtime.
func RuntimeFromProto(ar *messagepb.Message_Metadata_AttemptRuntime) LeaseRuntime {
	if ar == nil {
		return LeaseRuntime{}
	}

	rt := LeaseRuntime{
		AttemptID:       ar.GetAttemptId(),
		WorkerID:        ar.GetWorkerId(),
		LeaseStart:      tsOrZero(ar.GetLeaseStartedAt()),
		LeaseExpiry:     time.UnixMilli(ar.GetLeaseExpiry()),
		ExtensionUsed:   durOrZero(ar.GetLeaseExtensionUsed()),
		RenewalCount:    ar.GetLeaseRenewalCount(),
		LastHeartbeat:   tsOrZero(ar.GetLastHeartbeatAt()),
		HeartbeatExpiry: time.UnixMilli(ar.GetHeartbeatExpiry()),
	}

	// If LeaseExpiry is zero, interpret as "not set yet".
	if rt.LeaseExpiry.UnixMilli() == 0 {
		rt.LeaseExpiry = time.Time{}
	}
	if rt.HeartbeatExpiry.UnixMilli() == 0 {
		rt.HeartbeatExpiry = time.Time{}
	}

	return rt
}

// ApplyRuntimeToProto writes the LeaseRuntime back into the AttemptRuntime proto.
func ApplyRuntimeToProto(rt LeaseRuntime, ar *messagepb.Message_Metadata_AttemptRuntime) {
	if ar == nil {
		return
	}

	ar.AttemptId = rt.AttemptID
	ar.WorkerId = rt.WorkerID

	if !rt.LeaseStart.IsZero() {
		ar.LeaseStartedAt = timestamppb.New(rt.LeaseStart)
	}

	ar.LeaseExpiry = rt.LeaseExpiry.UnixMilli()
	ar.LeaseExtensionUsed = durationpb.New(rt.ExtensionUsed)
	ar.LeaseRenewalCount = rt.RenewalCount

	if !rt.LastHeartbeat.IsZero() {
		ar.LastHeartbeatAt = timestamppb.New(rt.LastHeartbeat)
	}

	if !rt.HeartbeatExpiry.IsZero() {
		ar.HeartbeatExpiry = rt.HeartbeatExpiry.UnixMilli()
	} else {
		ar.HeartbeatExpiry = 0
	}
}

// InitRuntime initializes LeaseRuntime for a *new attempt* using the
// effective LeasePolicy, attemptID, and workerID.
func (p LeasePolicy) InitRuntime(now time.Time, attemptID, workerID string) LeaseRuntime {
	rt := LeaseRuntime{
		AttemptID:   attemptID,
		WorkerID:    workerID,
		LeaseStart:  now,
		LeaseExpiry: now.Add(p.BaseTimeout),

		ExtensionUsed:   0,
		RenewalCount:    0,
		LastHeartbeat:   now,
		HeartbeatExpiry: time.Time{},
	}
	if p.HeartbeatTimeout > 0 {
		rt.HeartbeatExpiry = now.Add(p.HeartbeatTimeout)
	}
	return rt
}

// -----------------------------------------------------------------------------
// Heartbeat & reclaim logic
// -----------------------------------------------------------------------------

// ApplyHeartbeat updates runtime according to the policy when a heartbeat is
// received at time `now`.
//
// It:
//   - Checks if the attempt is already timed out.
//   - Updates LastHeartbeat (+ HeartbeatExpiry if enabled).
//   - Extends LeaseExpiry in increments of ExtendStep, bounded by MaxExtension.
func (p LeasePolicy) ApplyHeartbeat(rt *LeaseRuntime, now time.Time) HeartbeatResult {
	res := HeartbeatResult{}

	// Already past lease deadline?
	if !rt.LeaseExpiry.IsZero() && now.After(rt.LeaseExpiry) {
		res.LeaseTimedOut = true
		res.ShouldFail = true
		return res
	}

	// Heartbeat timeout?
	if !rt.HeartbeatExpiry.IsZero() && now.After(rt.HeartbeatExpiry) {
		res.HeartbeatTimedOut = true
		res.ShouldFail = true
		return res
	}

	// Update heartbeat timestamps first.
	rt.LastHeartbeat = now
	if p.HeartbeatTimeout > 0 {
		rt.HeartbeatExpiry = now.Add(p.HeartbeatTimeout)
	}

	// Extension logic.
	if p.MaxExtension > 0 && rt.ExtensionUsed < p.MaxExtension {
		remainingBudget := p.MaxExtension - rt.ExtensionUsed
		inc := p.ExtendStep
		if inc > remainingBudget {
			inc = remainingBudget
		}
		if inc > 0 {
			base := rt.LeaseExpiry
			if base.IsZero() {
				base = now
			}
			if now.After(base) {
				base = now
			}
			rt.LeaseExpiry = base.Add(inc)
			rt.ExtensionUsed += inc
			rt.RenewalCount++
			res.Extended = true
		}
	}

	return res
}

// CheckTimeouts determines if the attempt has timed out as of `now`.
func (p LeasePolicy) CheckTimeouts(rt LeaseRuntime, now time.Time) TimeoutStatus {
	var st TimeoutStatus

	if !rt.LeaseExpiry.IsZero() && now.After(rt.LeaseExpiry) {
		st.LeaseTimedOut = true
		st.ExpiredAt = rt.LeaseExpiry
		return st
	}

	if !rt.HeartbeatExpiry.IsZero() && now.After(rt.HeartbeatExpiry) {
		st.HeartbeatTimedOut = true
		st.ExpiredAt = rt.HeartbeatExpiry
		return st
	}

	return st
}

// -----------------------------------------------------------------------------
// Convenience helpers for server handlers
// -----------------------------------------------------------------------------

// HandleHeartbeat is a high-level helper you can call in your heartbeat RPC:
//
//   - Merges queue + message lease policy.
//   - Loads current runtime from metadata.current_attempt.
//   - Applies heartbeat semantics.
//   - Writes updated runtime back into metadata.current_attempt.
//   - Returns HeartbeatResult so you can decide whether to fail/retry.
func HandleHeartbeat(
	q *queuepb.QueueMetadata,
	m *messagepb.Message_Metadata,
	now time.Time,
) HeartbeatResult {
	policy := MergeLeasePolicy(q, m, 0)

	ar := m.GetCurrentAttempt()
	if ar == nil {
		// No current attempt; this heartbeat is stale / invalid.
		return HeartbeatResult{
			ShouldFail: true,
		}
	}

	rt := RuntimeFromProto(ar)
	res := policy.ApplyHeartbeat(&rt, now)
	if !res.ShouldFail {
		ApplyRuntimeToProto(rt, ar)
	}
	return res
}

// HandleReclaim is a high-level helper you can use in a reclaimer/timer worker.
//
//   - Merges lease policy.
//   - Loads runtime from current_attempt.
//   - Checks for timeouts.
//   - Returns status so you can apply retry/failure logic on the message.
//
// It does NOT mutate the message; you decide how to change state/attempts.
func HandleReclaim(
	q *queuepb.QueueMetadata,
	m *messagepb.Message_Metadata,
	now time.Time,
) TimeoutStatus {
	policy := MergeLeasePolicy(q, m, 0)

	ar := m.GetCurrentAttempt()
	if ar == nil {
		// No current attempt => nothing to reclaim from lease perspective.
		return TimeoutStatus{}
	}

	rt := RuntimeFromProto(ar)
	return policy.CheckTimeouts(rt, now)
}

// -----------------------------------------------------------------------------
// Optional: helper to compute an effective LeasePolicy from queue-only config,
// when you don't have a message (e.g. for defaults or tests).
// -----------------------------------------------------------------------------

func LeasePolicyFromCommon(lp *commonpb.LeasePolicy) LeasePolicy {
	if lp == nil {
		return LeasePolicy{
			BaseTimeout: 3 * time.Second,
			ExtendStep:  600 * time.Millisecond,
		}
	}

	base := durOrZero(lp.BaseLease)
	maxExt := durOrZero(lp.MaxExtension)
	hb := durOrZero(lp.HeartbeatTimeout)
	step := durOrZero(lp.ExtendStep)

	if base <= 0 {
		base = 3 * time.Second
	}
	if step <= 0 {
		step = base / 5
		if step <= 0 {
			step = 500 * time.Millisecond
		}
	}
	if maxExt < 0 {
		maxExt = 0
	}

	return LeasePolicy{
		BaseTimeout:      base,
		MaxExtension:     maxExt,
		HeartbeatTimeout: hb,
		ExtendStep:       step,
	}
}

// -----------------------------------------------------------------------------
// Optional: helper to generate attempt ID strings.
// -----------------------------------------------------------------------------

// GenerateAttemptID creates a compact, collision-resistant attempt ID.
// Format: att_<timestamp><hash> (20 chars total)
// - Timestamp: base32-encoded milliseconds (sortable)
// - Hash: 6-char base32 hash of (messageID + workerID + nanos + random)
func GenerateAttemptID(messageID, workerID string, now time.Time) string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ" // Crockford base32

	// 1. Timestamp component (10 chars, millisecond precision)
	tsMs := uint64(now.UnixMilli())
	timeStr := encodeBase32(tsMs, alphabet, 10)

	// 2. Collision-resistant hash component (6 chars)
	// Include ALL uniqueness factors: message, worker, nanos, AND random bytes
	hasher := sha256.New()
	hasher.Write([]byte(messageID))
	hasher.Write([]byte(workerID))
	_ = binary.Write(hasher, binary.LittleEndian, now.UnixNano())

	// Add 4 random bytes for additional entropy
	rb := make([]byte, 4)
	_, _ = rand.Read(rb)
	hasher.Write(rb)

	hash := hasher.Sum(nil)
	// Take first 4 bytes of hash, encode to 6 chars
	hashValue := uint64(binary.BigEndian.Uint32(hash[:4]))
	hashStr := encodeBase32(hashValue, alphabet, 6)

	return fmt.Sprintf("att_%s%s", timeStr, hashStr) // e.g., "att_01HQZN8K7G2R5W"
}

func encodeBase32(value uint64, alphabet string, length int) string {
	result := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		result[i] = alphabet[value%32]
		value /= 32
	}
	return string(result)
}
