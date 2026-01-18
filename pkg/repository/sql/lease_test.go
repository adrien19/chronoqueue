package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
)

func TestLeaseRuntimeCalculator_CalculateLeaseRuntime(t *testing.T) {
	clock := NewClock()
	calculator := NewLeaseRuntimeCalculator(clock)

	tests := []struct {
		name                string
		policy              *commonpb.LeasePolicy
		checkHeartbeat      bool
		expectedMinDuration int64 // milliseconds
	}{
		{
			name: "default lease duration (30s)",
			policy: &commonpb.LeasePolicy{
				BaseLease: durationpb.New(30 * time.Second),
			},
			checkHeartbeat:      false,
			expectedMinDuration: 30000,
		},
		{
			name: "with heartbeat timeout",
			policy: &commonpb.LeasePolicy{
				BaseLease:        durationpb.New(60 * time.Second),
				HeartbeatTimeout: durationpb.New(10 * time.Second),
			},
			checkHeartbeat:      true,
			expectedMinDuration: 60000,
		},
		{
			name: "long lease (5 minutes)",
			policy: &commonpb.LeasePolicy{
				BaseLease: durationpb.New(5 * time.Minute),
			},
			checkHeartbeat:      false,
			expectedMinDuration: 300000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startTime := clock.NowMs()
			runtime := calculator.CalculateLeaseRuntime(tt.policy)

			assert.GreaterOrEqual(t, runtime.LeaseStartedAt, startTime, "Lease should start at or after current time")
			assert.Greater(t, runtime.LeaseExpiry, runtime.LeaseStartedAt, "Lease expiry should be after start time")
			assert.GreaterOrEqual(t, runtime.LeaseExpiry-runtime.LeaseStartedAt, tt.expectedMinDuration, "Lease duration should match policy")

			if tt.checkHeartbeat {
				assert.Greater(t, runtime.HeartbeatExpiry, int64(0), "Heartbeat expiry should be set")
			}
		})
	}
}

func TestLeaseRuntimeCalculator_BasicLease(t *testing.T) {
	clock := NewClock()
	calculator := NewLeaseRuntimeCalculator(clock)

	policy := &commonpb.LeasePolicy{
		BaseLease: durationpb.New(45 * time.Second),
	}

	runtime := calculator.CalculateLeaseRuntime(policy)

	// Verify all fields are populated
	assert.NotZero(t, runtime.LeaseStartedAt)
	assert.NotZero(t, runtime.LeaseExpiry)
	assert.NotZero(t, runtime.LastHeartbeatAt)

	// Verify lease duration is approximately correct (within 1 second tolerance)
	actualDuration := runtime.LeaseExpiry - runtime.LeaseStartedAt
	expectedDuration := int64(45000) // 45 seconds in ms
	assert.InDelta(t, expectedDuration, actualDuration, 1000, "Lease duration should be approximately 45 seconds")
}
