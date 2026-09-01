package background

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeDrainConfigUsesDefaultsAndOverrides(t *testing.T) {
	defaults := normalizeDrainConfig(DrainConfig{})
	require.Equal(t, defaultBackgroundBatchSize, defaults.BatchSize)
	require.Equal(t, defaultBackgroundMaxDrainBatches, defaults.MaxBatches)
	require.Equal(t, defaultBackgroundCycleDuration, defaults.MaxDuration)

	configured := normalizeDrainConfig(DrainConfig{BatchSize: 25, MaxBatches: 4, MaxDuration: time.Second})
	require.Equal(t, 25, configured.BatchSize)
	require.Equal(t, 4, configured.MaxBatches)
	require.Equal(t, time.Second, configured.MaxDuration)
}
