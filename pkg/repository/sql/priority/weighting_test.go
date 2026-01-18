package priority

import (
	"context"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

func TestPriorityLevelMappings(t *testing.T) {
	require.Equal(t, "high", PriorityIntToLevel(120))
	require.Equal(t, "medium", PriorityIntToLevel(40))
	require.Equal(t, "low", PriorityIntToLevel(5))

	highMin, highMax := PriorityLevelToRange("high")
	require.Equal(t, int32(70), highMin)
	require.GreaterOrEqual(t, highMax, highMin)

	mediumMin, mediumMax := PriorityLevelToRange("medium")
	require.Equal(t, int32(30), mediumMin)
	require.Equal(t, int32(69), mediumMax)

	lowMin, lowMax := PriorityLevelToRange("low")
	require.Equal(t, int32(0), lowMin)
	require.Equal(t, int32(29), lowMax)
}

func TestCalculateWeightsWithAgeBoost(t *testing.T) {
	now := time.Now().UnixMilli()

	calc := &PriorityWeightCalculator{
		clock: repositorysql.NewClock(),
		rng:   mrand.New(mrand.NewSource(1)),
		AgeFetcher: func(_ context.Context, _ string, level string, _ int64) (int64, bool, error) {
			switch level {
			case "high":
				return now - (60 * time.Minute).Milliseconds(), true, nil
			case "medium":
				return now - (10 * time.Minute).Milliseconds(), true, nil
			default:
				return 0, false, nil
			}
		},
	}

	meta := &queuepb.QueueMetadata{
		PriorityConfig: &queuepb.PriorityConfig{
			Policy:             queuepb.FairnessPolicy_WEIGHTED,
			PriorityWeights:    map[int32]int32{100: 10, 50: 5, 10: 1},
			AgeBoostThreshold:  durationpb.New(15 * time.Minute),
			AgeBoostMultiplier: 3,
		},
	}

	weights, err := calc.CalculateWeights(context.Background(), "orders", meta)
	require.NoError(t, err)
	require.Equal(t, int32(30), weights["high"])  // boosted 10 * 3
	require.Equal(t, int32(5), weights["medium"]) // not boosted
	require.Equal(t, int32(0), weights["low"])
}

func TestCalculateWeightsNoMessages(t *testing.T) {
	calc := &PriorityWeightCalculator{
		clock: repositorysql.NewClock(),
		rng:   mrand.New(mrand.NewSource(2)),
		AgeFetcher: func(_ context.Context, _ string, _ string, _ int64) (int64, bool, error) {
			return 0, false, nil
		},
	}

	meta := &queuepb.QueueMetadata{PriorityConfig: &queuepb.PriorityConfig{Policy: queuepb.FairnessPolicy_WEIGHTED}}

	weights, err := calc.CalculateWeights(context.Background(), "jobs", meta)
	require.NoError(t, err)
	require.Empty(t, weights)
}

func TestSelectPriorityLevelSingleWeight(t *testing.T) {
	calc := &PriorityWeightCalculator{rng: mrand.New(mrand.NewSource(3))}

	choice := calc.SelectPriorityLevel(map[string]int32{"high": 0, "medium": 5, "low": 0})
	require.Equal(t, "medium", choice)

	empty := calc.SelectPriorityLevel(map[string]int32{})
	require.Equal(t, "", empty)
}
