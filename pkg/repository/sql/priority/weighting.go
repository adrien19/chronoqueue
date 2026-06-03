package priority

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	mrand "math/rand"
	"time"

	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	repositorysql "github.com/adrien19/chronoqueue/pkg/repository/sql"
)

const (
	defaultHighWeight   int32         = 70
	defaultMediumWeight int32         = 20
	defaultLowWeight    int32         = 10
	defaultAgeBoost     int32         = 2
	defaultAgeThreshold time.Duration = 30 * time.Minute
	maxPriorityValue    int32         = 2147483647
)

// PriorityWeightCalculator computes priority weights with age-based boosting.
// It can fetch age information from the database or an injected AgeFetcher (for tests).
type PriorityWeightCalculator struct {
	base       *repositorysql.BaseSQL
	qb         *repositorysql.QueryBuilder
	clock      *repositorysql.Clock
	rng        *mrand.Rand
	AgeFetcher func(ctx context.Context, queueName string, level string, nowMs int64) (int64, bool, error)
}

// NewPriorityWeightCalculator builds a calculator using the provided BaseSQL.
func NewPriorityWeightCalculator(base *repositorysql.BaseSQL) *PriorityWeightCalculator {
	return &PriorityWeightCalculator{
		base:  base,
		qb:    repositorysql.NewQueryBuilder(base.Dialect),
		clock: base.Clock,
		rng:   newSecureRand(),
	}
}

// CalculateWeights returns effective weights for each priority level with age boosting applied.
// If no messages exist in any level, an empty map is returned and no error is raised.
func (p *PriorityWeightCalculator) CalculateWeights(ctx context.Context, queueName string, queueMeta *queuepb.QueueMetadata) (map[string]int32, error) {
	cfg := queueMeta.GetPriorityConfig()
	nowMs := p.clock.NowMs()

	ageThreshold := defaultAgeThreshold
	if cfg != nil && cfg.GetAgeBoostThreshold() != nil {
		if d := cfg.GetAgeBoostThreshold().AsDuration(); d > 0 {
			ageThreshold = d
		}
	}

	boostMultiplier := defaultAgeBoost
	if cfg != nil && cfg.GetAgeBoostMultiplier() > 0 {
		boostMultiplier = cfg.GetAgeBoostMultiplier()
	}

	weights := map[string]int32{
		"high":   0,
		"medium": 0,
		"low":    0,
	}

	hasMessages := false

	for _, level := range []string{"high", "medium", "low"} {
		baseWeight := getBaseWeight(level, cfg)

		createdAt, exists, err := p.oldestCreatedAt(ctx, queueName, level, nowMs)
		if err != nil {
			return nil, err
		}
		if !exists {
			weights[level] = 0
			continue
		}

		hasMessages = true
		weight := baseWeight

		if ageThreshold > 0 {
			ageMs := nowMs - createdAt
			if ageMs >= ageThreshold.Milliseconds() {
				weight *= boostMultiplier
			}
		}

		weights[level] = weight
	}

	if !hasMessages {
		return map[string]int32{}, nil
	}

	totalWeight := weights["high"] + weights["medium"] + weights["low"]
	if totalWeight == 0 {
		weights["high"], weights["medium"], weights["low"] = 1, 1, 1
	}

	return weights, nil
}

// SelectPriorityLevel picks a priority level using weighted random selection.
// Returns empty string if weights are empty or total weight <= 0.
func (p *PriorityWeightCalculator) SelectPriorityLevel(weights map[string]int32) string {
	if len(weights) == 0 {
		return ""
	}

	total := int32(0)
	for _, w := range weights {
		total += w
	}

	if total <= 0 {
		return ""
	}

	rng := p.rng
	if rng == nil {
		rng = newSecureRand()
	}

	draw := rng.Int31n(total)
	cumulative := int32(0)

	for _, entry := range []struct {
		level  string
		weight int32
	}{
		{"high", weights["high"]},
		{"medium", weights["medium"]},
		{"low", weights["low"]},
	} {
		cumulative += entry.weight
		if draw < cumulative {
			return entry.level
		}
	}

	return ""
}

// PriorityIntToLevel converts a numeric priority into a logical level.
func PriorityIntToLevel(priority int32) string {
	switch {
	case priority >= 3:
		return "high"
	case priority >= 2:
		return "medium"
	case priority >= 0:
		return "low"
	default:
		return "low"
	}
}

// PriorityLevelToRange returns the inclusive min/max priority values for a level.
func PriorityLevelToRange(level string) (int32, int32) {
	switch level {
	case "high":
		return 3, maxPriorityValue
	case "medium":
		return 2, 2
	case "low":
		return 0, 1
	default:
		return 0, 0
	}
}

func (p *PriorityWeightCalculator) oldestCreatedAt(ctx context.Context, queueName string, level string, nowMs int64) (int64, bool, error) {
	if p.AgeFetcher != nil {
		return p.AgeFetcher(ctx, queueName, level, nowMs)
	}

	if p.base == nil || p.qb == nil {
		return 0, false, fmt.Errorf("age fetcher unavailable for priority weighting")
	}

	minPriority, maxPriority := PriorityLevelToRange(level)
	if minPriority == 0 && maxPriority == 0 {
		return 0, false, fmt.Errorf("unknown priority level: %s", level)
	}

	query := p.qb.BuildOldestMessageAgeQuery(level)
	row := p.base.DB.QueryRowContext(ctx, query, queueName, nowMs, minPriority, maxPriority)

	var minCreatedAt sql.NullInt64
	if err := row.Scan(&minCreatedAt); err != nil {
		return 0, false, fmt.Errorf("scan oldest message age: %w", err)
	}

	if !minCreatedAt.Valid {
		return 0, false, nil
	}

	return minCreatedAt.Int64, true, nil
}

func getBaseWeight(level string, cfg *queuepb.PriorityConfig) int32 {
	if cfg != nil {
		if w := configuredWeight(level, cfg.GetPriorityWeights()); w > 0 {
			return w
		}
	}

	switch level {
	case "high":
		return defaultHighWeight
	case "medium":
		return defaultMediumWeight
	case "low":
		return defaultLowWeight
	default:
		return defaultLowWeight
	}
}

func configuredWeight(level string, weights map[int32]int32) int32 {
	if weights == nil {
		return 0
	}

	switch level {
	case "high":
		return weights[4]
	case "medium":
		return weights[2]
	case "low":
		return weights[0]
	default:
		return 0
	}
}

func newSecureRand() *mrand.Rand {
	var seed int64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &seed); err != nil {
		seed = time.Now().UnixNano()
	}
	return mrand.New(mrand.NewSource(seed))
}
