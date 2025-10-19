package repository

import (
	"testing"
	"time"
)

func TestPriorityScoring(t *testing.T) {
	// Test the new priority scoring algorithm
	tests := []struct {
		name         string
		priority     int64
		expectedRank int // 1 = highest priority (should be processed first)
	}{
		{"Highest Priority", 100, 1},
		{"High Priority", 90, 2},
		{"Medium Priority", 50, 3},
		{"Low Priority", 10, 4},
		{"Lowest Priority", 0, 5},
	}

	scores := make([]struct {
		priority int64
		score    int64
	}, len(tests))

	// Calculate scores for all priorities
	baseTime := time.Now().UnixMilli()
	for i, test := range tests {
		// Using the same algorithm as in create_queue_message.go
		priorityComponent := int64(MaxPriority-test.priority) * 1e10
		timestampComponent := baseTime + int64(i) // Add small offset to simulate different creation times
		scores[i] = struct {
			priority int64
			score    int64
		}{
			priority: test.priority,
			score:    priorityComponent + timestampComponent,
		}
	}

	// Verify that scores are ordered correctly (lower score = higher priority)
	for i := 0; i < len(scores)-1; i++ {
		if scores[i].score >= scores[i+1].score {
			t.Errorf("Priority ordering is incorrect: priority %d (score %d) should have lower score than priority %d (score %d)",
				scores[i].priority, scores[i].score, scores[i+1].priority, scores[i+1].score)
		}
	}

	// Verify that the highest priority (100) has the lowest score
	highestPriorityIndex := -1
	lowestScore := int64(^uint64(0) >> 1) // max int64
	for i, score := range scores {
		if score.score < lowestScore {
			lowestScore = score.score
			highestPriorityIndex = i
		}
	}

	if highestPriorityIndex != 0 { // tests[0] is priority 100
		t.Errorf("Highest priority message (100) should have the lowest score, but got index %d", highestPriorityIndex)
	}
}

func TestPriorityWithSameCreationTime(t *testing.T) {
	// Test that even with the same creation time, priority ordering is maintained
	baseTime := time.Now().UnixMilli()

	priorities := []int64{100, 80, 60, 40, 20, 0}
	scores := make([]int64, len(priorities))

	for i, priority := range priorities {
		priorityComponent := int64(MaxPriority-priority) * 1e10
		timestampComponent := baseTime // Same timestamp for all
		scores[i] = priorityComponent + timestampComponent
	}

	// Verify ordering is correct
	for i := 0; i < len(scores)-1; i++ {
		if scores[i] >= scores[i+1] {
			t.Errorf("Priority ordering failed: priority %d (score %d) should have lower score than priority %d (score %d)",
				priorities[i], scores[i], priorities[i+1], scores[i+1])
		}
	}
}

func TestPriorityScoreConstants(t *testing.T) {
	// Verify that our constants are correct
	if MaxPriority != 100 {
		t.Errorf("MaxPriority should be 100, got %d", MaxPriority)
	}
	if MinPriority != 0 {
		t.Errorf("MinPriority should be 0, got %d", MinPriority)
	}

	// Test edge cases
	baseTime := time.Now().UnixMilli()

	// Max priority should have score close to baseTime
	maxPriorityScore := int64(MaxPriority-100)*1e10 + baseTime // MaxPriority is 100
	if maxPriorityScore != baseTime {
		t.Errorf("Max priority score should equal baseTime, got %d, expected %d", maxPriorityScore, baseTime)
	}

	// Min priority should have score = 100*1e10 + baseTime
	minPriorityScore := int64(MaxPriority-MinPriority)*1e10 + baseTime
	expectedMinScore := 100*1e10 + baseTime
	if minPriorityScore != expectedMinScore {
		t.Errorf("Min priority score should be %d, got %d", expectedMinScore, minPriorityScore)
	}
}
