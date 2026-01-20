package common

import (
	"fmt"

	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
)

// StateValidator provides common validation logic for message states
type StateValidator struct{}

// NewStateValidator creates a new StateValidator instance
func NewStateValidator() *StateValidator {
	return &StateValidator{}
}

// ValidateStateTransition checks if a state transition is valid
func (v *StateValidator) ValidateStateTransition(from, to messagepb.Message_Metadata_State) error {
	// Valid state transitions:
	// PENDING -> RUNNING
	// RUNNING -> PENDING (reclaim)
	// RUNNING -> COMPLETED (acknowledge success)
	// RUNNING -> ERROR (acknowledge failure with retries exhausted)
	// INVISIBLE -> PENDING (scheduled time arrives)
	// PENDING -> ERROR (max attempts exceeded during claim)

	validTransitions := map[messagepb.Message_Metadata_State][]messagepb.Message_Metadata_State{
		messagepb.Message_Metadata_PENDING: {
			messagepb.Message_Metadata_RUNNING,
			messagepb.Message_Metadata_ERRORED,
		},
		messagepb.Message_Metadata_RUNNING: {
			messagepb.Message_Metadata_PENDING,
			messagepb.Message_Metadata_COMPLETED,
			messagepb.Message_Metadata_ERRORED,
		},
		messagepb.Message_Metadata_INVISIBLE: {
			messagepb.Message_Metadata_PENDING,
		},
		messagepb.Message_Metadata_ERRORED: {
			messagepb.Message_Metadata_PENDING, // Requeue from DLQ
		},
	}

	allowed, exists := validTransitions[from]
	if !exists {
		return fmt.Errorf("invalid source state: %v", from)
	}

	for _, allowedState := range allowed {
		if to == allowedState {
			return nil
		}
	}

	return fmt.Errorf("invalid state transition: %v -> %v", from, to)
}
