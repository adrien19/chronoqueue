package sql

import messagepb "github.com/adrien19/chronoqueue/api/message/v1"

// ReclaimResult describes the committed outcome of reclaiming an expired message.
type ReclaimResult struct {
	State            messagepb.Message_Metadata_State
	LeaseExpired     bool
	HeartbeatExpired bool
	DLQTarget        string
}
