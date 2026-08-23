package validator

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/adrien19/chronoqueue/api/common/v1"
	messagepb "github.com/adrien19/chronoqueue/api/message/v1"
	queuepb "github.com/adrien19/chronoqueue/api/queue/v1"
	schemapb "github.com/adrien19/chronoqueue/api/schema/v1"
	"github.com/adrien19/chronoqueue/internal/util"
)

type LeasePolicyValidator struct {
	queueMeta *queuepb.QueueMetadata
}

func NewLeasePolicyValidator(queueMeta *queuepb.QueueMetadata) Validator {
	return &LeasePolicyValidator{queueMeta: queueMeta}
}

func (v *LeasePolicyValidator) Validate(_ context.Context, msg *messagepb.Message) *ValidationResult {
	result := NewValidationResult()
	if msg.GetMetadata() == nil {
		return result
	}

	metadata := msg.GetMetadata()
	requestedPolicy := metadata.GetLeasePolicy()
	var effective *commonpb.LeasePolicy
	if v.queueMeta.GetLeasePolicy() != nil {
		effective = proto.Clone(v.queueMeta.GetLeasePolicy()).(*commonpb.LeasePolicy)
	} else {
		effective = &commonpb.LeasePolicy{}
	}

	if requestedPolicy != nil {
		if requestedPolicy.GetBaseLease() != nil {
			effective.BaseLease = requestedPolicy.GetBaseLease()
		}
		if requestedPolicy.GetMaxExtension() != nil {
			effective.MaxExtension = requestedPolicy.GetMaxExtension()
		}
		if requestedPolicy.GetHeartbeatTimeout() != nil {
			effective.HeartbeatTimeout = requestedPolicy.GetHeartbeatTimeout()
		}
		if requestedPolicy.GetExtendStep() != nil {
			effective.ExtendStep = requestedPolicy.GetExtendStep()
		}
		if requestedPolicy.GetMaxRenewals() != 0 {
			effective.MaxRenewals = requestedPolicy.GetMaxRenewals()
		}
	}

	if legacyLease := metadata.GetLeaseDuration(); legacyLease != nil {
		if requestedPolicy.GetBaseLease() != nil && requestedPolicy.GetBaseLease().AsDuration() != legacyLease.AsDuration() {
			result.AddError(NewValidationError("metadata.lease_policy.base_lease", schemapb.ErrorCode_INVALID_FORMAT, "lease_duration and lease_policy.base_lease must match when both are set"))
			return result
		}
		effective.BaseLease = legacyLease
	}
	if effective.GetBaseLease() == nil {
		if queueLease := v.queueMeta.GetLeaseDuration(); queueLease != nil {
			effective.BaseLease = queueLease
		} else {
			effective.BaseLease = durationpb.New(DefaultLeaseDuration)
		}
	}

	if err := util.ValidateLeasePolicy(effective); err != nil {
		result.AddError(NewValidationError("metadata.lease_policy", schemapb.ErrorCode_VALUE_OUT_OF_RANGE, err.Error()))
		return result
	}
	metadata.LeasePolicy = effective
	metadata.LeaseDuration = effective.GetBaseLease()
	return result
}
