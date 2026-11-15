package kernel

import (
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

func newToolCallInfo(lease domain.Lease) tools.CallInfo {
	info := tools.CallInfo{
		TenantID: lease.TenantID, RunID: lease.RunID, WorkID: lease.WorkID, Subject: lease.Subject,
		Environment: lease.Environment, StepID: lease.StepID, AttemptID: lease.AttemptID,
		FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
		InputRef: lease.InputRef, Deadline: lease.Deadline,
	}
	if lease.Intent != nil {
		info.ActionID = lease.Intent.ID
		info.ActionInputHash = lease.Intent.InputHash
	}
	return info
}
