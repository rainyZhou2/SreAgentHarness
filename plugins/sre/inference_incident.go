package sre

import "go-sre-harness/internal/work"

// InferenceIncidentTemplates adds an opt-in parallel TTFT incident graph. It
// leaves the v1 route intact. The caller must supply the service's machine
// recovery goal (for example TTFTSuccess); inference.diagnose and the existing
// Action adapters provide observations and controlled proposals, not success.
func InferenceIncidentTemplates() []work.Template {
	mitigation := InferenceLatencyTemplate()
	mitigation.ID, mitigation.Version = "inference-mitigation-v2", 2
	mitigation.RequireMachineSuccess = true
	// Postmortem drafting belongs after recovery, not on the mitigation path.
	mitigation.Stages = mitigation.Stages[:3]
	rca := work.Template{ID: "inference-rca-v2", Version: 2, Kind: work.KindIncident, Queue: work.QueueBackground,
		Stages: []work.Stage{
			{ID: "investigate", Type: work.StageRun, Phase: "investigate", Run: &work.RunSpec{Goal: "Investigate the root cause of the inference TTFT incident on ${subjects}. ${goal.description}\nUse read-only inference.diagnose, GPU, metrics and change evidence. Record hypotheses and causal evidence; distinguish trigger, root cause and contributing factors. Recovery and a successful mitigation do not prove a root cause. State unknown or inconclusive when evidence is insufficient. Do not perform production writes."}},
			{ID: "review", Type: work.StageConfirm, Phase: "review", Confirm: &work.ConfirmSpec{Prompt: "Review the RCA evidence and its current Decision verdict. Confirm that the investigation is complete, including an explicitly inconclusive result when appropriate. This does not verify service recovery."}},
		}}
	parent := work.Template{ID: "inference-incident-v2", Version: 2, Kind: work.KindIncident, Queue: work.QueueIncident, RequireMachineSuccess: true,
		Graph: &work.GraphSpec{Children: []work.ChildSpec{
			{Key: "mitigation", Template: mitigation.ID, Required: true, InheritSuccess: true, Intent: "Restore inference service: ${goal.intent}", Description: "${goal.description}", Constraints: work.Constraints{ProductionWrites: work.WritesApprovalRequired}},
			{Key: "rca", Template: rca.ID, Intent: "Investigate inference root cause: ${goal.intent}", Description: "${goal.description}", Constraints: work.Constraints{ProductionWrites: work.WritesForbidden}, Queue: work.QueueBackground},
		}},
		Stages: []work.Stage{
			{ID: "coordinate", Type: work.StageChildren, Phase: "mitigate", OnFailure: work.OnFailBlock},
			{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: work.OnFailBlock, OnTimeout: work.OnFailBlock},
		}}
	return []work.Template{parent, mitigation, rca}
}
