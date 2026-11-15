package security

type Dependencies struct {
	Recorder         DecisionRecorder
	FreshRevocations RevocationReader
	ApprovalVerifier ApprovalVerifier
	ToolsDecorated   bool
	CommitGuard      bool
}
