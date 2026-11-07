package domain

// ProfileInfo lists one registered profile version for the client's mode
// picker. It deliberately omits the spec body.
type ProfileInfo struct {
	ID       string `json:"id"`
	Version  int64  `json:"version"`
	Status   string `json:"status"`
	Strategy string `json:"strategy"`
	Default  bool   `json:"default"`
}
