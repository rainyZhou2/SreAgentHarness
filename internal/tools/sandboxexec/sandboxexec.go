// Package sandboxexec is the builtin tool that runs commands in the run's
// sandbox: an isolated workspace without network or credentials, for
// computing over data the agent already has (parsing logs, transforming
// tool output, quick scripts). It cannot reach production: changes there
// go through typed tools and approval.
package sandboxexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/sandbox"
	"go-sre-harness/internal/tools"
)

// Name is the tool name.
const Name = domain.SandboxExecToolName

// Tool runs commands through a sandbox backend.
type Tool struct {
	Backend sandbox.Backend
}

func (t Tool) Descriptor() tools.Descriptor {
	info := sandbox.Info{Network: "none"}
	if t.Backend != nil {
		info = t.Backend.Info()
	}
	desc := fmt.Sprintf("Run a command in this run's isolated sandbox (%s, image %s, network %s, no credentials). "+
		"The working directory %s persists across calls of this run. Use it to compute over data you already have "+
		"(parse, filter, aggregate, convert, quick scripts), never to change external systems. "+
		"Pass inputs as files, list the files to read back in outputs. A non-zero exit is reported, not an error.",
		info.Isolation, info.Image, info.Network, info.Workdir)
	return tools.Descriptor{Name: Name, Version: "1", Risk: domain.RiskR0, Trust: "T4", Source: "native", Description: desc,
		InputSchema: json.RawMessage(`{"type":"object","properties":{
"command":{"type":"string","description":"Shell command, run with sh -c."},
"argv":{"type":"array","items":{"type":"string"},"description":"Program and arguments, instead of command."},
"stdin":{"type":"string"},
"files":{"type":"object","additionalProperties":{"type":"string"},"description":"Workspace-relative path to text content, written before the command runs."},
"outputs":{"type":"array","items":{"type":"string"},"description":"Workspace-relative files to return after the command."},
"timeout_seconds":{"type":"integer","minimum":1,"maximum":600}}}`)}
}

func (t Tool) Invoke(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	if t.Backend == nil {
		return nil, errors.New("sandbox is not configured")
	}
	var in struct {
		Command string            `json:"command"`
		Argv    []string          `json:"argv"`
		Stdin   string            `json:"stdin"`
		Files   map[string]string `json:"files"`
		Outputs []string          `json:"outputs"`
		Timeout int               `json:"timeout_seconds"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}
	argv := in.Argv
	if strings.TrimSpace(in.Command) != "" {
		if len(argv) > 0 {
			return nil, errors.New("set command or argv, not both")
		}
		argv = []string{"sh", "-c", in.Command}
	}
	info, _ := tools.CallInfoFrom(ctx)
	if info.TenantID == "" || info.RunID == "" {
		return nil, errors.New("no run in call context")
	}
	res, err := t.Backend.Exec(ctx, sandbox.Request{TenantID: info.TenantID, RunID: info.RunID, Argv: argv,
		Stdin: in.Stdin, Files: in.Files, Outputs: in.Outputs, Timeout: time.Duration(in.Timeout) * time.Second})
	if err != nil {
		if errors.Is(err, sandbox.ErrInvalid) {
			// The agent can fix its request: a permanent, non-retryable error.
			return nil, err
		}
		return nil, domain.Retryable(err, 0)
	}
	return json.Marshal(res)
}
