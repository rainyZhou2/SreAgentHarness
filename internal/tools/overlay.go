package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/flow"
)

// BuiltinPrefix is the namespace of framework tools. They never reach an
// external system (they read the run's own artifacts), so profile
// allowlists permit them implicitly; tenant toggles can still hide them.
const BuiltinPrefix = "harness."

// IsBuiltin reports whether name is a framework tool.
func IsBuiltin(name string) bool { return strings.HasPrefix(name, BuiltinPrefix) }

// Lifecycle declares the post-action loop of a side-effecting tool.
type Lifecycle struct {
	// Verify is a (normally read-only) tool called after the action.
	Verify *FollowUp `json:"verify,omitempty"`
	// Expect is a flow expression over {args, action, output} that must be
	// true for the action to count as verified. Empty means "the verify tool
	// returned without error".
	Expect string `json:"expect,omitempty"`
	// Attempts bounds the verification polls (default 3, max 20).
	Attempts int `json:"attempts,omitempty"`
	// Interval between polls (default 10s).
	Interval Duration `json:"interval,omitempty"`
	// Delay before the first poll (default 0).
	Delay Duration `json:"delay,omitempty"`
	// Compensate is proposed (always behind human approval) when the
	// expectation is refuted.
	Compensate *FollowUp `json:"compensate,omitempty"`
}

// FollowUp names a tool and its arguments. Args values may contain ${...}
// expressions over {args, action}; nil Args reuses the original arguments.
type FollowUp struct {
	Tool string         `json:"tool"`
	Args map[string]any `json:"args,omitempty"`
}

// TimeArgs names a tool's time-range arguments.
type TimeArgs struct {
	Start string `json:"start,omitempty"`
	End   string `json:"end,omitempty"`
	// Format is rfc3339 (default), unix or unix_ms.
	Format string `json:"format,omitempty"`
	// Lookback is used when the run has no incident window (default 1h).
	Lookback Duration `json:"lookback,omitempty"`
}

// Overlay is operator metadata attached to a tool by name, whatever its
// source (native, MCP, remote, tenant extension).
type Overlay struct {
	Labels         map[string]string `json:"labels,omitempty"`
	Lifecycle      *Lifecycle        `json:"lifecycle,omitempty"`
	TimeArgs       *TimeArgs         `json:"time_args,omitempty"`
	MaxInlineBytes int               `json:"max_inline_bytes,omitempty"`
}

// Validate checks an overlay's expressions and bounds.
func (o Overlay) Validate(name string) error {
	if lc := o.Lifecycle; lc != nil {
		if lc.Verify == nil && lc.Compensate != nil {
			return fmt.Errorf("tool overlay %s: compensate requires verify", name)
		}
		if lc.Verify != nil && strings.TrimSpace(lc.Verify.Tool) == "" {
			return fmt.Errorf("tool overlay %s: verify.tool is required", name)
		}
		if lc.Compensate != nil && strings.TrimSpace(lc.Compensate.Tool) == "" {
			return fmt.Errorf("tool overlay %s: compensate.tool is required", name)
		}
		if lc.Verify != nil && lc.Verify.Tool == name {
			return fmt.Errorf("tool overlay %s: a tool cannot verify itself", name)
		}
		if lc.Expect != "" {
			if _, err := flow.CompileExpr(lc.Expect); err != nil {
				return fmt.Errorf("tool overlay %s: expect: %w", name, err)
			}
		}
		if lc.Attempts < 0 || lc.Attempts > 20 {
			return fmt.Errorf("tool overlay %s: attempts must be 0..20", name)
		}
		if lc.Interval < 0 || lc.Delay < 0 {
			return fmt.Errorf("tool overlay %s: interval and delay must be >= 0", name)
		}
		for _, f := range []*FollowUp{lc.Verify, lc.Compensate} {
			if f == nil {
				continue
			}
			if err := flow.ValidateInterpolations(anyMap(f.Args), nil); err != nil {
				return fmt.Errorf("tool overlay %s: %s args: %w", name, f.Tool, err)
			}
		}
	}
	if ta := o.TimeArgs; ta != nil {
		if ta.Start == "" && ta.End == "" {
			return fmt.Errorf("tool overlay %s: time_args needs start or end", name)
		}
		switch ta.Format {
		case "", "rfc3339", "unix", "unix_ms":
		default:
			return fmt.Errorf("tool overlay %s: time_args.format must be rfc3339, unix or unix_ms", name)
		}
	}
	return nil
}

func anyMap(m map[string]any) any {
	if m == nil {
		return nil
	}
	return map[string]any(m)
}

// apply copies overlay metadata onto a descriptor.
func (o Overlay) apply(d Descriptor) Descriptor {
	if len(o.Labels) > 0 {
		labels := make(map[string]string, len(d.Labels)+len(o.Labels))
		for k, v := range d.Labels {
			labels[k] = v
		}
		for k, v := range o.Labels {
			labels[k] = v
		}
		d.Labels = labels
	}
	if o.Lifecycle != nil {
		d.Lifecycle = o.Lifecycle
	}
	if o.TimeArgs != nil {
		d.TimeArgs = o.TimeArgs
	}
	if o.MaxInlineBytes != 0 {
		d.MaxInlineBytes = o.MaxInlineBytes
	}
	return d
}

type overlaid struct {
	Tool
	ov Overlay
}

func (t overlaid) Descriptor() Descriptor { return t.ov.apply(t.Tool.Descriptor()) }

func (t overlaid) Invoke(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
	return t.Tool.Invoke(ctx, key, args)
}

// Duration decodes "30s"/"1h" or a number of seconds.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\" or a number of seconds")
	}
	*d = Duration(time.Duration(n * float64(time.Second)))
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
