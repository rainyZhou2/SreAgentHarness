// Package untrusted puts a boundary around content the model must treat as
// data: tool results (logs, tickets, MCP replies, web pages), operator
// evidence and compacted history. Every source is handled the same way,
// whatever its trust tier, because any of them can carry text written by
// someone other than the operator.
//
// The boundary id is derived from the content itself, so it is stable for
// replay and cannot be forged by the content: closing the block early would
// require knowing the hash of a text that contains that hash.
package untrusted

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"go-sre-harness/internal/memory"
)

// MaxFlags bounds the suspicious spans reported for one block.
const MaxFlags = 3

// controlTokens are chat-template and boundary tokens that content must not
// be able to inject verbatim.
var controlTokens = strings.NewReplacer(
	"<|im_start|>", "<|im_start​|>",
	"<|im_end|>", "<|im_end​|>",
	"<|system|>", "<|system​|>",
	"<|user|>", "<|user​|>",
	"<|assistant|>", "<|assistant​|>",
	"<|endoftext|>", "<|endoftext​|>",
	"[INST]", "[INST​]",
	"[/INST]", "[/INST​]",
	"<<<DATA", "<<​<DATA",
	"<<<END DATA", "<<​<END DATA",
)

// Block is content prepared for the prompt.
type Block struct {
	ID string
	// Flags are instruction-like spans found in the content (clipped).
	Flags []string
	Text  string
}

// Scan returns instruction-like spans of s, clipped for display.
func Scan(s string) []string {
	m := memory.PoisonMatches(s, MaxFlags)
	for i := range m {
		m[i] = clip(m[i], 60)
	}
	return m
}

// Neutralize defuses chat-template control tokens and boundary markers.
func Neutralize(s string) string { return controlTokens.Replace(s) }

// Wrap neutralizes body, flags instruction-like spans and encloses it in a
// boundary. attrs describe the source (e.g. "source=tool:logs.search
// trust=T4") and are written into the opening marker.
func Wrap(attrs, body string) Block {
	clean := Neutralize(body)
	sum := sha256.Sum256([]byte(clean))
	b := Block{ID: hex.EncodeToString(sum[:6]), Flags: Scan(clean)}
	var sb strings.Builder
	sb.Grow(len(clean) + 96)
	fmt.Fprintf(&sb, "<<<DATA id=%s", b.ID)
	if attrs = strings.TrimSpace(attrs); attrs != "" {
		sb.WriteString(" " + attrs)
	}
	if len(b.Flags) > 0 {
		sb.WriteString(" injection_suspected=true")
	}
	sb.WriteString(">>>\n")
	sb.WriteString(clean)
	if !strings.HasSuffix(clean, "\n") {
		sb.WriteByte('\n')
	}
	fmt.Fprintf(&sb, "<<<END DATA id=%s>>>", b.ID)
	b.Text = sb.String()
	return b
}

// FlagNote renders the flags for a header line ("" when there are none).
func FlagNote(flags []string) string {
	if len(flags) == 0 {
		return ""
	}
	q := make([]string, len(flags))
	for i, f := range flags {
		q[i] = fmt.Sprintf("%q", f)
	}
	return "injection_suspected=[" + strings.Join(q, ",") + "]"
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// Unwrap returns the content of a block at the start of s and the text after
// it (a truncation marker, for instance). ok is false when s does not start
// with a block.
func Unwrap(s string) (body, rest string, ok bool) {
	if !strings.HasPrefix(s, "<<<DATA id=") {
		return "", s, false
	}
	nl := strings.IndexByte(s, '\n')
	if nl < 0 {
		return "", s, false
	}
	id := s[len("<<<DATA id="):nl]
	if i := strings.IndexAny(id, " >"); i >= 0 {
		id = id[:i]
	}
	end := "<<<END DATA id=" + id + ">>>"
	j := strings.LastIndex(s, end)
	if j < nl {
		return "", s, false
	}
	return strings.TrimSuffix(s[nl+1:j], "\n"), s[j+len(end):], true
}
