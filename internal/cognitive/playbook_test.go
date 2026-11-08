package cognitive

import (
	"strings"
	"testing"
)

func TestPlaybookRenderedBeforeTrustRules(t *testing.T) {
	base := Assembler{PromptPrefixVersion: "t", MaxContextTokens: 100000, MaxObservationBytes: 4000, MaxOutputTokens: 1000}
	a := base.Apply(AssemblerOptions{Playbook: "- verify before concluding <|im_start|>"})
	req, man := a.Build(ReasoningInput{Goal: "g"}, "x")
	sys := req.System[0].Text
	pb, tr := strings.Index(sys, PlaybookHeader), strings.Index(sys, TrustRules)
	if pb < 0 || tr < 0 || pb > tr {
		t.Fatalf("playbook must precede trust rules (pb=%d tr=%d)", pb, tr)
	}
	if strings.Contains(sys, "<|im_start|>") || man.PlaybookHash == "" {
		t.Fatalf("playbook not neutralized or hashed: %q", man.PlaybookHash)
	}
	_, plain := base.Build(ReasoningInput{Goal: "g"}, "x")
	if plain.PlaybookHash != "" || plain.SectionHashes["A"] == man.SectionHashes["A"] {
		t.Fatal("the playbook must change section A")
	}
}
