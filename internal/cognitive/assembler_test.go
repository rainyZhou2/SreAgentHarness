package cognitive

import (
	"strings"
	"testing"
)

func TestCustomInstructionsKeepTrustRules(t *testing.T) {
	a := DefaultAssembler()
	a.Instructions = "You triage incidents for the payments team."
	req, man := a.Build(ReasoningInput{Goal: "x"}, "m")
	sys := req.System[0].Text
	if !strings.HasPrefix(sys, "You triage incidents") || !strings.Contains(sys, TrustRules) {
		t.Fatalf("system prompt = %q", sys)
	}
	def, defMan := DefaultAssembler().Build(ReasoningInput{Goal: "x"}, "m")
	if !strings.Contains(def.System[0].Text, TrustRules) || defMan.SectionHashes["A"] == man.SectionHashes["A"] {
		t.Fatal("default prompt must carry trust rules and hash differently from custom instructions")
	}
	opts := a.Apply(AssemblerOptions{MaxOutputTokens: 77, Effort: "low"})
	if opts.MaxOutputTokens != 77 || opts.Effort != "low" || opts.Instructions != a.Instructions {
		t.Fatalf("apply: %+v", opts)
	}
}

func TestSkillsRenderBeforeTrustRules(t *testing.T) {
	skills := []Skill{{Name: "zeta", Description: "last", Instructions: "Z steps"}, {Name: "db-triage", Description: "db", Instructions: "Search runbooks first."}}
	req, man := DefaultAssembler().Build(ReasoningInput{Goal: "x", Skills: skills}, "m")
	sys := req.System[0].Text
	iSkill, iZeta, iTrust := strings.Index(sys, "Search runbooks first."), strings.Index(sys, "Z steps"), strings.Index(sys, TrustRules)
	if iSkill < 0 || iZeta < iSkill || iTrust < iZeta {
		t.Fatalf("skills must be sorted and precede the trust rules:\n%s", sys)
	}
	if len(man.Skills) != 2 || !strings.HasPrefix(man.Skills[0], "db-triage@") {
		t.Fatalf("manifest skills = %v", man.Skills)
	}
	_, none := DefaultAssembler().Build(ReasoningInput{Goal: "x"}, "m")
	if len(none.Skills) != 0 || none.SectionHashes["A"] == man.SectionHashes["A"] {
		t.Fatal("skills must change the section A hash and be absent when none are loaded")
	}
}

func TestSelectSkillsManualNeedsMention(t *testing.T) {
	all := []Skill{{Name: "always"}, {Name: "db", Manual: true}, {Name: "db-triage", Manual: true}}
	names := func(ss []Skill) string {
		var out []string
		for _, s := range ss {
			out = append(out, s.Name)
		}
		return strings.Join(out, ",")
	}
	cases := map[string][]string{
		"always":              {"checkout is slow"},
		"always,db-triage":    {"please /db-triage this"},
		"always,db":           {"earlier: /db.", "now something else"},
		"always,db,db-triage": {"/db /db-triage"},
	}
	for want, texts := range cases {
		if got := names(SelectSkills(all, texts...)); got != want {
			t.Errorf("%q: got %s, want %s", texts, got, want)
		}
	}
	if got := names(SelectSkills(all, "path/db-triage and x/db")); got != "always" {
		t.Errorf("a /name inside a word must not match: %s", got)
	}
}
