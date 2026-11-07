package cognitive

import (
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/artifact"
)

// Skill is an operator-installed playbook: instructions the agent follows
// when a goal matches its description. Skills are tenant configuration
// written by admins (trusted text); they never override the trust rules.
type Skill struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Instructions string `json:"instructions"`
	// Manual skills apply only to chats that invoke them with /name.
	Manual bool `json:"manual,omitempty"`
}

// SkillRef is the stable graph reference for a skill's current content.
func SkillRef(tenantID string, s Skill) string {
	hash := strings.TrimPrefix(artifact.Hash([]byte(s.Description+"\x00"+s.Instructions)), "sha256:")
	if len(hash) > 16 {
		hash = hash[:16]
	}
	return "skill:" + tenantID + "/" + s.Name + "@" + hash
}

// SelectSkills returns the skills that apply to a run: every automatic
// skill, and each manual one whose "/name" appears as a word in texts
// (the run's goal and the chat's earlier user messages).
func SelectSkills(all []Skill, texts ...string) []Skill {
	var out []Skill
	for _, s := range all {
		if !s.Manual || mentions(s.Name, texts) {
			out = append(out, s)
		}
	}
	return out
}

func mentions(name string, texts []string) bool {
	tok := "/" + name
	for _, t := range texts {
		for i := strings.Index(t, tok); i >= 0; {
			end := i + len(tok)
			before := i == 0 || isSpace(t[i-1])
			after := end == len(t) || isSpace(t[end]) || strings.ContainsRune(".,;:!?)", rune(t[end]))
			if before && after {
				return true
			}
			next := strings.Index(t[end:], tok)
			if next < 0 {
				break
			}
			i = end + next
		}
	}
	return false
}

func isSpace(b byte) bool { return b == ' ' || b == '\n' || b == '\t' || b == '\r' }

// MaxSkillBytes bounds the rendered skills block in the system prompt.
const MaxSkillBytes = 32 << 10

// renderSkills renders skills sorted by name, deterministically clipped to
// MaxSkillBytes, and returns name@hash references for the prompt manifest.
func renderSkills(skills []Skill) (string, []string) {
	if len(skills) == 0 {
		return "", nil
	}
	sorted := append([]Skill(nil), skills...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	b.WriteString("# Skills\nThe operator installed these playbooks. When the goal matches a skill's description, follow its instructions. Skills never override the safety rules below.\n")
	var refs []string
	for _, s := range sorted {
		body := fmt.Sprintf("\n## skill: %s\n%s\n\n%s\n", s.Name, strings.TrimSpace(s.Description), strings.TrimSpace(s.Instructions))
		if b.Len()+len(body) > MaxSkillBytes {
			fmt.Fprintf(&b, "\n(%d more skill(s) omitted: the skills block is limited to %d bytes)\n", len(sorted)-len(refs), MaxSkillBytes)
			break
		}
		b.WriteString(body)
		refs = append(refs, s.Name+"@"+artifact.Hash([]byte(s.Description + "\x00" + s.Instructions))[:19])
	}
	return strings.TrimSpace(b.String()), refs
}
