// Package memory implements long-term memory (§8–§11): redaction and
// poisoning checks on the write path, a hybrid retrieval pipeline (hard
// filters → full-text + vector + dependency-graph branches → RRF fusion →
// trust/conflict resolution) on the read path, and the cold-path
// consolidator that turns finished runs into experiences and candidate
// memories. Memory is evidence for the model, never instructions.
package memory

import (
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

type Memory struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id"`
	Scope          string     `json:"scope"` // organization | service | incident | agent
	ScopeID        string     `json:"scope_id"`
	Type           string     `json:"memory_type"` // semantic | episodic | procedural
	Status         string     `json:"status"`
	Version        int64      `json:"version"`
	Subject        string     `json:"subject"`
	Title          string     `json:"title"`
	Content        string     `json:"content"`
	ContentRef     string     `json:"content_ref"`
	ContentHash    string     `json:"content_hash"`
	SourceRefs     []string   `json:"source_refs"`
	EvidenceRefs   []string   `json:"evidence_refs"`
	Trust          string     `json:"trust"`
	Sensitivity    string     `json:"sensitivity"`
	Confidence     *float64   `json:"confidence,omitempty"`
	VerifiedBy     *string    `json:"verified_by,omitempty"`
	Environment    *string    `json:"environment,omitempty"`
	SupersededBy   *string    `json:"superseded_by,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	CreatedBy      string     `json:"created_by"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	Embedding      []float32  `json:"-"`
	EmbeddingModel string     `json:"-"`
}

// ---------- write-path safety (§8.4) ----------

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`),
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`\b(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}\b`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{30,}\b`),
	regexp.MustCompile(`\bxox[abpr]-[A-Za-z0-9-]{10,}\b`),
	regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key)\b\s*[:=]\s*["']?[^\s"',;]+`),
	regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s:/@]+:[^\s@/]+@`), // credentials in URLs
}

// Redact removes credentials before anything is persisted as memory. It
// returns the redacted text and how many secrets were removed.
func Redact(s string) (string, int) {
	n := 0
	for _, re := range secretPatterns {
		s = re.ReplaceAllStringFunc(s, func(m string) string {
			n++
			if i := strings.IndexAny(m, ":="); i > 0 && i < 40 && !strings.Contains(m, "://") && !strings.HasPrefix(m, "-----") {
				return m[:i+1] + "[REDACTED]"
			}
			return "[REDACTED]"
		})
	}
	return s, n
}

var poisonPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)ignore\s+(all\s+|any\s+)?(previous|prior|above|earlier)\s+(instructions|rules|messages)`),
	regexp.MustCompile(`(?i)disregard\s+(the\s+)?(system|previous|safety)`),
	regexp.MustCompile(`(?i)\byou\s+are\s+now\b`),
	regexp.MustCompile(`(?i)\b(system|developer)\s+prompt\b`),
	regexp.MustCompile(`(?i)(always|automatically)\s+approve`),
	regexp.MustCompile(`(?i)(skip|bypass|disable)\s+(the\s+)?(approval|policy|policy\s+gate|review)`),
	regexp.MustCompile(`(?i)do\s+not\s+(ask|wait)\s+for\s+approval`),
	regexp.MustCompile(`<\|im_start\|>|<\|system\|>|\[INST\]`),
}

// PoisonCheck flags instruction-like content. Such text is never stored as an
// ACTIVE memory: it is rejected or kept for human review (§8.4, §16.2).
func PoisonCheck(s string) (bool, string) {
	for _, re := range poisonPatterns {
		if m := re.FindString(s); m != "" {
			return true, m
		}
	}
	return false, ""
}

// ---------- embeddings ----------

// Embedder turns text into a fixed-size vector. Model identifies the
// embedding space; vectors of different models are never compared (§13.5).
type Embedder interface {
	Model() string
	Embed(text string) []float32
}

// HashingEmbedder is a dependency-free feature-hashing embedder over word
// unigrams and bigrams. It is deterministic, which keeps retrieval
// reproducible in tests and replay; production deployments can plug in a
// model-backed Embedder behind the same interface.
type HashingEmbedder struct{ Dims int }

func (h HashingEmbedder) Model() string { return "hash-uni-bigram-v1-" + itoa(h.dims()) }

func (h HashingEmbedder) dims() int {
	if h.Dims <= 0 {
		return 256
	}
	return h.Dims
}

func (h HashingEmbedder) Embed(text string) []float32 {
	d := h.dims()
	v := make([]float32, d)
	toks := Tokenize(text)
	add := func(s string, w float32) {
		f := fnv.New32a()
		_, _ = f.Write([]byte(s))
		x := f.Sum32()
		sign := float32(1)
		if x&1 == 1 {
			sign = -1
		}
		v[int(x>>1)%d] += sign * w
	}
	for i, t := range toks {
		add(t, 1)
		if i > 0 {
			add(toks[i-1]+"_"+t, 0.5)
		}
	}
	var norm float64
	for _, x := range v {
		norm += float64(x * x)
	}
	if norm > 0 {
		n := float32(math.Sqrt(norm))
		for i := range v {
			v[i] /= n
		}
	}
	return v
}

var stop = map[string]bool{"the": true, "a": true, "an": true, "of": true, "and": true, "or": true, "to": true,
	"in": true, "on": true, "for": true, "is": true, "was": true, "with": true, "by": true, "at": true, "be": true}

// Tokenize lower-cases and splits on non-alphanumerics, dropping stop words.
func Tokenize(s string) []string {
	f := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-'
	})
	out := f[:0]
	for _, t := range f {
		if len(t) > 1 && !stop[t] {
			out = append(out, t)
		}
	}
	return out
}

func Cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i] * b[i])
		na += float64(a[i] * a[i])
		nb += float64(b[i] * b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// ---------- fusion and conflicts ----------

// RRF fuses ranked lists with reciprocal rank fusion (k = 60).
func RRF(lists ...[]string) map[string]float64 {
	const k = 60.0
	out := map[string]float64{}
	for _, l := range lists {
		for i, id := range l {
			out[id] += 1 / (k + float64(i+1))
		}
	}
	return out
}

var trustRank = map[string]int{"T0": 0, "T1": 1, "T2": 2, "T3": 3, "T4": 4}

// ResolveConflicts keeps one memory per subject: the most trusted, then the
// most confident, then the newest. Dropped duplicates are reported so the
// model can be told the evidence is contested.
func ResolveConflicts(ms []Memory) (kept []Memory, conflicts map[string][]string) {
	conflicts = map[string][]string{}
	best := map[string]Memory{}
	order := []string{}
	for _, m := range ms {
		key := m.Scope + "|" + m.ScopeID + "|" + m.Subject
		cur, ok := best[key]
		if !ok {
			best[key] = m
			order = append(order, key)
			continue
		}
		if cur.ContentHash != m.ContentHash {
			conflicts[m.Subject] = append(conflicts[m.Subject], m.ID)
		}
		if better(m, cur) {
			if cur.ContentHash != m.ContentHash {
				conflicts[m.Subject] = append(conflicts[m.Subject], cur.ID)
			}
			best[key] = m
		}
	}
	for _, k := range order {
		kept = append(kept, best[k])
	}
	return kept, conflicts
}

func better(a, b Memory) bool {
	if trustRank[a.Trust] != trustRank[b.Trust] {
		return trustRank[a.Trust] < trustRank[b.Trust]
	}
	ca, cb := conf(a), conf(b)
	if ca != cb {
		return ca > cb
	}
	return a.UpdatedAt.After(b.UpdatedAt)
}

func conf(m Memory) float64 {
	if m.Confidence == nil {
		return 0.5
	}
	return *m.Confidence
}

func sortByScore(ids []string, score map[string]float64) {
	sort.SliceStable(ids, func(i, j int) bool {
		if score[ids[i]] != score[ids[j]] {
			return score[ids[i]] > score[ids[j]]
		}
		return ids[i] < ids[j]
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// ServiceFromGoal extracts "service=<name>" from a goal.
func ServiceFromGoal(goal string) string {
	for _, f := range strings.Fields(goal) {
		if v, ok := strings.CutPrefix(f, "service="); ok {
			return strings.Trim(v, ",.;")
		}
	}
	return ""
}
