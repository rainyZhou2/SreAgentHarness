package planner

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sort"
	"strings"
	"unicode"
)

// Classifier predicts the orchestration mode of a goal. confidence is the
// margin between the best and the second-best mode (0..1); the router only
// trusts a prediction above its threshold.
type Classifier interface {
	Classify(goal string) (mode string, confidence float64)
}

// Example is one labelled goal for the classifier.
type Example struct {
	Text string `json:"text"`
	Mode string `json:"mode"` // react | flow
}

// DefaultExamples are generic, language-mixed SRE goals. Deployments add
// their own with HARNESS_PLANNER_ROUTER_EXAMPLES (a JSON file of Examples).
var DefaultExamples = []Example{
	{"why is checkout latency high", "react"},
	{"what changed in the payments service in the last hour", "react"},
	{"explain the 5xx spike on the api gateway", "react"},
	{"is the database replica lagging", "react"},
	{"show me the error rate for orders", "react"},
	{"summarize the current incident", "react"},
	{"which deployment caused the regression", "react"},
	{"check whether the queue consumers are healthy", "react"},
	{"investigate the memory growth in the cache service", "react"},
	{"find the root cause of the timeouts", "react"},
	{"status of the node pool", "react"},
	{"what does this alert mean", "react"},
	{"为什么支付服务延迟升高", "react"},
	{"查一下订单服务的错误率", "react"},
	{"解释一下这个告警", "react"},
	{"排查网关超时的根因", "react"},
	{"数据库主从延迟是否正常", "react"},
	{"总结一下当前故障的进展", "react"},
	{"看看最近一小时有哪些变更", "react"},
	{"分析缓存服务内存上涨的原因", "react"},
	{"restart the checkout deployment after checking its health", "flow"},
	{"roll back payments to the previous release", "flow"},
	{"scale the orders service to ten replicas and verify", "flow"},
	{"drain the node and move its pods", "flow"},
	{"fail over the database to the standby region", "flow"},
	{"rotate the api gateway certificates", "flow"},
	{"collect metrics and logs in parallel then decide whether to restart", "flow"},
	{"run the disk cleanup runbook on the build hosts", "flow"},
	{"apply the hotfix and confirm error rate recovers", "flow"},
	{"cordon the faulty node and open a ticket", "flow"},
	{"redeploy the search service with the last good image", "flow"},
	{"mitigate the incident: shed load, then restart the workers", "flow"},
	{"重启支付服务并确认恢复", "flow"},
	{"把订单服务回滚到上一个版本", "flow"},
	{"将网关扩容到十个副本", "flow"},
	{"对故障节点摘流并迁移容器", "flow"},
	{"执行磁盘清理预案", "flow"},
	{"数据库切换到备库", "flow"},
	{"先收集指标和日志再决定是否重启", "flow"},
	{"发布热修复并验证错误率下降", "flow"},
	{"帮我重启一下网关", "flow"},
	{"把推荐服务降级一下", "flow"},
	{"给缓存集群扩一下容", "flow"},
	{"bounce the api pods", "flow"},
	{"revert the config change from yesterday", "flow"},
	{"kick the stuck consumers and confirm the lag drains", "flow"},
	{"网关现在什么情况", "react"},
	{"帮我看一下网关的延迟", "react"},
	{"how many errors did billing return today", "react"},
	{"search the logs for checkout errors", "react"},
	{"did the deploy finish", "react"},
	{"latency of the checkout service", "react"},
}

// EmbeddingClassifier classifies with any text embedding (for example a
// model-backed memory.Embedder) by k-nearest labelled examples.
type EmbeddingClassifier struct {
	Embed    func(string) []float32
	K        int
	examples []vecExample
}

// NewEmbeddingClassifier embeds the examples once.
func NewEmbeddingClassifier(embed func(string) []float32, examples []Example) *EmbeddingClassifier {
	c := &EmbeddingClassifier{Embed: embed, K: 3}
	for _, e := range examples {
		if (e.Mode == "react" || e.Mode == "flow") && strings.TrimSpace(e.Text) != "" {
			v := append([]float32(nil), embed(e.Text)...)
			normalize(v)
			c.examples = append(c.examples, vecExample{mode: e.Mode, v: v})
		}
	}
	return c
}

// Classify implements Classifier.
func (c *EmbeddingClassifier) Classify(goal string) (string, float64) {
	if c == nil || len(c.examples) == 0 {
		return "", 0
	}
	q := append([]float32(nil), c.Embed(goal)...)
	normalize(q)
	return knn(q, c.examples, c.K)
}

// LoadExamples reads a JSON array of Examples.
func LoadExamples(path string) ([]Example, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ex []Example
	if err := json.Unmarshal(data, &ex); err != nil {
		return nil, fmt.Errorf("router examples %s: %w", path, err)
	}
	for i, e := range ex {
		if e.Mode != "react" && e.Mode != "flow" {
			return nil, fmt.Errorf("router examples %s: entry %d has mode %q", path, i, e.Mode)
		}
	}
	return ex, nil
}

// NgramClassifier is a deterministic nearest-neighbour classifier over
// hashed word and character n-grams. Character bigrams make it work for
// languages without spaces, and paraphrases that share no keyword with the
// regex lists still land near similar labelled goals. A model-backed
// embedding can replace it through the Classifier interface.
type NgramClassifier struct {
	// K is the number of nearest examples averaged per mode (default 3).
	K        int
	examples []vecExample
	// weight is the per-feature discriminativeness learned from the
	// examples: features both modes share (service names, filler words)
	// count little, features one mode owns (verbs, question openers) a lot.
	weight []float32
}

type vecExample struct {
	mode string
	v    []float32
}

const ngramDims = 4096

// NewNgramClassifier indexes the examples.
func NewNgramClassifier(examples []Example) *NgramClassifier {
	c := &NgramClassifier{K: 3}
	type raw struct {
		mode string
		v    []float32
	}
	var rs []raw
	present := map[string][]float64{"react": make([]float64, ngramDims), "flow": make([]float64, ngramDims)}
	count := map[string]float64{}
	for _, e := range examples {
		if e.Mode != "react" && e.Mode != "flow" || strings.TrimSpace(e.Text) == "" {
			continue
		}
		v := ngramRaw(e.Text)
		rs = append(rs, raw{e.Mode, v})
		count[e.Mode]++
		for i, x := range v {
			if x != 0 {
				present[e.Mode][i]++
			}
		}
	}
	c.weight = make([]float32, ngramDims)
	for i := range c.weight {
		var pr, pf float64
		if count["react"] > 0 {
			pr = present["react"][i] / count["react"]
		}
		if count["flow"] > 0 {
			pf = present["flow"][i] / count["flow"]
		}
		c.weight[i] = float32(math.Abs(pr-pf) + 0.02)
	}
	for _, r := range rs {
		c.examples = append(c.examples, vecExample{mode: r.mode, v: c.weigh(r.v)})
	}
	return c
}

func (c *NgramClassifier) weigh(v []float32) []float32 {
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = x * c.weight[i]
	}
	normalize(out)
	return out
}

// Classify returns the mode whose nearest examples are most similar.
func (c *NgramClassifier) Classify(goal string) (string, float64) {
	if c == nil || len(c.examples) == 0 {
		return "", 0
	}
	return knn(c.weigh(ngramRaw(goal)), c.examples, c.K)
}

func knn(q []float32, examples []vecExample, k int) (string, float64) {
	sims := map[string][]float64{}
	for _, e := range examples {
		if len(e.v) != len(q) {
			continue
		}
		sims[e.mode] = append(sims[e.mode], dot(q, e.v))
	}
	if k <= 0 {
		k = 3
	}
	if len(sims) == 0 {
		return "", 0
	}
	score := map[string]float64{}
	for mode, s := range sims {
		sort.Sort(sort.Reverse(sort.Float64Slice(s)))
		n := k
		if len(s) < n {
			n = len(s)
		}
		var sum float64
		for _, x := range s[:n] {
			sum += x
		}
		score[mode] = sum / float64(n)
	}
	react, flow := score["react"], score["flow"]
	if react == 0 && flow == 0 {
		return "", 0
	}
	if flow > react {
		return "flow", flow - react
	}
	return "react", react - flow
}

// ngramRaw returns the unnormalized hashed feature vector of text.
func ngramRaw(text string) []float32 {
	v := make([]float32, ngramDims)
	add := func(s string, w float32) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(s))
		x := h.Sum32()
		sign := float32(1)
		if x&1 == 1 {
			sign = -1
		}
		v[int(x>>1)%ngramDims] += sign * w
	}
	text = strings.ToLower(text)
	var words []string
	var cjk []rune
	flushCJK := func() {
		for i, r := range cjk {
			add("c:"+string(r), 0.5)
			if i > 0 {
				add("c2:"+string(cjk[i-1:i+1]), 1)
			}
		}
		cjk = cjk[:0]
	}
	var word []rune
	flushWord := func() {
		if len(word) > 1 {
			words = append(words, string(word))
		}
		word = word[:0]
	}
	for _, r := range text {
		switch {
		case unicode.Is(unicode.Han, r) || unicode.Is(unicode.Hiragana, r) || unicode.Is(unicode.Katakana, r) || unicode.Is(unicode.Hangul, r):
			flushWord()
			cjk = append(cjk, r)
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			flushCJK()
			word = append(word, r)
		default:
			flushWord()
			flushCJK()
		}
	}
	flushWord()
	flushCJK()
	for i, w := range words {
		add("w:"+w, 1)
		if len(w) > 4 {
			add("p:"+w[:4], 0.5) // crude stem: restart/restarting, deploy/deployment
		}
		if i > 0 {
			add("b:"+words[i-1]+"_"+w, 0.5)
		}
	}
	if len(words) > 0 {
		add("first:"+words[0], 1) // question openers and imperative verbs
	}
	return v
}

func normalize(v []float32) {
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
}

func dot(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i] * b[i])
	}
	return s
}
