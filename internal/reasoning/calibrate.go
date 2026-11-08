package reasoning

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Calibrator maps a stated confidence to the measured probability that a
// hypothesis with that confidence holds. It is an isotonic (monotone)
// regression of outcomes on stated confidence, fitted with pool-adjacent-
// violators and smoothed with a Beta(1,1) prior per block, so a block of a
// few samples never maps to exactly 0 or 1.
type Calibrator struct {
	// Blocks are ordered by Upper; a confidence maps to the first block
	// whose Upper is at least the confidence.
	Blocks []CalBlock `json:"blocks"`
	N      int        `json:"n"`
	// Buckets are the raw hit rates per stated-confidence bucket, for the
	// prompt note and reports.
	Buckets []CalBucket `json:"buckets"`
	// ECE is the expected calibration error of the raw confidences.
	ECE     float64   `json:"ece"`
	FitAt   time.Time `json:"fit_at"`
	Version string    `json:"version"`
}

// CalBlock is one isotonic block.
type CalBlock struct {
	Upper float64 `json:"upper"`
	Value float64 `json:"value"`
	N     int     `json:"n"`
}

// CalBucket is the hit rate of stated confidences in [Lo, Hi).
type CalBucket struct {
	Lo   float64 `json:"lo"`
	Hi   float64 `json:"hi"`
	N    int     `json:"n"`
	Mean float64 `json:"mean_stated"`
	Rate float64 `json:"hit_rate"`
}

// DefaultMinForecasts is the sample size below which no calibrator is fit.
const DefaultMinForecasts = 30

var bucketEdges = []float64{0, 0.3, 0.5, 0.7, 0.85, 1.0001}

// FitCalibrator fits a calibrator, or returns nil when there are fewer than
// minN usable forecasts.
func FitCalibrator(fs []Forecast, minN int) *Calibrator {
	if minN <= 0 {
		minN = DefaultMinForecasts
	}
	pts := make([]Forecast, 0, len(fs))
	for _, f := range fs {
		if f.P > 0 && f.P <= 1 && (f.Y == 0 || f.Y == 1) {
			pts = append(pts, f)
		}
	}
	if len(pts) < minN {
		return nil
	}
	sort.SliceStable(pts, func(i, j int) bool { return pts[i].P < pts[j].P })
	type block struct {
		upper    float64
		sum, n   float64
		smoothed float64
	}
	var bs []block
	for _, p := range pts {
		bs = append(bs, block{upper: p.P, sum: float64(p.Y), n: 1})
		// Pool adjacent violators of monotonicity (using smoothed means so
		// single-sample blocks do not dominate).
		for len(bs) > 1 {
			a, b := bs[len(bs)-2], bs[len(bs)-1]
			if (a.sum+1)/(a.n+2) <= (b.sum+1)/(b.n+2) && a.upper < b.upper {
				break
			}
			bs = bs[:len(bs)-2]
			bs = append(bs, block{upper: b.upper, sum: a.sum + b.sum, n: a.n + b.n})
		}
	}
	c := &Calibrator{N: len(pts), FitAt: time.Now().UTC(), Version: "isotonic-v1"}
	for _, b := range bs {
		c.Blocks = append(c.Blocks, CalBlock{Upper: b.upper, Value: round((b.sum + 1) / (b.n + 2)), N: int(b.n)})
	}
	c.Buckets, c.ECE = Buckets(pts)
	return c
}

// Apply returns the calibrated probability of a stated confidence
// (unchanged when c is nil or p is out of range).
func (c *Calibrator) Apply(p float64) float64 {
	if c == nil || len(c.Blocks) == 0 || p <= 0 || p > 1 {
		return p
	}
	for _, b := range c.Blocks {
		if p <= b.Upper {
			return b.Value
		}
	}
	return c.Blocks[len(c.Blocks)-1].Value
}

// Buckets returns per-bucket hit rates and the expected calibration error.
func Buckets(fs []Forecast) ([]CalBucket, float64) {
	var out []CalBucket
	total := 0
	var ece float64
	for i := 0; i+1 < len(bucketEdges); i++ {
		lo, hi := bucketEdges[i], bucketEdges[i+1]
		var n, hits int
		var sum float64
		for _, f := range fs {
			if f.P >= lo && f.P < hi {
				n++
				hits += f.Y
				sum += f.P
			}
		}
		if n == 0 {
			continue
		}
		b := CalBucket{Lo: lo, Hi: math.Min(hi, 1), N: n, Mean: round(sum / float64(n)), Rate: round(float64(hits) / float64(n))}
		out = append(out, b)
		total += n
		ece += float64(n) * math.Abs(b.Mean-b.Rate)
	}
	if total == 0 {
		return out, 0
	}
	return out, round(ece / float64(total))
}

// Note renders the measured calibration for the prompt: how often the
// model's stated confidence turned out right. Buckets with fewer than five
// samples are left out; "" when nothing is worth saying.
func (c *Calibrator) Note() string {
	if c == nil {
		return ""
	}
	var lines []string
	for _, b := range c.Buckets {
		if b.N < 5 {
			continue
		}
		lines = append(lines, fmt.Sprintf("- stated %.2f–%.2f (mean %.2f): held %.0f%% of the time (n=%d)",
			b.Lo, b.Hi, b.Mean, b.Rate*100, b.N))
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n")
}

// ScoreReader reads stored run scores.
type ScoreReader interface {
	ReasoningScores(ctx context.Context, tenantID string, f ScoreFilter) ([]RunScore, error)
}

// CalibrationService fits and caches calibrators per tenant and profile
// from the forecasts of recent scored runs. Forecasts settled by the
// independent verifier are used when there are enough of them; otherwise
// all forecasts are used.
type CalibrationService struct {
	Store  ScoreReader
	Window time.Duration // default 30 days
	TTL    time.Duration // default 15 minutes
	MinN   int           // default DefaultMinForecasts
	Now    func() time.Time

	mu    sync.Mutex
	cache map[string]calEntry
}

type calEntry struct {
	c  *Calibrator
	at time.Time
}

// Calibrator returns the calibrator of a tenant's profile (nil: not enough
// data).
func (s *CalibrationService) Calibrator(ctx context.Context, tenantID, profileID string) (*Calibrator, error) {
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	ttl := s.TTL
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	key := tenantID + "\x00" + profileID
	s.mu.Lock()
	if e, ok := s.cache[key]; ok && now.Sub(e.at) < ttl {
		s.mu.Unlock()
		return e.c, nil
	}
	s.mu.Unlock()
	window := s.Window
	if window <= 0 {
		window = 30 * 24 * time.Hour
	}
	scores, err := s.Store.ReasoningScores(ctx, tenantID, ScoreFilter{ProfileID: profileID, Since: now.Add(-window), Limit: 5000})
	if err != nil {
		return nil, err
	}
	var all, independent []Forecast
	for _, sc := range scores {
		for _, f := range sc.Metrics.Forecasts {
			all = append(all, f)
			if f.Independent {
				independent = append(independent, f)
			}
		}
	}
	minN := s.MinN
	if minN <= 0 {
		minN = DefaultMinForecasts
	}
	use := all
	if len(independent) >= minN {
		use = independent
	}
	c := FitCalibrator(use, minN)
	s.mu.Lock()
	if s.cache == nil {
		s.cache = map[string]calEntry{}
	}
	s.cache[key] = calEntry{c: c, at: now}
	s.mu.Unlock()
	return c, nil
}
