// Package cpkit holds small primitives shared by the v2 agent control plane
// (work, eventing, world, continuous, action, outcome, evidence): config
// durations, canonical hashing, resource references and a clock. It has no
// dependencies on storage, HTTP or the kernel.
package cpkit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Duration is a JSON duration: "30s", "5m", or a number of seconds.
type Duration time.Duration

// D returns the time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		if s == "" {
			*d = 0
			return nil
		}
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string like \"5m\" or a number of seconds")
	}
	*d = Duration(n * float64(time.Second))
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return []byte(`"0s"`), nil
	}
	return json.Marshal(time.Duration(d).String())
}

// Clock is the time source; tests replace it.
type Clock func() time.Time

// Now returns the current UTC time from c, or the wall clock when c is nil.
func (c Clock) Now() time.Time {
	if c != nil {
		return c().UTC()
	}
	return time.Now().UTC()
}

// NewID returns a time-ordered UUID (v7).
func NewID() string { return uuid.Must(uuid.NewV7()).String() }

// ValidID reports whether id is a UUID.
func ValidID(id string) bool { _, err := uuid.Parse(id); return err == nil }

// CanonicalJSON re-encodes v with object keys sorted at every level and no
// insignificant whitespace, so equal values hash equally regardless of the
// field order a client sent.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, generic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonical(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteByte(':')
			if err := writeCanonical(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, e); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}

// Hash returns "sha256:<hex>" of the canonical JSON of v.
func Hash(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	return HashBytes(b), nil
}

// MustHash is Hash for values that always marshal (plain structs).
func MustHash(v any) string {
	h, err := Hash(v)
	if err != nil {
		panic(err)
	}
	return h
}

// HashBytes returns "sha256:<hex>" of b.
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ErrInvalidRef is returned for malformed resource references.
var ErrInvalidRef = errors.New("invalid resource reference")

// MaxRefLen bounds a resource reference.
const MaxRefLen = 512

// NormalizeRef validates and canonicalizes a resource reference of the form
// scheme://authority/path (e.g. "service://prod/search-api",
// "k8s://cluster/ns/Deployment/name"). The scheme is lower-cased, empty path
// segments and trailing slashes are removed. Free text is rejected: a ref is
// an identity used for authorization and correlation, never a description.
func NormalizeRef(ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" || len(ref) > MaxRefLen {
		return "", fmt.Errorf("%w: %q", ErrInvalidRef, ref)
	}
	scheme, rest, ok := strings.Cut(ref, "://")
	if !ok || scheme == "" || rest == "" {
		return "", fmt.Errorf("%w: %q (want scheme://path)", ErrInvalidRef, ref)
	}
	scheme = strings.ToLower(scheme)
	for _, r := range scheme {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' || r == '+') {
			return "", fmt.Errorf("%w: bad scheme %q", ErrInvalidRef, scheme)
		}
	}
	for _, r := range rest {
		if r <= ' ' || r == 0x7f || r == '?' || r == '#' || r == '\\' {
			return "", fmt.Errorf("%w: %q contains a forbidden character", ErrInvalidRef, ref)
		}
	}
	parts := strings.Split(rest, "/")
	clean := parts[:0]
	for _, p := range parts {
		if p == "" {
			continue
		}
		if p == "." || p == ".." {
			return "", fmt.Errorf("%w: %q contains a relative segment", ErrInvalidRef, ref)
		}
		if u, err := url.PathUnescape(p); err != nil || u != p && strings.ContainsAny(u, "/") {
			return "", fmt.Errorf("%w: %q contains an encoded separator", ErrInvalidRef, ref)
		}
		clean = append(clean, p)
	}
	if len(clean) == 0 {
		return "", fmt.Errorf("%w: %q has no path", ErrInvalidRef, ref)
	}
	return scheme + "://" + strings.Join(clean, "/"), nil
}

// NormalizeRefs normalizes, de-duplicates and sorts refs.
func NormalizeRefs(refs []string) ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		n, err := NormalizeRef(r)
		if err != nil {
			return nil, err
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
}

// RefScheme returns the scheme of a normalized ref.
func RefScheme(ref string) string {
	s, _, _ := strings.Cut(ref, "://")
	return s
}

// RefMatches reports whether ref equals pattern or lies under it
// (pattern "service://prod" matches "service://prod/search-api"). A pattern
// ending in "/*" matches exactly one more segment.
func RefMatches(pattern, ref string) bool {
	if pattern == ref {
		return true
	}
	if p, ok := strings.CutSuffix(pattern, "/*"); ok {
		rest, ok := strings.CutPrefix(ref, p+"/")
		return ok && rest != "" && !strings.Contains(rest, "/")
	}
	return strings.HasPrefix(ref, pattern+"/")
}

// Truncate bounds a string to n bytes on a rune boundary.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && n < len(s) && (s[n]&0xC0) == 0x80 {
		n--
	}
	return s[:n]
}
