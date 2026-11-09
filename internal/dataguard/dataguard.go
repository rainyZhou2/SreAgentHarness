// Package dataguard classifies and redacts text before it leaves the
// control plane for a model provider (§19.3, §19.5).
//
// Two independent controls apply:
//
//   - Classification: every Work carries a data class (public, internal,
//     confidential, restricted). A deployment sets the highest class it may
//     send to its model provider; anything above is refused, not trimmed.
//   - Redaction: credentials are always removed; personal data (emails,
//     phone numbers, payment card and national id numbers) is removed for
//     every class above public. Infrastructure identifiers (hosts, IPs,
//     service names) are kept: they are what an SRE agent reasons about.
package dataguard

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"go-sre-harness/internal/memory"
)

// Class is a data classification level, ordered from least to most
// sensitive.
type Class int

const (
	Public Class = iota
	Internal
	Confidential
	Restricted
)

var names = []string{"public", "internal", "confidential", "restricted"}

func (c Class) String() string {
	if c < Public || c > Restricted {
		return "unknown"
	}
	return names[c]
}

// Parse reads a class name; "" is Internal (the default for operational
// data).
func Parse(s string) (Class, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return Internal, nil
	case "public":
		return Public, nil
	case "internal":
		return Internal, nil
	case "confidential":
		return Confidential, nil
	case "restricted":
		return Restricted, nil
	}
	return Internal, fmt.Errorf("unknown data class %q (public, internal, confidential, restricted)", s)
}

// ErrTooSensitive is returned when data above the allowed class would be
// sent to a model.
var ErrTooSensitive = errors.New("data class exceeds what may be sent to the model provider")

// Report counts what was removed.
type Report struct {
	Secrets int `json:"secrets,omitempty"`
	PII     int `json:"pii,omitempty"`
}

// Total is the number of removed spans.
func (r Report) Total() int { return r.Secrets + r.PII }

var piiPatterns = []struct {
	re    *regexp.Regexp
	label string
	check func(string) bool
}{
	{re: regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`), label: "[EMAIL]"},
	// Mainland China resident id (18 chars, checksum validated).
	{re: regexp.MustCompile(`\b\d{17}[\dXx]\b`), label: "[NATIONAL_ID]", check: validCNID},
	// Payment cards: 13–19 digits, optionally grouped, Luhn validated.
	{re: regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`), label: "[CARD]", check: luhn},
	// Phone numbers: CN mobile and E.164.
	{re: regexp.MustCompile(`(?:\+?86[ -]?)?\b1[3-9]\d{9}\b`), label: "[PHONE]"},
	{re: regexp.MustCompile(`\+[1-9]\d{7,14}\b`), label: "[PHONE]"},
}

// Redact removes credentials, and personal data unless class is Public.
func Redact(s string, class Class) (string, Report) {
	var rep Report
	s, rep.Secrets = memory.Redact(s)
	if class == Public {
		return s, rep
	}
	for _, p := range piiPatterns {
		s = p.re.ReplaceAllStringFunc(s, func(m string) string {
			if p.check != nil && !p.check(m) {
				return m
			}
			rep.PII++
			return p.label
		})
	}
	return s, rep
}

// ForModel prepares text of a given class for a model provider that may
// receive data up to max. It refuses data above max and redacts the rest.
func ForModel(s string, class, max Class) (string, Report, error) {
	if class > max {
		return "", Report{}, fmt.Errorf("%w: %s data, provider limit is %s", ErrTooSensitive, class, max)
	}
	out, rep := Redact(s, class)
	return out, rep, nil
}

func digits(s string) []int {
	var d []int
	for _, r := range s {
		if r >= '0' && r <= '9' {
			d = append(d, int(r-'0'))
		}
	}
	return d
}

func luhn(s string) bool {
	d := digits(s)
	if len(d) < 13 || len(d) > 19 {
		return false
	}
	sum := 0
	for i := len(d) - 1; i >= 0; i-- {
		v := d[i]
		if (len(d)-1-i)%2 == 1 {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
	}
	return sum%10 == 0
}

func validCNID(s string) bool {
	if len(s) != 18 {
		return false
	}
	w := []int{7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2}
	sum := 0
	for i := 0; i < 17; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return false
		}
		sum += int(c-'0') * w[i]
	}
	check := "10X98765432"[sum%11]
	last := s[17]
	if last == 'x' {
		last = 'X'
	}
	return last == check
}
