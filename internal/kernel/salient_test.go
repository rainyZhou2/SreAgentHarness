package kernel

import (
	"fmt"
	"strings"
	"testing"
)

func logWith(n int, special string, at int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		if i == at {
			b.WriteString(special + "\n")
		}
		fmt.Fprintf(&b, "2026-11-11T10:%02d:%02dZ INFO heartbeat ok seq=%d latency=%dms\n", i/60%60, i%60, i, 10+i%5)
	}
	return b.String()
}

func TestSalientKeepsBuriedLine(t *testing.T) {
	special := "2026-11-11T10:09:41Z WARN db connection pool exhausted (100/100 in use)"
	body := logWith(400, special, 200) // ~28 KB, the line is in the middle
	out := salientExcerpt(body, 1500)
	if !strings.Contains(out, "pool exhausted (100/100 in use)") {
		t.Fatalf("the decisive middle line was dropped:\n%s", out)
	}
	if len(out) > 1500+64 {
		t.Fatalf("over budget: %d", len(out))
	}
	if !strings.Contains(out, "[…]") {
		t.Fatal("omitted stretches must be marked")
	}
	if clipBytes(body, 1500) == out || strings.Contains(clipBytes(body, 1500), "pool exhausted") {
		t.Fatal("the head clip is expected to miss it (that is the point)")
	}
}

func TestSalientKeepsOrderAndShortInput(t *testing.T) {
	if got := salientExcerpt("short", 100); got != "short" {
		t.Fatalf("%q", got)
	}
	body := logWith(300, "ERROR boom A", 100) + "ERROR boom B\n"
	out := salientExcerpt(body, 1200)
	a, b := strings.Index(out, "boom A"), strings.Index(out, "boom B")
	if a < 0 || b < 0 || a > b {
		t.Fatalf("both lines must be kept in original order: %d %d", a, b)
	}
	// One very long line cannot be selected from: falls back to a clip.
	long := strings.Repeat("x", 5000)
	if got := salientExcerpt(long, 100); len(got) > 110 {
		t.Fatalf("%d", len(got))
	}
}

func TestSalientIsValidUTF8(t *testing.T) {
	body := strings.Repeat("日志行 正常 心跳 ok\n", 500) + "致命错误 fatal 连接耗尽\n" + strings.Repeat("日志行 正常 心跳 ok\n", 500)
	out := salientExcerpt(body, 700)
	if !strings.Contains(out, "致命错误") {
		t.Fatalf("%s", out)
	}
}
