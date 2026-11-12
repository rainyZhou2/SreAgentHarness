package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// Personal memory: the stable preferences the agent keeps about the user
// (language, format, defaults). Only the owner sees and edits them.

type personalMemory struct {
	Subject string `json:"subject"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

// MemoryList prints the user's active personal memories.
func MemoryList(ctx context.Context, c *Client, out io.Writer) error {
	var r struct {
		Memories []personalMemory `json:"memories"`
	}
	if err := c.Do(ctx, "GET", "/v1/me/memories", nil, &r); err != nil {
		return err
	}
	n := 0
	for _, m := range r.Memories {
		if m.Status != "" && m.Status != "ACTIVE" {
			continue
		}
		// The subject key is what set/rm accept (the server normalizes keys).
		key := strings.TrimPrefix(m.Subject, "pref:")
		if key == "" {
			key = m.Title
		}
		fmt.Fprintf(out, "%-24s %s\n", key, m.Content)
		n++
	}
	if n == 0 {
		fmt.Fprintln(out, "no personal memories yet")
	}
	return nil
}

// MemorySet creates or revises one personal memory.
func MemorySet(ctx context.Context, c *Client, out io.Writer, key, value string) error {
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if key == "" || value == "" {
		return errors.New("a key and a value are required")
	}
	var r struct {
		Created    bool `json:"created"`
		Redactions int  `json:"redactions"`
	}
	if err := c.Do(ctx, "PUT", "/v1/me/memories/"+url.PathEscape(key), map[string]string{"value": value}, &r); err != nil {
		return err
	}
	verb := "updated"
	if r.Created {
		verb = "saved"
	}
	fmt.Fprintf(out, "%s %q\n", verb, key)
	if r.Redactions > 0 {
		fmt.Fprintf(out, "  note: %d sensitive value(s) were redacted before saving\n", r.Redactions)
	}
	return nil
}

// MemoryRemove archives one personal memory.
func MemoryRemove(ctx context.Context, c *Client, out io.Writer, key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("a key is required")
	}
	if err := c.Do(ctx, "DELETE", "/v1/me/memories/"+url.PathEscape(key), nil, nil); err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == 404 {
			return fmt.Errorf("no personal memory %q", key)
		}
		return err
	}
	fmt.Fprintf(out, "removed %q\n", key)
	return nil
}
