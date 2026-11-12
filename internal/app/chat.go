package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go-sre-harness/internal/cli"
	"go-sre-harness/internal/localapp"
)

// RunChat is `harness chat`: an interactive session with inline approvals.
//
//	harness chat [-server URL] [-token T | -token-file F] [-data DIR] [-session ID] [-yes]
//
// Without flags it talks to a local runtime (`harness local`) using the
// token in its data directory.
func RunChat(ctx context.Context, args []string, getenv func(string) string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("chat", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", firstNonEmpty(getenv("HARNESS_SERVER"), "http://"+defaultLocalAddr), "harness API URL")
	token := fs.String("token", getenv("HARNESS_TOKEN"), "bearer token (or HARNESS_TOKEN)")
	tokenFile := fs.String("token-file", "", "file holding the bearer token")
	data := fs.String("data", localapp.DataDirFromEnv(getenv), "local data directory (its token file is the default credential)")
	session := fs.String("session", "", "resume this session id")
	title := fs.String("title", "", "title for a new session")
	yes := fs.Bool("yes", false, "approve actions without asking (R3 is always denied by the server)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	tok, err := resolveToken(*token, *tokenFile, *data)
	if err != nil {
		fmt.Fprintln(stderr, "harness chat:", err)
		return 2
	}
	c, err := cli.NewClient(*server, tok)
	if err != nil {
		fmt.Fprintln(stderr, "harness chat:", err)
		return 2
	}
	ch := &cli.Chat{C: c, In: stdin, Out: stdout, Session: *session, Title: *title, AutoYes: *yes}
	if err := ch.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(stderr, "harness chat:", err)
		return 1
	}
	return 0
}

// RunMemory is `harness memory list|set KEY VALUE|rm KEY`: view and edit the
// personal memory the agent keeps about you.
func RunMemory(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: harness memory list | set KEY VALUE | rm KEY | review | show ID | approve ID | reject ID [REASON] | archive ID   [-server URL] [-token T | -token-file F] [-data DIR]")
		return 2
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("memory", flag.ContinueOnError)
	fs.SetOutput(stderr)
	server := fs.String("server", firstNonEmpty(getenv("HARNESS_SERVER"), "http://"+defaultLocalAddr), "harness API URL")
	token := fs.String("token", getenv("HARNESS_TOKEN"), "bearer token (or HARNESS_TOKEN)")
	tokenFile := fs.String("token-file", "", "file holding the bearer token")
	data := fs.String("data", localapp.DataDirFromEnv(getenv), "local data directory")
	// Flags may come before or after the positional arguments.
	var pos []string
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			return 2
		}
		rest = fs.Args()
		if len(rest) > 0 {
			pos, rest = append(pos, rest[0]), rest[1:]
		}
	}
	tok, err := resolveToken(*token, *tokenFile, *data)
	if err != nil {
		fmt.Fprintln(stderr, "harness memory:", err)
		return 2
	}
	c, err := cli.NewClient(*server, tok)
	if err != nil {
		fmt.Fprintln(stderr, "harness memory:", err)
		return 2
	}
	switch {
	case sub == "list" && len(pos) == 0:
		err = cli.MemoryList(ctx, c, stdout)
	case sub == "set" && len(pos) >= 2:
		err = cli.MemorySet(ctx, c, stdout, pos[0], strings.Join(pos[1:], " "))
	case (sub == "rm" || sub == "delete") && len(pos) == 1:
		err = cli.MemoryRemove(ctx, c, stdout, pos[0])
	case sub == "review" && len(pos) == 0:
		err = cli.MemoryReview(ctx, c, stdout)
	case sub == "show" && len(pos) == 1:
		err = cli.MemoryShow(ctx, c, stdout, pos[0])
	case (sub == "approve" || sub == "reject" || sub == "archive") && len(pos) >= 1:
		err = cli.MemoryDecide(ctx, c, stdout, pos[0], sub, strings.Join(pos[1:], " "))
	default:
		fmt.Fprintln(stderr, "usage: harness memory list | set KEY VALUE | rm KEY | review | show ID | approve ID | reject ID [REASON] | archive ID")
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "harness memory:", err)
		return 1
	}
	return 0
}

func resolveToken(flagToken, file, dataDir string) (string, error) {
	if t := strings.TrimSpace(flagToken); t != "" {
		return t, nil
	}
	path := file
	if path == "" {
		path = filepath.Join(dataDir, "token")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("no token: pass -token, set HARNESS_TOKEN, or run `harness local` first (looked for %s)", path)
	}
	return strings.TrimSpace(string(b)), nil
}
