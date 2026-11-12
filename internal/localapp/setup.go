package localapp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// EnvFileName is the file `harness local init` writes model settings to
// (KEY=VALUE lines, mode 0600) in the data directory.
const EnvFileName = "local.env"

// envAllowed limits what the file may set: model credentials and the
// harness's own HARNESS_* settings, nothing else (PATH, LD_PRELOAD, ...).
func envAllowed(k string) bool {
	return strings.HasPrefix(k, "HARNESS_") || k == "ANTHROPIC_API_KEY" || k == "GEMINI_API_KEY"
}

// ParseEnv reads KEY=VALUE lines ('#' comments, optional quotes).
func ParseEnv(data string) (map[string]string, error) {
	out := map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(data))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("line %d: want KEY=VALUE", n)
		}
		if !envAllowed(k) {
			return nil, fmt.Errorf("line %d: %s may not be set here (only HARNESS_*, ANTHROPIC_API_KEY, GEMINI_API_KEY)", n, k)
		}
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out[k] = v
	}
	return out, sc.Err()
}

// LoadEnvFile applies the data directory's local.env to the process
// environment without overriding anything already set (the real
// environment wins). A missing file is fine. It returns the keys it set.
func LoadEnvFile(dataDir string) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(dataDir, EnvFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	vals, err := ParseEnv(string(b))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", EnvFileName, err)
	}
	var set []string
	for k, v := range vals {
		if os.Getenv(k) == "" {
			if err := os.Setenv(k, v); err != nil {
				return set, err
			}
			set = append(set, k)
		}
	}
	sort.Strings(set)
	return set, nil
}

// SaveEnv merges updates into local.env (an empty value removes a key) and
// rewrites it atomically with mode 0600.
func SaveEnv(dataDir string, updates map[string]string) error {
	path := filepath.Join(dataDir, EnvFileName)
	cur := map[string]string{}
	if b, err := os.ReadFile(path); err == nil {
		if cur, err = ParseEnv(string(b)); err != nil {
			return fmt.Errorf("%s: %w", EnvFileName, err)
		}
	}
	for k, v := range updates {
		if !envAllowed(k) {
			return fmt.Errorf("%s may not be stored", k)
		}
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("%s: value must be a single line", k)
		}
		if v == "" {
			delete(cur, k)
		} else {
			cur[k] = v
		}
	}
	keys := make([]string, 0, len(cur))
	for k := range cur {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# written by `harness local init`; the real environment overrides these\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, cur[k])
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	return writePrivate(path, []byte(b.String()))
}

// Provider kinds understood by ProbeProvider.
const (
	ProviderAnthropic = "anthropic"
	ProviderOpenAI    = "openai"
	ProviderGemini    = "gemini"
)

// ProbeProvider checks a model provider's endpoint and credential with a
// cheap read-only call (list models): it spends no tokens. baseURL "" means
// the provider's public endpoint.
func ProbeProvider(ctx context.Context, hc *http.Client, kind, baseURL, key string) error {
	if hc == nil {
		hc = &http.Client{Timeout: 15 * time.Second}
	}
	base := strings.TrimRight(baseURL, "/")
	hdr := map[string]string{}
	var path string
	switch kind {
	case ProviderAnthropic:
		if base == "" {
			base = "https://api.anthropic.com"
		}
		path = "/v1/models"
		hdr["x-api-key"], hdr["anthropic-version"] = key, "2023-06-01"
	case ProviderOpenAI:
		if base == "" {
			return errors.New("an OpenAI-compatible endpoint needs a base URL (for example https://api.openai.com/v1)")
		}
		path = "/models"
		if key != "" {
			hdr["Authorization"] = "Bearer " + key
		}
	case ProviderGemini:
		if base == "" {
			base = "https://generativelanguage.googleapis.com"
		}
		path = "/v1beta/models"
		hdr["x-goog-api-key"] = key
	default:
		return fmt.Errorf("unknown provider %q", kind)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach %s: %w", base, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return fmt.Errorf("%s rejected the key (HTTP %d)", base, resp.StatusCode)
	default:
		return fmt.Errorf("%s answered HTTP %d", base, resp.StatusCode)
	}
}

// ConfiguredProviders lists the model providers the environment selects, as
// buildGateway reads them.
func ConfiguredProviders(getenv func(string) string) []Provider {
	var ps []Provider
	if k := getenv("ANTHROPIC_API_KEY"); k != "" {
		ps = append(ps, Provider{ProviderAnthropic, getenv("HARNESS_ANTHROPIC_BASE_URL"), k})
	}
	if u := getenv("HARNESS_OPENAI_BASE_URL"); u != "" {
		ps = append(ps, Provider{ProviderOpenAI, u, getenv("HARNESS_OPENAI_API_KEY")})
	}
	if k := getenv("GEMINI_API_KEY"); k != "" {
		ps = append(ps, Provider{ProviderGemini, getenv("HARNESS_GEMINI_BASE_URL"), k})
	}
	return ps
}

// Provider is one configured model provider.
type Provider struct{ Kind, BaseURL, Key string }
