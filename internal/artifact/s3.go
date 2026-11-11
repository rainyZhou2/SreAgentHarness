package artifact

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// S3Config configures an S3-compatible store (AWS S3, MinIO, Ceph RGW).
// Credentials are read from the environment or files at call time, never
// from this struct's JSON, so the configuration can be committed.
type S3Config struct {
	// Endpoint is the service URL ("https://s3.eu-west-1.amazonaws.com",
	// "http://minio:9000"). Buckets are addressed path-style.
	Endpoint string `json:"endpoint"`
	Region   string `json:"region"`
	Bucket   string `json:"bucket"`
	// Prefix is prepended to every key ("harness/").
	Prefix string `json:"prefix,omitempty"`
	// AccessKeyEnv / SecretKeyEnv / SessionTokenEnv name the environment
	// variables holding the credentials (defaults: AWS_ACCESS_KEY_ID,
	// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
	AccessKeyEnv    string `json:"access_key_env,omitempty"`
	SecretKeyEnv    string `json:"secret_key_env,omitempty"`
	SessionTokenEnv string `json:"session_token_env,omitempty"`
	// Encryption: "" (bucket default), "AES256" or "aws:kms". With aws:kms,
	// KMSKeyID selects the key (empty: the bucket's default KMS key).
	Encryption string `json:"encryption,omitempty"`
	KMSKeyID   string `json:"kms_key_id,omitempty"`
	// VerifyWrites reads every object back after writing it and compares
	// the hash (default true).
	VerifyWrites *bool `json:"verify_writes,omitempty"`
	// RetentionClass is written as the object tag "retention" (e.g. "30d",
	// "incident"); bucket lifecycle rules expire raw telemetry by tag while
	// evidence cited by a postmortem keeps the longer class.
	RetentionClass string `json:"retention_class,omitempty"`
	// TimeoutSeconds bounds one request (default 30).
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`

	// HTTP replaces the client (tests).
	HTTP *http.Client `json:"-"`
	// Now replaces the clock (tests).
	Now func() time.Time `json:"-"`
}

// S3 is a content-addressed artifact store on an S3-compatible service.
//
//   - Tenant isolation: keys are "<prefix><tenant>/<kind>/<sha256>" and a
//     ref is only accepted for the tenant in its key. The tenant comes from
//     the caller's authenticated context, never from the object.
//   - Immutability: the key is the content hash, so a ref always names the
//     same bytes; writing existing content is a no-op.
//   - Integrity: Put sends the SHA-256 the service must verify, reads the
//     object back (VerifyWrites), and Get re-hashes what it read against
//     the key. A corrupted or swapped object is an error, not data.
//   - Encryption: TLS in transit (use an https endpoint) and server-side
//     encryption at rest (AES256 or KMS).
//   - Failure: an unreachable store fails the Put, so a step is never
//     reported successful without its artifact; nothing is cached locally
//     and silently lost.
type S3 struct {
	cfg  S3Config
	base *url.URL
	hc   *http.Client
}

// NewS3 validates the configuration. It does not contact the service.
func NewS3(cfg S3Config) (*S3, error) {
	u, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("artifact s3: endpoint must be http(s)://host")
	}
	if cfg.Bucket == "" || strings.ContainsAny(cfg.Bucket, "/ ") {
		return nil, fmt.Errorf("artifact s3: bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	switch cfg.Encryption {
	case "", "AES256", "aws:kms":
	default:
		return nil, fmt.Errorf("artifact s3: encryption must be AES256 or aws:kms")
	}
	if cfg.KMSKeyID != "" && cfg.Encryption != "aws:kms" {
		return nil, fmt.Errorf("artifact s3: kms_key_id needs encryption aws:kms")
	}
	if cfg.Prefix != "" && !strings.HasSuffix(cfg.Prefix, "/") {
		cfg.Prefix += "/"
	}
	if strings.HasPrefix(cfg.Prefix, "/") || strings.Contains(cfg.Prefix, "..") {
		return nil, fmt.Errorf("artifact s3: prefix is invalid")
	}
	def := func(v *string, d string) {
		if *v == "" {
			*v = d
		}
	}
	def(&cfg.AccessKeyEnv, "AWS_ACCESS_KEY_ID")
	def(&cfg.SecretKeyEnv, "AWS_SECRET_ACCESS_KEY")
	def(&cfg.SessionTokenEnv, "AWS_SESSION_TOKEN")
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = 30
	}
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &S3{cfg: cfg, base: u, hc: hc}, nil
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func hexSHA256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// uriEncode is the SigV4 path encoding (RFC 3986 unreserved characters and
// "/" are kept).
func uriEncode(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' || c == '/' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// sign adds AWS Signature Version 4 headers to req for payload hash ph.
func (s *S3) sign(req *http.Request, ph string) error {
	access, secret := os.Getenv(s.cfg.AccessKeyEnv), os.Getenv(s.cfg.SecretKeyEnv)
	if access == "" || secret == "" {
		return errors.New("artifact s3: credentials are not available")
	}
	now := s.cfg.Now().UTC()
	amzDate, date := now.Format("20060102T150405Z"), now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", ph)
	if tok := os.Getenv(s.cfg.SessionTokenEnv); tok != "" {
		req.Header.Set("x-amz-security-token", tok)
	}
	headers := map[string]string{"host": req.URL.Host}
	for k, v := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") || lk == "content-type" {
			headers[lk] = strings.TrimSpace(strings.Join(v, ","))
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canon strings.Builder
	for _, k := range names {
		canon.WriteString(k + ":" + headers[k] + "\n")
	}
	signed := strings.Join(names, ";")
	query := req.URL.Query().Encode() // sorted by key; the store only sends simple values
	creq := strings.Join([]string{req.Method, uriEncode(req.URL.Path), strings.ReplaceAll(query, "+", "%20"), canon.String(), signed, ph}, "\n")
	scope := date + "/" + s.cfg.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(creq))
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, s.cfg.Region)
	k = hmacSHA256(k, "s3")
	k = hmacSHA256(k, "aws4_request")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		access, scope, signed, hex.EncodeToString(hmacSHA256(k, toSign))))
	return nil
}

func (s *S3) do(ctx context.Context, method, key string, body []byte, hdr map[string]string) (*http.Response, error) {
	u := *s.base
	u.Path = strings.TrimRight(u.Path, "/") + "/" + s.cfg.Bucket + "/" + s.cfg.Prefix + key
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if err := s.sign(req, hexSHA256(body)); err != nil {
		return nil, err
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		// The transport error may quote the URL; the endpoint is not the
		// caller's business.
		return nil, errors.New("artifact s3: the store is not reachable")
	}
	return resp, nil
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
}

// Put stores data under its content hash and returns "s3://<key>".
func (s *S3) Put(ctx context.Context, tenantID, kind string, data []byte) (string, string, error) {
	key, h, err := objectKey(tenantID, kind, data)
	if err != nil {
		return "", "", err
	}
	// Same content, same key: an object that is already there is the
	// object we would write.
	if resp, err := s.do(ctx, http.MethodHead, key, nil, nil); err == nil {
		drain(resp)
		if resp.StatusCode == http.StatusOK {
			return "s3://" + key, h, nil
		}
	}
	hdr := map[string]string{"Content-Type": "application/octet-stream",
		"x-amz-meta-sha256": strings.TrimPrefix(h, "sha256:"), "x-amz-meta-tenant": tenantID}
	if s.cfg.Encryption != "" {
		hdr["x-amz-server-side-encryption"] = s.cfg.Encryption
		if s.cfg.KMSKeyID != "" {
			hdr["x-amz-server-side-encryption-aws-kms-key-id"] = s.cfg.KMSKeyID
		}
	}
	if s.cfg.RetentionClass != "" {
		hdr["x-amz-tagging"] = "retention=" + url.QueryEscape(s.cfg.RetentionClass) + "&kind=" + url.QueryEscape(kind)
	}
	resp, err := s.do(ctx, http.MethodPut, key, data, hdr)
	if err != nil {
		return "", "", err
	}
	drain(resp)
	if resp.StatusCode/100 != 2 {
		return "", "", fmt.Errorf("artifact s3: put failed with HTTP %d", resp.StatusCode)
	}
	if s.cfg.VerifyWrites == nil || *s.cfg.VerifyWrites {
		got, err := s.Get(ctx, tenantID, "s3://"+key)
		if err != nil {
			return "", "", fmt.Errorf("artifact s3: the object could not be read back: %w", err)
		}
		if Hash(got) != h {
			return "", "", errors.New("artifact s3: the object read back differs from the one written")
		}
	}
	return "s3://" + key, h, nil
}

// Get returns the bytes of a ref of the tenant, verified against the hash
// in its key.
func (s *S3) Get(ctx context.Context, tenantID, ref string) ([]byte, error) {
	key, err := parseRef("s3", tenantID, ref)
	if err != nil {
		return nil, err
	}
	resp, err := s.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotFound
	case resp.StatusCode/100 != 2:
		return nil, fmt.Errorf("artifact s3: get failed with HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxObjectBytes+1))
	if err != nil {
		return nil, errors.New("artifact s3: the object could not be read")
	}
	if len(data) > MaxObjectBytes {
		return nil, fmt.Errorf("artifact s3: object exceeds %d bytes", MaxObjectBytes)
	}
	if want := key[strings.LastIndex(key, "/")+1:]; strings.TrimPrefix(Hash(data), "sha256:") != want {
		// The bytes are not what the ref names: corruption or tampering.
		return nil, ErrCorrupt
	}
	return data, nil
}

// Delete removes an object (retention).
func (s *S3) Delete(ctx context.Context, tenantID, ref string) error {
	key, err := parseRef("s3", tenantID, ref)
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, http.MethodDelete, key, nil, nil)
	if err != nil {
		return err
	}
	drain(resp)
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("artifact s3: delete failed with HTTP %d", resp.StatusCode)
	}
	return nil
}

// MaxObjectBytes bounds one artifact read from a remote store.
const MaxObjectBytes = 256 << 20

// ErrCorrupt: the stored bytes do not match the content hash of their ref.
var ErrCorrupt = errors.New("artifact content does not match its hash")

// Mux writes to a primary store and reads from whichever store a ref
// belongs to. It is how a deployment moves from one backend to another
// (fs to s3): new artifacts go to the primary, and every ref written
// before the move — in run results, evidence and postmortems — stays
// readable from the store that holds it.
type Mux struct {
	Primary Store
	// Readers are the stores by ref scheme ("fs", "s3", "mem").
	Readers map[string]Store
}

func refScheme(ref string) string {
	s, _, _ := strings.Cut(ref, "://")
	return s
}

func (m *Mux) Put(ctx context.Context, tenantID, kind string, data []byte) (string, string, error) {
	return m.Primary.Put(ctx, tenantID, kind, data)
}

func (m *Mux) Get(ctx context.Context, tenantID, ref string) ([]byte, error) {
	if st, ok := m.Readers[refScheme(ref)]; ok {
		return st.Get(ctx, tenantID, ref)
	}
	return nil, ErrNotFound
}

func (m *Mux) Delete(ctx context.Context, tenantID, ref string) error {
	if d, ok := m.Readers[refScheme(ref)].(Deleter); ok {
		return d.Delete(ctx, tenantID, ref)
	}
	return nil
}
