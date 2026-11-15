package eventcursor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

var (
	ErrInvalid    = errors.New("event cursor is invalid")
	ErrCorrupt    = errors.New("event cursor is corrupt")
	ErrRegression = errors.New("event cursor cannot move backwards")
	ErrConflict   = errors.New("event cursor sequence identifies another event")
)

type Key struct {
	ServerURL  string
	TenantID   string
	SubjectID  string
	Stream     string
	ResourceID string
	Purpose    string
}

func (k Key) normalized() (Key, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(k.ServerURL), "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return Key{}, fmt.Errorf("%w: server URL", ErrInvalid)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	k.ServerURL = strings.TrimRight(u.String(), "/")
	for name, value := range map[string]string{"tenant": k.TenantID, "subject": k.SubjectID, "stream": k.Stream, "resource": k.ResourceID, "purpose": k.Purpose} {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return Key{}, fmt.Errorf("%w: %s", ErrInvalid, name)
		}
		switch name {
		case "tenant":
			k.TenantID = value
		case "subject":
			k.SubjectID = value
		case "stream":
			k.Stream = value
		case "resource":
			k.ResourceID = value
		case "purpose":
			k.Purpose = value
		}
	}
	return k, nil
}

func (k Key) ID() string {
	normalized, err := k.normalized()
	if err != nil {
		return ""
	}
	canonical, _ := json.Marshal(normalized)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
