package cognitive

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools/artifactq"
)

// Limits on the structured parts of a final answer. They keep a result
// artifact small and a client card readable whatever the model sends.
const (
	maxAttachments    = 12
	maxTableColumns   = 20
	maxTableRows      = 200
	maxCellRunes      = 500
	maxAttachTitle    = 200
	maxAttachCaption  = 1000
	maxAttachURLBytes = 2048
)

// attachmentsSchema describes submit_conclusion.attachments.
const attachmentsSchema = `{"type":"array","description":"Optional structured parts of the answer that the client shows as cards next to the text. Use them only when they help: a table of results, an image or chart URL, a link to a dashboard or document, or a file (a tool output to download). Do not repeat the same content in the answer text.","items":{"type":"object","properties":{
"type":{"type":"string","enum":["table","image","link","file"]},
"title":{"type":"string"},
"caption":{"type":"string","description":"One short line shown under the card."},
"url":{"type":"string","description":"https or http address (image, link, or a file to download)."},
"ref":{"type":"string","description":"For a file or image produced by a tool in this run: the observation ref (obs:step<N>) or the artifact handle shown in its header. Only offloaded tool outputs can be attached."},
"mime_type":{"type":"string"},
"columns":{"type":"array","items":{"type":"string"},"description":"Table header."},
"rows":{"type":"array","items":{"type":"array","items":{}},"description":"Table rows, one value per column."}},
"required":["type"]}}`

var mimeRe = regexp.MustCompile(`^[a-z0-9][a-z0-9!#$&^_.+-]{0,63}/[a-z0-9][a-z0-9!#$&^_.+-]{0,63}$`)

// rawAttachment is the lenient wire form: table cells may be numbers,
// booleans or null, and are rendered as text.
type rawAttachment struct {
	Type     string            `json:"type"`
	Title    string            `json:"title"`
	Caption  string            `json:"caption"`
	URL      string            `json:"url"`
	Ref      string            `json:"ref"`
	MimeType string            `json:"mime_type"`
	Columns  []json.RawMessage `json:"columns"`
	Rows     []json.RawMessage `json:"rows"`
}

// parseAttachments decodes submit_conclusion.attachments. Malformed items
// are skipped rather than failing the conclusion: they are optional.
func parseAttachments(raw json.RawMessage) []domain.Attachment {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return nil
	}
	var out []domain.Attachment
	for _, it := range items {
		var r rawAttachment
		if json.Unmarshal(it, &r) != nil {
			continue
		}
		a := domain.Attachment{Type: strings.ToLower(strings.TrimSpace(r.Type)), Title: r.Title, Caption: r.Caption,
			URL: r.URL, Ref: r.Ref, MimeType: r.MimeType}
		for _, c := range r.Columns {
			a.Columns = append(a.Columns, cellText(c))
		}
		for _, row := range r.Rows {
			var cells []json.RawMessage
			if json.Unmarshal(row, &cells) != nil {
				cells = []json.RawMessage{row}
			}
			line := make([]string, 0, len(cells))
			for _, c := range cells {
				line = append(line, cellText(c))
			}
			a.Rows = append(a.Rows, line)
		}
		out = append(out, a)
	}
	return out
}

func cellText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	t := strings.TrimSpace(string(raw))
	if t == "null" {
		return ""
	}
	return t
}

// ResolveAttachments validates the model's attachments against what the
// run actually produced and returns the ones a client may render:
//   - link/image/file URLs must be absolute http(s) addresses;
//   - a ref must name an offloaded tool output of this tenant (an obs:step<N>
//     citation is resolved to its artifact handle);
//   - text is trimmed and capped, tables are squared to their header.
//
// Anything else is dropped. The result is safe to store and serve; clients
// still treat every field as untrusted text.
func ResolveAttachments(tenantID string, in ReasoningInput, list []domain.Attachment) []domain.Attachment {
	byObs := map[string]*domain.ObservationArtifact{}
	byHandle := map[string]*domain.ObservationArtifact{}
	for i := range in.Observations {
		o := in.Observations[i]
		if o.Artifact == nil || o.Artifact.Handle == "" {
			continue
		}
		byObs[ObsPartRef(o.StepSeq, o.Part)] = o.Artifact
		byHandle[o.Artifact.Handle] = o.Artifact
	}
	var out []domain.Attachment
	for _, a := range list {
		if len(out) >= maxAttachments {
			break
		}
		a.Title = clip(a.Title, maxAttachTitle)
		a.Caption = clip(a.Caption, maxAttachCaption)
		a.MimeType = strings.ToLower(strings.TrimSpace(a.MimeType))
		if !mimeRe.MatchString(a.MimeType) {
			a.MimeType = ""
		}
		a.SizeBytes = 0
		switch a.Type {
		case domain.AttachTable:
			a.URL, a.Ref, a.MimeType = "", "", ""
			if !squareTable(&a) {
				continue
			}
		case domain.AttachLink:
			a.Ref, a.Columns, a.Rows = "", nil, nil
			if a.URL = webURL(a.URL); a.URL == "" {
				continue
			}
		case domain.AttachImage, domain.AttachFile:
			a.Columns, a.Rows = nil, nil
			a.URL = webURL(a.URL)
			ref := strings.TrimSpace(a.Ref)
			a.Ref = ""
			if ref != "" {
				art := byObs[strings.ToLower(ref)]
				if art == nil {
					art = byHandle[ref]
				}
				switch {
				case art != nil:
					a.Ref, a.SizeBytes = art.Handle, int64(art.Bytes)
					if a.MimeType == "" {
						a.MimeType = strings.ToLower(art.ContentType)
					}
				case artifactq.ValidHandle(tenantID, ref):
					a.Ref = ref // an earlier output, compacted out of view
				}
			}
			if a.URL == "" && a.Ref == "" {
				continue
			}
			if a.URL != "" && a.Ref != "" {
				a.URL = "" // a stored output is preferred over a remote copy
			}
			if a.Type == domain.AttachImage && a.Ref != "" && !InlineImage(a.MimeType) {
				a.Type = domain.AttachFile // not an image a client can show
			}
		default:
			continue
		}
		out = append(out, a)
	}
	return out
}

// InlineImage reports whether a stored attachment of this type may be shown
// inline. SVG is excluded: it can carry script.
func InlineImage(mime string) bool {
	switch strings.ToLower(mime) {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
		return true
	}
	return false
}

func webURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxAttachURLBytes {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil {
		return ""
	}
	return u.String()
}

func squareTable(a *domain.Attachment) bool {
	cols := a.Columns
	if len(cols) > maxTableColumns {
		cols = cols[:maxTableColumns]
	}
	width := len(cols)
	for _, r := range a.Rows {
		width = max(width, min(len(r), maxTableColumns))
	}
	if width == 0 {
		return false
	}
	head := make([]string, width)
	for i := range head {
		if i < len(cols) {
			head[i] = clip(cols[i], maxCellRunes)
		} else {
			head[i] = strconv.Itoa(i + 1)
		}
	}
	rows := a.Rows
	if len(rows) > maxTableRows {
		rows = rows[:maxTableRows]
		if a.Caption == "" {
			a.Caption = fmt.Sprintf("first %d of %d rows", maxTableRows, len(a.Rows))
		}
	}
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		line := make([]string, width)
		for i := range line {
			if i < len(r) {
				line[i] = clip(r[i], maxCellRunes)
			}
		}
		out = append(out, line)
	}
	a.Columns, a.Rows = head, out
	return len(out) > 0
}

func clip(s string, n int) string {
	s = strings.TrimSpace(strings.ToValidUTF8(s, ""))
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n-1]) + "…"
}
