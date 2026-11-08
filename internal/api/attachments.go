package api

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

// maxAttachmentBytes caps a served attachment; larger stored outputs are
// refused rather than streamed into a client.
const maxAttachmentBytes = 32 << 20

var unsafeFileChars = regexp.MustCompile(`[^\p{L}\p{N}._ -]+`)

// getAttachment serves GET /v1/runs/{run_id}/attachments/{index}: the stored
// tool output that attachment <index> of the run's result names. Only refs
// the kernel accepted into the result can be read here, always as a
// download (or an inline raster image), never as active content.
func (s *Server) getAttachment(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	idx, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || idx < 0 {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "index must be a non-negative integer")
		return
	}
	run, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if run.ResultRef == nil {
		writeErr(w, http.StatusNotFound, "not_found", "run has no result")
		return
	}
	data, err := s.Artifacts.Get(r.Context(), id.tenant, *run.ResultRef)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	var res struct {
		Attachments []domain.Attachment `json:"attachments"`
	}
	if json.Unmarshal(data, &res) != nil || idx >= len(res.Attachments) || res.Attachments[idx].Ref == "" {
		writeErr(w, http.StatusNotFound, "not_found", "attachment not found")
		return
	}
	att := res.Attachments[idx]
	body, err := s.Artifacts.Get(r.Context(), id.tenant, att.Ref)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if len(body) > maxAttachmentBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "attachment exceeds the download limit")
		return
	}
	ctype, disposition := "application/octet-stream", "attachment"
	if att.Type == domain.AttachImage && cognitive.InlineImage(att.MimeType) {
		ctype, disposition = att.MimeType, "inline"
	} else if t := textType(att.MimeType); t != "" {
		ctype = t
	}
	h := w.Header()
	h.Set("Content-Type", ctype)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": attachmentName(att, idx)}))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'none'; sandbox")
	h.Set("Cache-Control", "private, max-age=3600")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// textType keeps plain data types (served as downloads, so never rendered
// by a browser); everything else is an opaque byte stream.
func textType(m string) string {
	switch m {
	case "application/json", "text/plain", "text/csv", "application/x-ndjson", "text/markdown":
		return m + "; charset=utf-8"
	}
	return ""
}

// attachmentName derives a safe download name from the title and type.
func attachmentName(att domain.Attachment, idx int) string {
	name := strings.TrimSpace(unsafeFileChars.ReplaceAllString(att.Title, " "))
	name = strings.Trim(name, ". ")
	if r := []rune(name); len(r) > 80 {
		name = string(r[:80])
	}
	if name == "" {
		name = fmt.Sprintf("attachment-%d", idx+1)
	}
	if !strings.Contains(name, ".") {
		if exts, _ := mime.ExtensionsByType(att.MimeType); len(exts) > 0 {
			name += exts[0]
		} else if strings.HasPrefix(att.MimeType, "application/json") {
			name += ".json"
		}
	}
	return name
}
