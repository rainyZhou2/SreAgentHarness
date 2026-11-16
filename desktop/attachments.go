package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// Answer cards: links open in the system browser, stored attachments are
// fetched through the authenticated client (the WebView never holds the
// token) and either previewed inline (raster images) or saved to disk.

const (
	maxInlineImage = 8 << 20
	maxDownload    = 32 << 20
)

// OpenURL opens an http(s) or mailto link from an answer in the system
// browser. Other schemes (javascript:, file:, custom handlers) are refused.
func (a *App) OpenURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "http", "https":
		if u.Host == "" {
			return errors.New(a.tr("link has no host", "链接缺少主机名"))
		}
	case "mailto":
	default:
		return fmt.Errorf(a.tr("refusing to open %q links", "不打开 %q 类型的链接"), u.Scheme)
	}
	if !a.native {
		return errors.New("preview mode: open links in the page")
	}
	runtime.BrowserOpenURL(a.ctx, u.String())
	return nil
}

// unsafeFileName keeps letters, digits, dots, dashes, underscores and spaces.
var unsafeFileName = regexp.MustCompile(`[^\p{L}\p{N}._ -]+`)

func attachmentPath(runID string, index int) string {
	return fmt.Sprintf("/api/runs/%s/attachments/%d", url.PathEscape(runID), index)
}

// AttachmentImage returns a stored image attachment as a data: URL for an
// inline preview. Only raster types are accepted (no SVG).
func (a *App) AttachmentImage(runID string, index int) (string, error) {
	c, err := a.c()
	if err != nil {
		return "", err
	}
	data, ctype, _, err := c.Download(a.ctx, attachmentPath(runID, index), maxInlineImage)
	if err != nil {
		return "", err
	}
	mt, _, _ := mime.ParseMediaType(ctype)
	switch mt {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
	default:
		return "", fmt.Errorf(a.tr("not an inline image (%s)", "不是可预览的图片（%s）"), mt)
	}
	return "data:" + mt + ";base64," + base64.StdEncoding.EncodeToString(data), nil
}

// SaveAttachment downloads a stored attachment and saves it (a save dialog in
// the native app, ~/Downloads in preview mode). It returns the path, or ""
// when the dialog was cancelled.
func (a *App) SaveAttachment(runID string, index int, suggested string) (string, error) {
	c, err := a.c()
	if err != nil {
		return "", err
	}
	data, _, disp, err := c.Download(a.ctx, attachmentPath(runID, index), maxDownload)
	if err != nil {
		return "", err
	}
	name := suggested
	if _, params, err := mime.ParseMediaType(disp); err == nil && params["filename"] != "" {
		name = params["filename"]
	}
	name = strings.Trim(strings.TrimSpace(unsafeFileName.ReplaceAllString(filepath.Base(name), " ")), ". ")
	if name == "" {
		name = fmt.Sprintf("attachment-%d", index+1)
	}
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, "Downloads")
	path := filepath.Join(dir, name)
	if a.native {
		path, err = runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{DefaultFilename: name, DefaultDirectory: dir,
			Title: a.tr("Save attachment", "保存附件")})
		if err != nil {
			return "", err
		}
		if path == "" {
			return "", nil
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// PickedFile is a text file selected by the user for one turn. Paths never
// leave the desktop process; only the display name and content are sent.
type PickedFile struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Text string `json:"text"`
}

// PickFiles opens a native multi-file dialog and returns UTF-8 text files.
func (a *App) PickFiles() ([]PickedFile, error) {
	if !a.native {
		return nil, errors.New(a.tr("file selection is available in the desktop app", "文件选择仅在桌面应用中可用"))
	}
	paths, err := runtime.OpenMultipleFilesDialog(a.ctx, runtime.OpenDialogOptions{
		Title: a.tr("Attach text files", "附加文本文件"),
	})
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return []PickedFile{}, nil
	}
	if len(paths) > maxPickedFiles {
		return nil, errors.New(a.tr("select at most 8 files", "最多选择 8 个文件"))
	}
	out := make([]PickedFile, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if info.Size() > maxPickedFileBytes {
			return nil, errors.New(a.tr("a file is larger than 256 KB", "单个文件不能超过 256 KB"))
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if !utf8.Valid(data) {
			return nil, errors.New(a.tr("only UTF-8 text files are supported", "仅支持 UTF-8 文本文件"))
		}
		out = append(out, PickedFile{Name: filepath.Base(path), Size: info.Size(), Text: string(data)})
	}
	return out, nil
}

const (
	maxPickedFileBytes = 256 << 10
	maxPickedFiles     = 8
)
