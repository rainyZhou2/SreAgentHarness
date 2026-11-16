package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go-sre-harness/desktop/harness"
)

// TurnContext is the optional environment/service/time window a user pins to
// one turn. It is a hint for the planner, never a permission grant.
type TurnContext struct {
	Environment string   `json:"environment,omitempty"`
	Services    []string `json:"services,omitempty"`
	WindowStart string   `json:"window_start,omitempty"`
	WindowEnd   string   `json:"window_end,omitempty"`
	// Attachments are UTF-8 text files selected for this turn. The server
	// treats them as untrusted context, never as instructions.
	Attachments []string `json:"attachments,omitempty"`
}

// SendTurn submits a turn with optional pinned context.
func (a *App) SendTurn(sessionID, text string, input TurnContext) (map[string]any, error) {
	if strings.TrimSpace(text) == "" {
		return nil, errors.New(a.tr("message is required", "消息不能为空"))
	}
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"text": text, "client_request_id": harness.NewKey()}
	context := map[string]any{}
	if input.Environment != "" {
		context["labels"] = map[string]string{"environment": input.Environment}
	}
	services := make([]string, 0, len(input.Services))
	for _, service := range input.Services {
		if service = strings.TrimSpace(service); service != "" {
			services = append(services, service)
		}
	}
	if len(services) > 0 {
		context["services"] = services
	}
	if input.WindowStart != "" {
		start, err := time.Parse(time.RFC3339, input.WindowStart)
		if err != nil {
			return nil, errors.New(a.tr("start time must be RFC3339", "开始时间必须为 RFC3339"))
		}
		window := map[string]any{"start": start.Format(time.RFC3339)}
		if input.WindowEnd != "" {
			end, err := time.Parse(time.RFC3339, input.WindowEnd)
			if err != nil || !end.After(start) {
				return nil, errors.New(a.tr("end time must be after start", "结束时间必须晚于开始时间"))
			}
			window["end"] = end.Format(time.RFC3339)
		}
		context["window"] = window
	}
	if len(input.Attachments) > 0 {
		context["attachments"] = input.Attachments
	}
	if len(context) > 0 {
		body["context"] = context
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/api/sessions/"+url.PathEscape(sessionID)+"/turns", body, &out)
}
