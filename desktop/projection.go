package main

import (
	"errors"
	"net/url"

	"go-sre-harness/internal/projection"
)

// GetRunProjection returns the ordered blocks the conversation renders for one run.
func (a *App) GetRunProjection(runID string) (projection.View, error) {
	if runID == "" {
		return projection.View{}, errors.New("run id is required")
	}
	c, err := a.c()
	if err != nil {
		return projection.View{}, err
	}
	var out projection.View
	err = c.Get(a.ctx, "/api/runs/"+url.PathEscape(runID)+"/projection", &out)
	return out, err
}
