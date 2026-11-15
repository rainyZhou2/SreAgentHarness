package revocation

import (
	"context"

	"go-sre-harness/internal/security"
)

type StampReader interface {
	Stamps(ctx context.Context, query Query) ([]security.RevocationStamp, error)
}

type RequestReader struct {
	Reader StampReader
}

func (r RequestReader) Stamps(ctx context.Context, request security.Request) ([]security.RevocationStamp, error) {
	return r.Reader.Stamps(ctx, Query{TenantID: request.TenantID, Subject: request.Subject, WorkID: request.WorkID, RunID: request.RunID})
}
