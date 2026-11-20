package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	attemptProtocol = "attempt-v1"
	commandAdapter  = "chronos-command@1"
)

type attemptJob struct {
	ID            int64           `json:"id"`
	RepetitionID  int64           `json:"repetitionId"`
	AttemptID     int64           `json:"attemptId"`
	Status        *Status         `json:"status"`
	System        string          `json:"system"`
	Environment   string          `json:"environment"`
	Protocol      string          `json:"protocol"`
	Specification json.RawMessage `json:"specification"`
}

type attemptClient struct {
	client *Client
	token  string
}

func (a attemptClient) read(ctx context.Context, segments ...string) (*attemptJob, error) {
	u := a.client.jobURL(false, append(segments, "protocol="+attemptProtocol)...)
	for retry := 0; ; retry++ {
		var job attemptJob
		err, transient := a.client.requestWithToken(ctx, http.MethodGet, u, a.token, "", nil, 0, &job)
		if err == nil {
			spec := bytes.TrimSpace(job.Specification)
			if job.ID <= 0 || job.ID != job.RepetitionID || job.AttemptID <= 0 || job.Protocol != attemptProtocol ||
				job.Status == nil || *job.Status < Failed || *job.Status == Scheduled || *job.Status > Finished ||
				job.System == "" || job.Environment == "" || len(spec) == 0 || spec[0] != '{' {
				return nil, errors.New("invalid attempt-v1 job response")
			}
			return &job, nil
		}
		if retry == 2 || !retryAttemptRequest(err, transient) {
			return nil, err
		}
		if err := wait(ctx, time.Duration(1<<retry)*200*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func (a attemptClient) current(ctx context.Context, expected *attemptJob) (*attemptJob, error) {
	job, err := a.read(ctx, jobID(expected.ID))
	if err != nil {
		return nil, err
	}
	if job.ID != expected.ID || job.AttemptID != expected.AttemptID ||
		job.System != expected.System || job.Environment != expected.Environment ||
		!bytes.Equal(job.Specification, expected.Specification) {
		return nil, fmt.Errorf("%w: attempt identity or frozen specification differs", ErrJobStateChanged)
	}
	return job, nil
}

func (a attemptClient) form(ctx context.Context, method string, job *attemptJob, action string, fields url.Values) error {
	segments := []string{jobID(job.ID), "protocol=" + attemptProtocol}
	if action != "" {
		segments = append(segments, "action="+action)
	}
	body := strings.NewReader(fields.Encode())
	err, _ := a.client.requestWithToken(ctx, method, a.client.jobURL(false, segments...), a.token,
		"application/x-www-form-urlencoded", body, int64(body.Len()), nil)
	return err
}

func (a attemptClient) upload(ctx context.Context, job *attemptJob, path string) error {
	archive, err := os.Open(path)
	if err != nil {
		return err
	}
	defer archive.Close()
	stat, err := archive.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	target := a.client.jobURL(false, jobID(job.ID), "protocol="+attemptProtocol, "action=upload")
	for retry := 0; ; retry++ {
		// Core makes uploads idempotent by matching the ZIP's bytes.
		err, transient := a.client.uploadZIP(ctx, target, a.token, zipName, io.NewSectionReader(archive, 0, size), size)
		if err == nil || retry == 1 || !retryAttemptRequest(err, transient) {
			return err
		}
		if err := wait(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
}

// A corrupt proxy response can obscure a successful claim or upload.
// Only operations with an idempotent server-side identity use this retry.
func retryAttemptRequest(err error, transient bool) bool {
	var api *APIError
	return transient || !errors.As(err, &api)
}
