// Package agent implements the Chronos Control v1 job protocol and a local runner.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBytes = 4 << 20
	maxResultBytes   = 1 << 20
	maxArtifactBytes = 64 << 20
	maxLogBytes      = 64 << 10
)

// Status is the numeric job status in the v1 protocol.
type Status int

const (
	Failed    Status = -2
	Aborted   Status = -1
	Scheduled Status = 0
	Setup     Status = 1
	Running   Status = 2
	Finished  Status = 3
)

// Phase is the numeric currentPhase in the v1 protocol.
type Phase int

const (
	Prepare Phase = iota + 1
	Warmup
	Execute
	Analyze
	Clean
)

var phaseNames = [...]string{"", "prepare", "warmup", "execute", "analyze", "clean"}

type Job struct {
	ID           int64   `json:"id"`
	Status       Status  `json:"status"`
	Progress     int     `json:"progress"`
	PhaseMask    int     `json:"phases"`
	CurrentPhase Phase   `json:"currentPhase"`
	System       string  `json:"system"`
	Environment  string  `json:"environment"`
	CDL          string  `json:"cdl"`
	Name         string  `json:"name"`
	Description  *string `json:"description"`
	Created      string  `json:"created"`
	Started      *string `json:"started"`
	Finished     *string `json:"finished"`
	UserID       int64   `json:"user"`
	Username     string  `json:"username"`
}

// Includes interprets the v1 phases field as an exclusion bitmask.
func (j Job) Includes(phase Phase) bool {
	return phase >= Prepare && phase <= Clean && j.PhaseMask&(1<<(phase-1)) == 0
}

type APIError struct {
	HTTPStatus int
	Code       int
	Message    string
}

func (e *APIError) Error() string {
	if e.Code == 0 {
		return fmt.Sprintf("Chronos HTTP status %d", e.HTTPStatus)
	}
	return fmt.Sprintf("Chronos HTTP %d, v1 code %d: %q", e.HTTPStatus, e.Code, e.Message)
}

type ClientOptions struct {
	// Authorization is optional gateway authentication, for example "Bearer …".
	Authorization string
	// HTTPClient can supply custom transports and trust roots. Redirects are disabled.
	HTTPClient *http.Client
	// UploadEndpoint optionally specifies an operator-configured HTTP(S) upload
	// endpoint ending in /api/v1/job. Gateway auth is sent only to the base origin.
	UploadEndpoint string
}

type Client struct {
	base          url.URL
	upload        *url.URL
	http          http.Client
	authorization string
}

// NewClient takes the server origin, optionally followed by /api/v1.
func NewClient(base string, options ClientOptions) (*Client, error) {
	root, err := endpoint(base)
	if err != nil {
		return nil, err
	}
	root.Path = strings.TrimRight(root.Path, "/")
	if root.Path != "" && root.Path != "/api/v1" {
		return nil, errors.New("Chronos URL path must be empty or /api/v1")
	}
	root.Path, root.RawPath = "/api/v1", ""
	c := &Client{base: *root, authorization: options.Authorization}
	if options.HTTPClient != nil {
		c.http = *options.HTTPClient
	}
	if c.http.Timeout <= 0 {
		c.http.Timeout = 15 * time.Second
	}
	c.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// Cookies may be attached through a custom transport if the gateway requires them.
	c.http.Jar = nil

	if options.UploadEndpoint != "" {
		c.upload, err = endpoint(options.UploadEndpoint)
		if err != nil {
			return nil, err
		}
		if c.upload.Path != "/api/v1/job" {
			return nil, errors.New("upload endpoint path must be /api/v1/job")
		}
		if root.Scheme == "https" && c.upload.Scheme != "https" {
			return nil, errors.New("upload endpoint must use HTTPS when the Chronos URL uses HTTPS")
		}
		c.upload.RawPath = ""
	}
	return c, nil
}

func endpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("expected an HTTP(S) URL without credentials, query, or fragment")
	}
	return u, nil
}

func (c *Client) jobURL(upload bool, segments ...string) url.URL {
	u := c.base
	u.Path += "/job"
	if upload && c.upload != nil {
		u = *c.upload
	}
	u.RawPath = u.Path + "/" + strings.Join(segments, "/")
	u.Path, _ = url.PathUnescape(u.RawPath)
	return u
}

func defaultEnvironment(environment string) string {
	if environment == "" {
		return "default"
	}
	return environment
}

// Poll returns (nil, nil) for v1 code 601. The queue read does not reserve a job.
func (c *Client) Poll(ctx context.Context, systems []string, environment string) (*Job, error) {
	if len(systems) == 0 {
		return nil, errors.New("at least one supported system is required")
	}
	for _, system := range systems {
		if system == "" || strings.TrimSpace(system) != system || strings.Contains(system, ",") {
			return nil, errors.New("supported system identifiers must be nonempty and comma-free")
		}
	}
	environment = defaultEnvironment(environment)
	u := c.jobURL(false, "id=next", "supports="+url.QueryEscape(strings.Join(systems, ",")),
		"environment="+url.QueryEscape(environment))
	job, err := c.readJob(ctx, u)
	var api *APIError
	if errors.As(err, &api) && api.HTTPStatus == http.StatusOK && api.Code == 601 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if (!slices.Contains(systems, job.System) && !(len(systems) == 1 && systems[0] == "demoall")) ||
		job.Environment != "system-"+environment || job.Status != Scheduled {
		return nil, errors.New("queue returned a job outside the requested system, environment, or scheduled status")
	}
	return job, nil
}

func (c *Client) Job(ctx context.Context, id int64) (*Job, error) {
	if id <= 0 {
		return nil, errors.New("job ID must be positive")
	}
	job, err := c.readJob(ctx, c.jobURL(false, jobID(id)))
	if err == nil && job.ID != id {
		return nil, errors.New("v1 response job ID differs from the requested ID")
	}
	return job, err
}

func (c *Client) readJob(ctx context.Context, u url.URL) (*Job, error) {
	var wire struct {
		Job
		Status *Status `json:"status"`
		Phases *int    `json:"phases"`
	}
	for attempt := 0; ; attempt++ {
		err, retry := c.request(ctx, http.MethodGet, u, "", nil, 0, &wire)
		if err == nil {
			if wire.ID <= 0 || wire.Status == nil || *wire.Status < Failed || *wire.Status > Finished ||
				wire.Phases == nil || *wire.Phases < 0 || *wire.Phases > 31 || wire.System == "" ||
				wire.Environment == "" {
				return nil, errors.New("v1 response has invalid or missing job fields")
			}
			wire.Job.Status, wire.Job.PhaseMask = *wire.Status, *wire.Phases
			return &wire.Job, nil
		}
		if !retry || attempt == 2 {
			return nil, err
		}
		if err := wait(ctx, time.Duration(1<<attempt)*200*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func jobID(id int64) string { return "id=" + strconv.FormatInt(id, 10) }

type JobUpdate struct {
	Status   *Status
	Phase    *Phase
	Progress *int
	Result   json.RawMessage
}

// Update sends one form-encoded PATCH. Phase updates reset progress in v1.
func (c *Client) Update(ctx context.Context, id int64, update JobUpdate) error {
	fields := url.Values{}
	if update.Status != nil {
		if *update.Status < Failed || *update.Status > Finished {
			return errors.New("invalid job status")
		}
		fields.Set("status", strconv.Itoa(int(*update.Status)))
	}
	if update.Phase != nil {
		if *update.Phase < Prepare || *update.Phase > Clean {
			return errors.New("invalid job phase")
		}
		fields.Set("currentPhase", strconv.Itoa(int(*update.Phase)))
	}
	if update.Progress != nil {
		if *update.Progress < 0 || *update.Progress > 100 || update.Phase != nil {
			return errors.New("progress must be 0–100 and updated separately from phase")
		}
		fields.Set("progress", strconv.Itoa(*update.Progress))
	}
	if len(update.Result) != 0 {
		if err := validateResult(update.Result); err != nil {
			return err
		}
		fields.Set("result", string(update.Result))
	}
	if len(fields) == 0 {
		return errors.New("job update is empty")
	}
	return c.form(ctx, http.MethodPatch, id, "", fields, nil)
}

func validateResult(raw []byte) error {
	value := bytes.TrimSpace(raw)
	if len(raw) > maxResultBytes || !json.Valid(value) || (value[0] != '{' && value[0] != '[') {
		return errors.New("result must be a JSON object or array up to 1 MiB")
	}
	return nil
}

// AppendLog sends one v1 log attachment. Callers can split longer logs.
func (c *Client) AppendLog(ctx context.Context, id int64, log string) error {
	if log == "" || log == "0" || len(log) > maxLogBytes {
		return errors.New("log must contain 1–65536 bytes and cannot be the literal 0")
	}
	return c.form(ctx, http.MethodPost, id, "appendLog", url.Values{"log": {log}}, nil)
}

type UploadTarget struct {
	Method   string          `json:"method"`
	Hostname string          `json:"hostname"`
	Path     string          `json:"path"`
	Port     json.RawMessage `json:"port"`
	Username string          `json:"username"`
	Password string          `json:"password"`
	Filename string          `json:"filename"`
}

// UploadTarget inspects the provisioned target without contacting it.
func (c *Client) UploadTarget(ctx context.Context, id int64) (UploadTarget, error) {
	var target UploadTarget
	err := c.form(ctx, http.MethodPost, id, "getUploadTarget", nil, &target)
	return target, err
}

func (c *Client) form(ctx context.Context, method string, id int64, action string, fields url.Values, out any) error {
	if id <= 0 {
		return errors.New("job ID must be positive")
	}
	segments := []string{jobID(id)}
	if action != "" {
		segments = append(segments, "action="+action)
	}
	body := strings.NewReader(fields.Encode())
	err, _ := c.request(ctx, method, c.jobURL(false, segments...), "application/x-www-form-urlencoded", body, int64(body.Len()), out)
	return err
}

// UploadZIP streams one result ZIP into the v1 multipart "result" field.
// The reader must provide exactly size bytes.
func (c *Client) UploadZIP(ctx context.Context, id int64, name string, archive io.Reader, size int64) error {
	if id <= 0 || archive == nil || size <= 0 || size > maxArtifactBytes+(4<<20) {
		return errors.New("invalid job ID or ZIP size")
	}
	if filepath.Base(name) != name || !strings.HasSuffix(name, ".zip") ||
		strings.ContainsAny(name, "\r\n\x00") {
		return errors.New("expected a ZIP filename without path components or controls")
	}
	target := c.jobURL(true, jobID(id), "action=upload")
	err, _ := c.uploadZIP(ctx, target, "", name, archive, size)
	return err
}

func (c *Client) uploadZIP(ctx context.Context, target url.URL, token, name string, archive io.Reader, size int64) (error, bool) {
	var framing bytes.Buffer
	multipartWriter := multipart.NewWriter(&framing)
	if _, err := multipartWriter.CreateFormFile("result", name); err != nil {
		return err, false
	}
	prefixSize := framing.Len()
	if err := multipartWriter.Close(); err != nil {
		return err, false
	}
	frame := framing.Bytes()
	body := io.MultiReader(bytes.NewReader(frame[:prefixSize]), io.LimitReader(archive, size), bytes.NewReader(frame[prefixSize:]))
	return c.requestWithToken(ctx, http.MethodPost, target, token,
		multipartWriter.FormDataContentType(), body, int64(len(frame))+size, nil)
}

func (c *Client) request(ctx context.Context, method string, target url.URL, contentType string, body io.Reader, size int64, out any) (error, bool) {
	return c.requestWithToken(ctx, method, target, "", contentType, body, size, out)
}

func (c *Client) requestWithToken(ctx context.Context, method string, target url.URL, token, contentType string, body io.Reader, size int64, out any) (error, bool) {
	// Empty POST bodies and form bodies share this path. The reader wrapper
	// prevents replay of a mutation through Request.GetBody.
	if size == 0 {
		body = nil
	} else if method != http.MethodGet {
		body = struct{ io.Reader }{body}
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return err, false
	}
	req.ContentLength = size
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if target.Scheme == c.base.Scheme && strings.EqualFold(target.Host, c.base.Host) {
		if c.authorization != "" {
			req.Header.Set("Authorization", c.authorization)
		}
		if token != "" {
			req.Header.Set("X-Chronos-Attempt-Token", token)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err, ctx.Err() == nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		retry := resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504
		return &APIError{HTTPStatus: resp.StatusCode}, retry
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read v1 response: %w", err), ctx.Err() == nil
	}
	if len(data) > maxResponseBytes {
		return errors.New("v1 response exceeds 4 MiB"), false
	}
	var envelope struct {
		Status struct {
			Code    int     `json:"code"`
			Message *string `json:"message"`
		} `json:"status"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Status.Code == 0 || envelope.Status.Message == nil {
		return errors.New("invalid v1 response envelope"), false
	}
	if envelope.Status.Code != 200 {
		message := *envelope.Status.Message
		if len(message) > 512 {
			message = message[:512]
		}
		return &APIError{HTTPStatus: 200, Code: envelope.Status.Code, Message: message}, false
	}
	if out != nil {
		response := bytes.TrimSpace(envelope.Response)
		if len(response) == 0 || response[0] != '{' || json.Unmarshal(response, out) != nil {
			return errors.New("invalid v1 response payload"), false
		}
	}
	return nil, false
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
