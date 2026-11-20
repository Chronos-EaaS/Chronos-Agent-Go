package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func reply(w http.ResponseWriter, code int, message string, response any) {
	w.Header().Set("Content-Type", "application/json")
	payload := map[string]any{"status": map[string]any{"code": code, "message": message}}
	if response != nil {
		payload["response"] = response
	}
	_ = json.NewEncoder(w).Encode(payload)
}

func clientFor(t *testing.T, handler http.HandlerFunc, options ClientOptions) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestPollWireAndQueueEmpty(t *testing.T) {
	var calls atomic.Int32
	client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RawQuery != "" {
			t.Error("expected path-based GET:", r.Method, r.URL)
		}
		want := []string{"api", "v1", "job", "id=next", "supports=first,second + value", "environment=linux + 1"}
		segments := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/"), "/")
		for i, segment := range segments {
			segments[i], _ = url.QueryUnescape(segment)
		}
		if !reflect.DeepEqual(want, segments) {
			t.Errorf("path segments = %#v, want %#v", segments, want)
		}
		if calls.Add(1) == 2 {
			reply(w, 601, "No job in queue!", nil)
			return
		}
		reply(w, 200, "success", Job{
			ID: 90, Status: Scheduled, System: "first", Environment: "system-linux + 1", PhaseMask: 2,
		})
	}, ClientOptions{})
	systems := []string{"first", "second + value"}
	ctx := context.Background()
	job, err := client.Poll(ctx, systems, "linux + 1")
	if err != nil || job == nil || job.ID != 90 || !job.Includes(Execute) || job.Includes(Warmup) {
		t.Fatalf("poll: %+v, %v", job, err)
	}
	job, err = client.Poll(ctx, systems, "linux + 1")
	if job != nil || err != nil || calls.Load() != 2 {
		t.Fatalf("empty poll: %+v, %v, %d calls", job, err, calls.Load())
	}
}

func TestClientErrorContracts(t *testing.T) {
	for _, tc := range []struct {
		name string
		http int
		body string
		code int
	}{
		{"missing job", 200, `{"status":{"code":602,"message":"Job does not exist!"}}`, 602},
		{"unknown system", 200, `{"status":{"code":600,"message":"Unknown system"}}`, 600},
		{"gateway unauthorized", 401, `{"message":"Unauthorized"}`, 0},
		{"gateway forbidden", 403, `{"message":"Forbidden"}`, 0},
		{"malformed JSON", 200, `<html>Proxy login</html>`, -1},
		{"trailing JSON", 200, `{"status":{"code":200,"message":"success"}}{}`, -1},
		{"array payload", 200, `{"status":{"code":200,"message":"success"},"response":[]}`, -1},
		{"absent status metadata", 200, `{"status":{"code":200}}`, -1},
		{"UI envelope", 200, `{"status":{"code":200,"message":"success"},"data":{"id":90}}`, -1},
		{"status absent in job", 200, `{"status":{"code":200,"message":"success"},"response":{"id":90,"phases":0,"system":"fixture","environment":"system-default"}}`, -1},
		{"oversize response", 200, strings.Repeat(" ", maxResponseBytes+1), -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") != "Bearer synthetic-gateway-token" {
					t.Error("expected gateway Authorization header")
				}
				w.WriteHeader(tc.http)
				fmt.Fprint(w, tc.body)
			}, ClientOptions{Authorization: "Bearer synthetic-gateway-token"})
			_, err := client.Job(context.Background(), 90)
			var api *APIError
			if err == nil || (tc.code >= 0 && (!errors.As(err, &api) || api.Code != tc.code || api.HTTPStatus != tc.http)) ||
				(tc.code < 0 && errors.As(err, &api)) || calls.Load() != 1 {
				t.Fatalf("error = %v, calls = %d", err, calls.Load())
			}
		})
	}
}

func TestReadRetriesButWritesDoNot(t *testing.T) {
	var reads, writes atomic.Int32
	client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if reads.Add(1) == 2 {
				reply(w, 200, "success", Job{ID: 90, System: "fixture", Environment: "system-default"})
				return
			}
		case http.MethodPatch:
			writes.Add(1)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}, ClientOptions{})
	ctx := context.Background()
	if _, err := client.Job(ctx, 90); err != nil || reads.Load() != 2 {
		t.Fatalf("reads=%d, err=%v", reads.Load(), err)
	}
	status := Running
	err := client.Update(ctx, 90, JobUpdate{Status: &status})
	var api *APIError
	if !errors.As(err, &api) || api.HTTPStatus != 503 || writes.Load() != 1 {
		t.Fatalf("writes=%d, err=%v", writes.Load(), err)
	}
}

func TestClientTimeoutAndContext(t *testing.T) {
	client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}, ClientOptions{HTTPClient: &http.Client{Timeout: 40 * time.Millisecond}})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := client.Job(ctx, 90)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
}

func TestCrossOriginUploadAndRedirectKeepCredentialsLocal(t *testing.T) {
	var redirects atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirects.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("gateway authorization leaked to the upload origin")
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/job/id=90/action=upload" {
			t.Error("unexpected upload request", r.Method, r.URL.Path)
		}
		if r.ContentLength <= 0 {
			t.Error("expected multipart content length")
		}
		form, err := r.MultipartReader()
		if err != nil {
			t.Error(err)
		} else if part, err := form.NextPart(); err != nil || part.FormName() != "result" || part.FileName() != "result.zip" {
			t.Error("unexpected multipart field", err)
		}
		reply(w, 200, "success", nil)
	}))
	defer sink.Close()
	client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL, http.StatusTemporaryRedirect)
	}, ClientOptions{Authorization: "Bearer synthetic-gateway-token", UploadEndpoint: sink.URL + "/api/v1/job"})
	ctx := context.Background()
	if err := client.AppendLog(ctx, 90, "marker"); err == nil || redirects.Load() != 0 {
		t.Fatal("unexpected redirect following or mutation success:", err)
	}
	if err := client.UploadZIP(ctx, 90, "result.zip", strings.NewReader("synthetic"), 9); err != nil || redirects.Load() != 1 {
		t.Fatalf("upload: %v, external requests %d", err, redirects.Load())
	}
}

func TestUploadTargetReturnsHTTPContractWithoutContact(t *testing.T) {
	client := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/job/id=90/action=getUploadTarget" {
			t.Error("unexpected target request", r.Method, r.URL.Path)
		}
		reply(w, 200, "success", map[string]string{"method": "http", "path": "/api/v1/job", "hostname": "http://127.0.0.1:8088"})
	}, ClientOptions{})
	target, err := client.UploadTarget(context.Background(), 90)
	if err != nil || target.Method != "http" || target.Path != "/api/v1/job" || target.Hostname != "http://127.0.0.1:8088" {
		t.Fatalf("target: %+v, %v", target, err)
	}
}
