//go:build linux || darwin

package agent

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

type attemptTransport func(*http.Request) (*http.Response, error)

func (transport attemptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return transport(req)
}

func TestAttemptWire(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cacheBad      bool
		result        string
		startLost     bool
		logLost       bool
		finishLost    bool
		finishCommits bool
		finishRejects bool
		cem           bool
		retryClaimZIP bool
		want          []string
		status        Status
		terminal      bool
	}{
		{
			name: "claim and upload retries, committed completion with lost response", retryClaimZIP: true,
			finishLost: true, finishCommits: true,
			want: []string{"claim", "claim", "start", "log", "upload", "upload", "finish", "read"}, status: Finished, terminal: true,
		},
		{
			name: "CEM hands over its credential", cem: true,
			want: []string{"read", "start", "log", "upload", "finish"}, status: Finished, terminal: true,
		},
		{
			name: "cache digest mismatch", cacheBad: true,
			want: []string{"claim", "log", "upload", "fail"}, status: Failed, terminal: true,
		},
		{
			name: "frozen contract rejects a textual duration", result: `{"durationMs":"1.25","count":10,"ok":true}`,
			want: []string{"claim", "start", "log", "upload", "fail"}, status: Failed, terminal: true,
		},
		{
			name: "ambiguous start does not invoke driver", startLost: true,
			want: []string{"claim", "start"}, status: Setup,
		},
		{
			name: "ambiguous log is never appended twice", logLost: true,
			want: []string{"claim", "start", "log", "upload", "fail"}, status: Failed, terminal: true,
		},
		{
			name: "ambiguous completion stays running", finishLost: true,
			want: []string{"claim", "start", "log", "upload", "finish", "read"}, status: Running,
		},
		{
			name: "definite completion rejection is reported as failure", finishRejects: true,
			want: []string{"claim", "start", "log", "upload", "finish", "read", "fail"}, status: Failed, terminal: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const system = "fixture-system"
			const cemToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			environment := "system-fixture + local"
			if tc.cem {
				environment = "cem-fixture + local"
			}
			cache := t.TempDir()
			root := t.TempDir()
			t.Setenv("CHRONOS_ATTEMPT_TOKEN", "should-not-reach-driver")
			t.Setenv("CHRONOS_AUTHORIZATION", "should-not-reach-driver")
			resultJSON := tc.result
			if resultJSON == "" {
				resultJSON = `{"durationMs":1.25,"count":9223372036854775807,"ok":true,"aux":"diagnostic"}`
			}
			driver := []byte("#!/bin/sh\nset -eu\n" +
				"test -z \"${CHRONOS_ATTEMPT_TOKEN:-}\"\ntest -z \"${CHRONOS_AUTHORIZATION:-}\"\n" +
				"test \"$(cat ../artifacts/payload)\" = fixture\n" +
				"cat > driver-input.json\nprintf '%s\\n' 'output from driver' >&2\n" +
				"printf '%s\\n' '" + resultJSON + "'\n")
			payload := []byte("fixture")
			driverDigest := fmt.Sprintf("%x", sha256.Sum256(driver))
			payloadDigest := fmt.Sprintf("%x", sha256.Sum256(payload))
			if tc.cacheBad {
				payload = []byte("corrupt")
			}
			for digest, data := range map[string][]byte{driverDigest: driver, payloadDigest: payload} {
				if err := os.WriteFile(filepath.Join(cache, digest), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			specification := json.RawMessage(fmt.Sprintf(`{"schemaVersion":1,"protocol":"attempt-v1","system":{"id":1,"identifier":%q,"version":"build-1"},"benchmark":{"contract":{"results":{"durationMs":{"type":"number","unit":"ms"},"count":{"type":"integer"},"ok":{"type":"boolean"}}}},"binding":{"id":15,"version":"1.0"},"adapter":"chronos-command@1","artifacts":{"schemaVersion":1,"sourceRevision":"fixture-revision","artifacts":[{"role":"driver","uri":"file:///fixture-driver","sha256":%q,"sizeBytes":%d},{"role":"payload","uri":"file:///fixture-data","sha256":%q,"sizeBytes":%d}]},"environment":%q,"phases":4,"parameters":{"n":9007199254740993},"effectiveParameters":{"workers":9007199254740993},"setup":[]}`,
				system, driverDigest, len(driver), payloadDigest, len(payload), environment))
			status := Setup
			var token string
			var calls []string
			var uploaded []byte
			var claims, uploads int
			transport := attemptTransport(func(req *http.Request) (*http.Response, error) {
				const prefix = "/api/v1/job/"
				recorder := httptest.NewRecorder()
				route := make(map[string]string)
				for _, segment := range strings.Split(strings.TrimPrefix(req.URL.EscapedPath(), prefix), "/") {
					key, value, _ := strings.Cut(segment, "=")
					route[key], _ = url.QueryUnescape(value)
				}
				if route["protocol"] != "attempt-v1" || req.Header.Get("Authorization") != "Bearer gateway" {
					t.Errorf("unexpected protocol or gateway credentials on %s %s", req.Method, req.URL)
				}
				credential := req.Header.Get("X-Chronos-Attempt-Token")
				if len(credential) != 64 || !hex32Pattern.MatchString(credential) || (tc.cem && credential != cemToken) {
					t.Error("request omitted or changed the attempt credential")
				}
				if token == "" {
					token = credential
				} else if token != credential {
					t.Error("requests for one attempt used different credentials")
				}
				switch req.Method {
				case http.MethodGet:
					if route["id"] == "next" {
						claims++
						calls = append(calls, "claim")
						if tc.cem || route["adapter"] != "chronos-command@1" || route["supports"] != system ||
							route["environment"] != "fixture + local" || route["worker"] != "agent:test" {
							t.Errorf("unexpected claim route %v", route)
						}
						if tc.retryClaimZIP && claims == 1 {
							return nil, io.ErrUnexpectedEOF
						}
					} else {
						calls = append(calls, "read")
						if route["id"] != "90" {
							t.Errorf("unexpected attempt read %v", route)
						}
					}
					reply(recorder, 200, "success", map[string]any{
						"id": 90, "repetitionId": 90, "attemptId": 734, "status": status, "system": system,
						"environment": environment, "protocol": "attempt-v1", "specification": specification,
					})
				case http.MethodPatch:
					fields := readForm(t, req)
					if route["id"] != "90" || fields.Has("currentPhase") {
						t.Errorf("unexpected attempt update %v %v", route, fields)
					}
					switch fields.Get("status") {
					case "2":
						calls = append(calls, "start")
						status = Running
						assertObserved(t, fields, driverDigest, payloadDigest)
						if tc.startLost {
							return nil, io.ErrUnexpectedEOF
						}
					case "3":
						calls = append(calls, "finish")
						if strings.TrimSpace(fields.Get("result")) != resultJSON {
							t.Errorf("completion result = %q", fields.Get("result"))
						}
						if tc.finishRejects {
							reply(recorder, 600, "server did not accept the result", nil)
							return recorder.Result(), nil
						}
						if !tc.finishLost || tc.finishCommits {
							status = Finished
						}
						if tc.finishLost {
							return nil, io.ErrUnexpectedEOF
						}
					case "-2":
						calls = append(calls, "fail")
						status = Failed
						if fields.Get("reason") == "" {
							t.Error("failure did not include its reason")
						}
						if tc.cacheBad {
							assertObserved(t, fields, driverDigest, "")
							if !strings.Contains(fields.Get("reason"), "SHA-256 mismatch") {
								t.Errorf("cache error reason = %q", fields.Get("reason"))
							}
						}
					default:
						t.Errorf("unexpected status fields %v", fields)
					}
					reply(recorder, 200, "success", nil)
				case http.MethodPost:
					switch route["action"] {
					case "appendLog":
						calls = append(calls, "log")
						fields := readForm(t, req)
						want := "output from driver"
						if tc.cacheBad {
							want = "SHA-256 mismatch"
						}
						if !strings.Contains(fields.Get("log"), want) {
							t.Errorf("log %q does not contain %q", fields.Get("log"), want)
						}
						if tc.logLost {
							return nil, io.ErrUnexpectedEOF
						}
					case "upload":
						calls = append(calls, "upload")
						uploads++
						body, err := req.MultipartReader()
						if err != nil {
							t.Fatal(err)
						}
						part, err := body.NextPart()
						if err != nil || part.FormName() != "result" || part.FileName() != "result.zip" {
							t.Fatalf("unexpected ZIP part %v %v", part, err)
						}
						zipBytes, err := io.ReadAll(part)
						if err != nil {
							t.Fatal(err)
						}
						if uploads > 1 && !bytes.Equal(zipBytes, uploaded) {
							t.Error("a retry changed the uploaded archive")
						}
						uploaded = zipBytes
						if tc.retryClaimZIP && uploads == 1 {
							return nil, io.ErrUnexpectedEOF
						}
					default:
						t.Errorf("unexpected POST %v", route)
					}
					reply(recorder, 200, "success", nil)
				default:
					t.Errorf("unexpected method %s", req.Method)
				}
				return recorder.Result(), nil
			})
			client, err := NewClient("https://chrono.example.test", ClientOptions{
				Authorization: "Bearer gateway", HTTPClient: &http.Client{Transport: transport},
			})
			if err != nil {
				t.Fatal(err)
			}
			runner, err := NewAttemptRunner(client, AttemptOptions{
				RunnerOptions: RunnerOptions{Environment: "fixture + local", WorkDir: root, Timeout: 5 * time.Second, StatusInterval: time.Hour},
				CacheDir:      cache, Worker: "agent:test", Systems: []string{system},
			})
			if err != nil {
				t.Fatal(err)
			}
			var run *AttemptResult
			if tc.cem {
				run, err = runner.RunCEMJob(context.Background(), 90, cemToken)
			} else {
				run, err = runner.RunOnce(context.Background())
			}
			if run == nil || run.JobID != 90 || run.AttemptID != 734 || run.Status != tc.status || run.TerminalReported != tc.terminal {
				t.Fatalf("run = %+v, error = %v", run, err)
			}
			if (err == nil) != (tc.status == Finished) || !slices.Equal(calls, tc.want) {
				t.Errorf("calls = %v, expected %v, run error = %v", calls, tc.want, err)
			}
			input, err := os.ReadFile(filepath.Join(run.RunDir, "work", "driver-input.json"))
			if tc.cacheBad || tc.startLost {
				if !os.IsNotExist(err) {
					t.Fatal("the driver was invoked without authorization or verified files")
				}
			} else {
				assertDriverInput(t, input, specification, run.RunDir, err)
			}
			if !tc.startLost {
				wantStdout := resultJSON + "\n"
				if tc.cacheBad {
					wantStdout = ""
				}
				assertAttemptZIP(t, uploaded, specification, wantStdout)
			}
		})
	}
}

func assertObserved(t *testing.T, fields url.Values, driver, payload string) {
	t.Helper()
	var env map[string]string
	if err := json.Unmarshal([]byte(fields.Get("environment")), &env); err != nil ||
		env["os"] != runtime.GOOS || env["architecture"] != runtime.GOARCH || env["goVersion"] != runtime.Version() {
		t.Errorf("observed host environment is invalid: %v %v", env, err)
	}
	var artifacts map[string]verifiedArtifact
	if err := json.Unmarshal([]byte(fields.Get("artifacts")), &artifacts); err != nil ||
		artifacts["driver"].SHA256 != driver || artifacts["driver"].SizeBytes <= 0 {
		t.Errorf("invalid artifact evidence %v %v", artifacts, err)
	}
	if payload == "" {
		if _, ok := artifacts["payload"]; ok {
			t.Error("reported a digest for the mismatched cache entry")
		}
	} else if artifacts["payload"] != (verifiedArtifact{SHA256: payload, SizeBytes: 7}) {
		t.Errorf("invalid payload evidence %v", artifacts)
	}
}

func assertDriverInput(t *testing.T, data, specification []byte, dir string, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var input struct {
		RepetitionID  int64
		AttemptID     int64
		Specification json.RawMessage
		ArtifactPaths map[string]string
	}
	if err := json.Unmarshal(data, &input); err != nil || input.RepetitionID != 90 || input.AttemptID != 734 ||
		!bytes.Equal(input.Specification, specification) ||
		input.ArtifactPaths["driver"] != filepath.Join(dir, "artifacts", "driver") ||
		input.ArtifactPaths["payload"] != filepath.Join(dir, "artifacts", "payload") {
		t.Errorf("unexpected driver stdin: %s, %v", data, err)
	}
}

func assertAttemptZIP(t *testing.T, uploaded, specification []byte, stdout string) {
	t.Helper()
	archive, err := zip.NewReader(bytes.NewReader(uploaded), int64(len(uploaded)))
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]string)
	for _, file := range archive.File {
		in, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(in)
		in.Close()
		if err != nil {
			t.Fatal(err)
		}
		got[file.Name] = string(data)
	}
	if len(got) != 4 || got["specification.json"] != string(specification) ||
		got["stdout.json"] != stdout || !strings.Contains(got["observations.json"], `"attemptId": 734`) {
		t.Errorf("unexpected ZIP content %v", got)
	}
}

func TestDriverOutputLimit(t *testing.T) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "head -c 1048577 /dev/zero")
	observation, stdout, stderr, err := runProcess(ctx, cmd, "driver", maxResultBytes)
	if err != nil || !observation.Truncated || len(stdout) != maxResultBytes || len(stderr) != 0 {
		t.Fatalf("expected bounded output, stdout %d, stderr %d, truncated %t, error %v",
			len(stdout), len(stderr), observation.Truncated, err)
	}
}
