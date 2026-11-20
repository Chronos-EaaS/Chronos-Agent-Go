//go:build linux || darwin

package agent

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const syntheticPayload = "A&B + Zürich $(touch injected-by-cdl)"
const syntheticCDL = `<?xml version="1.0" encoding="utf-8"?>
<chronos><setup/><evaluation system="1"><payload>A&amp;B + Zürich $(touch injected-by-cdl)</payload></evaluation></chronos>`

type fakeControl struct {
	t          *testing.T
	mu         sync.Mutex
	job        Job
	polls      int
	calls      []string
	log        strings.Builder
	result     json.RawMessage
	zip        []byte
	failUpload bool
	forceAbort func() bool
}

func newControl(t *testing.T) *fakeControl {
	return &fakeControl{t: t, job: Job{
		ID: 90, System: "fixture-system", Environment: "system-agent",
		Status: Scheduled, PhaseMask: 2, CDL: syntheticCDL,
	}}
}

func (f *fakeControl) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	const jobPath = "/api/v1/job/id=90"
	where := req.URL.Path
	if req.Method == http.MethodGet {
		switch where {
		case "/api/v1/job/id=next/supports=fixture-system/environment=agent":
			f.polls++
			if f.job.Status != Scheduled {
				reply(w, 601, "No job in queue!", nil)
				return
			}
		case jobPath:
			if f.job.Status == Running && f.forceAbort != nil && f.forceAbort() {
				f.job.Status = Aborted
			}
		default:
			f.t.Errorf("unexpected GET %s", where)
			http.NotFound(w, req)
			return
		}
		reply(w, 200, "success", f.job)
		return
	}
	switch {
	case req.Method == http.MethodPatch && where == jobPath:
		fields := readForm(f.t, req)
		f.calls = append(f.calls, "PATCH "+fields.Encode())
		for name := range fields {
			switch name {
			case "status":
				status, _ := strconv.Atoi(fields.Get(name))
				f.job.Status = Status(status)
			case "currentPhase":
				phase, _ := strconv.Atoi(fields.Get(name))
				f.job.CurrentPhase = Phase(phase)
			case "progress":
				f.job.Progress, _ = strconv.Atoi(fields.Get(name))
			case "result":
				f.result = json.RawMessage(fields.Get(name))
			default:
				f.t.Errorf("unexpected PATCH field %s", name)
			}
		}
		if fields.Has("currentPhase") {
			f.job.Progress = 0
		}
	case req.Method == http.MethodPost && where == jobPath+"/action=appendLog":
		fields := readForm(f.t, req)
		f.calls = append(f.calls, "LOG")
		if len(fields) != 1 || fields.Get("log") == "" {
			f.t.Error("appendLog must carry one nonempty log field")
		}
		f.log.WriteString(fields.Get("log"))
	case req.Method == http.MethodPost && where == jobPath+"/action=upload":
		f.calls = append(f.calls, "UPLOAD")
		f.receiveZIP(req)
		if f.failUpload {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	default:
		f.t.Errorf("unexpected request %s %s", req.Method, where)
		http.NotFound(w, req)
		return
	}
	reply(w, 200, "success", nil)
}

func readForm(t *testing.T, r *http.Request) url.Values {
	t.Helper()
	if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
		t.Errorf("expected URL-encoded body: %s", r.Header.Get("Content-Type"))
	}
	if err := r.ParseForm(); err != nil {
		t.Error(err)
	}
	return r.PostForm
}

func (f *fakeControl) receiveZIP(req *http.Request) {
	reader, err := req.MultipartReader()
	if err != nil {
		f.t.Error(err)
		return
	}
	part, err := reader.NextPart()
	if err != nil || part.FormName() != "result" || part.FileName() != "result.zip" {
		f.t.Error("expected multipart result ZIP", err)
		return
	}
	f.zip, err = io.ReadAll(part)
	if err != nil {
		f.t.Error(err)
	}
	if _, err := reader.NextPart(); err != io.EOF {
		f.t.Error("expected exactly one part:", err)
	}
}

func startRunner(t *testing.T, fake *fakeControl, adapter Adapter, timeout, interval time.Duration, root string) *Runner {
	t.Helper()
	client := clientFor(t, fake.ServeHTTP, ClientOptions{})
	runner, err := NewRunner(client, map[string]Adapter{"fixture-system": adapter}, RunnerOptions{
		Environment: "agent", WorkDir: root, Timeout: timeout, StatusInterval: interval,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestSyntheticCLI(t *testing.T) {
	root := t.TempDir()
	fake := newControl(t)
	server := httptest.NewServer(fake)
	defer server.Close()

	binary := filepath.Join(root, "chronos-agent")
	build := exec.Command("go", "build", "-o", binary, "./cmd/chronos-agent")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, output)
	}
	example, err := filepath.Abs("examples/synthetic")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", example+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CHRONOS_AUTHORIZATION", "")
	runs := filepath.Join(root, "runs")
	args := []string{"-url", server.URL, "-config", filepath.Join(example, "adapter.json"),
		"-environment", "agent", "-work-dir", runs, "-once"}
	output, err := exec.Command(binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("CLI: %v\n%s", err, output)
	}
	empty, err := exec.Command(binary, args...).CombinedOutput()
	if err != nil || string(empty) != "No job in queue.\n" {
		t.Fatalf("empty CLI poll: %v, %s", err, empty)
	}
	t.Log(strings.TrimSpace(string(output)))
	t.Log(strings.TrimSpace(string(empty)))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	expected := []string{"PATCH progress=0&status=2", "PATCH currentPhase=3", "PATCH progress=100", "LOG",
		"UPLOAD", "PATCH result=%7B%22records%22%3A1%7D%0A&status=3"}
	if !reflect.DeepEqual(fake.calls, expected) || fake.job.Status != Finished || fake.job.Progress != 100 || fake.polls != 2 ||
		!strings.Contains(fake.log.String(), syntheticPayload) || !strings.Contains(fake.log.String(), "chronos-synthetic-benchmark 1") {
		t.Fatalf("unexpected lifecycle: calls=%v job=%+v polls=%d log=%q", fake.calls, fake.job, fake.polls, fake.log.String())
	}
	files := zipContents(t, fake.zip)
	if got := string(files["artifacts/payload.txt"]); got != syntheticPayload+"\n" {
		t.Fatalf("literal argument = %q", got)
	}
	if !bytes.Equal(fake.result, files["artifacts/metrics.json"]) {
		t.Fatal("PATCH and ZIP result bytes differ")
	}
	var manifest manifest
	if err := json.Unmarshal(files[manifestName], &manifest); err != nil || manifest.SchemaVersion != 1 || len(manifest.Artifacts) != 3 {
		t.Fatalf("manifest: %+v, %v", manifest, err)
	}
	for _, entry := range manifest.Artifacts {
		data, exists := files[entry.URI]
		if !exists || int64(len(data)) != entry.SizeBytes || fmt.Sprintf("%x", sha256.Sum256(data)) != entry.SHA256 {
			t.Fatalf("digest or size mismatch for %+v", entry)
		}
	}
	var run observations
	if err := json.Unmarshal(files[evidenceName], &run); err != nil || len(run.Commands) != 2 ||
		run.Commands[1].Argv[1] != syntheticPayload || run.Commands[1].ExitCode == nil || *run.Commands[1].ExitCode != 0 ||
		run.JobID != 90 || run.GoVersion == "" || run.OS == "" || run.Host == "" || run.CDLSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(syntheticCDL))) {
		t.Fatalf("observations: %+v, %v", run, err)
	}
	work := filepath.Join(runs, run.LocalRun, "work")
	if _, err := os.Stat(filepath.Join(work, "injected-by-cdl")); !os.IsNotExist(err) {
		t.Fatalf("unexpected command-substitution file: %v", err)
	}
}

func zipContents(t *testing.T, data []byte) map[string][]byte {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte)
	for _, file := range reader.File {
		input, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(input)
		input.Close()
		if err != nil {
			t.Fatal(err)
		}
		files[file.Name] = body
	}
	return files
}

func TestRunnerHonorsPhaseMaskAndCEMHandoff(t *testing.T) {
	fake := newControl(t)
	fake.job.Status, fake.job.Environment = Setup, "cem-agent"
	adapter := Adapter{Phases: map[string]Command{
		"prepare": {Program: "/bin/echo", Args: []string{"prepare"}},
		"warmup":  {Program: "/bin/sh", Args: []string{"-c", "exit 42"}},
		"execute": {Program: "/bin/echo", Args: []string{"execute"}},
		"clean":   {Program: "/bin/echo", Args: []string{"clean"}},
	}}
	runner := startRunner(t, fake, adapter, 5*time.Second, time.Hour, t.TempDir())
	result, err := runner.RunCEMJob(context.Background(), 90)
	if err != nil || result.Status != Finished || !result.TerminalReported {
		t.Fatalf("CEM handoff: %+v, %v", result, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	expected := []string{"PATCH progress=0&status=2", "PATCH currentPhase=1", "PATCH progress=100",
		"PATCH currentPhase=3", "PATCH progress=100", "PATCH currentPhase=5", "PATCH progress=100",
		"LOG", "UPLOAD", "PATCH status=3"}
	if fake.polls != 0 || !reflect.DeepEqual(fake.calls, expected) {
		t.Fatalf("CEM lifecycle: %v", fake.calls)
	}
}

func TestRunnerReportsExitAndMalformedCDL(t *testing.T) {
	for _, tc := range []struct {
		name, cdl string
		exit      *int
	}{
		{"nonzero exit", syntheticCDL, newInt(7)},
		{"bad CDL", "<chronos><setup>", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newControl(t)
			fake.job.CDL = tc.cdl
			adapter := Adapter{Phases: map[string]Command{
				"execute": {Program: "/bin/sh", Args: []string{"-c", "printf 'synthetic error\\n' >&2; exit 7"}},
			}}
			runner := startRunner(t, fake, adapter, 5*time.Second, time.Hour, t.TempDir())
			result, err := runner.RunOnce(context.Background())
			if err == nil || result.Status != Failed || !result.TerminalReported {
				t.Fatalf("failed job: %+v, %v", result, err)
			}
			var process *ProcessError
			if tc.exit != nil && (!errors.As(err, &process) || process.ExitCode == nil || *process.ExitCode != *tc.exit) {
				t.Fatalf("exit not visible: %v", err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.zip) != 0 || !strings.Contains(fake.log.String(), "[agent]") ||
				fake.calls[len(fake.calls)-1] != "PATCH status=-2" {
				t.Fatalf("failure lifecycle: %v, log %q", fake.calls, fake.log.String())
			}
			run := readObservation(t, result.RunDir)
			if tc.exit == nil && len(run.Commands) != 0 {
				t.Fatalf("malformed CDL ran commands: %+v", run.Commands)
			}
			if tc.exit != nil && (len(run.Commands) != 1 || *run.Commands[0].ExitCode != *tc.exit ||
				!strings.Contains(fake.log.String(), "synthetic error")) {
				t.Fatalf("missing observed exit: %+v", run.Commands)
			}
		})
	}
}

func newInt(n int) *int { return &n }

func readObservation(t *testing.T, dir string) observations {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, evidenceName))
	if err != nil {
		t.Fatal(err)
	}
	var run observations
	if err := json.Unmarshal(data, &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestRunnerTimeoutKillsProcessGroup(t *testing.T) {
	fake := newControl(t)
	adapter := Adapter{Phases: map[string]Command{
		"execute": {Program: "/bin/sh", Args: []string{"-c", "(/bin/sleep 1 && touch wrote-after-timeout) & /bin/sleep 10"}},
	}}
	runner := startRunner(t, fake, adapter, 400*time.Millisecond, time.Hour, t.TempDir())
	result, err := runner.RunOnce(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != Failed || !result.TerminalReported {
		t.Fatalf("timeout job: %+v, %v", result, err)
	}
	run := readObservation(t, result.RunDir)
	if len(run.Commands) != 1 || run.Commands[0].ExitCode == nil || *run.Commands[0].ExitCode == 0 {
		t.Fatalf("timeout process state: %+v", run.Commands)
	}
	time.Sleep(850 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(result.RunDir, "work", "wrote-after-timeout")); !os.IsNotExist(err) {
		log, _ := os.ReadFile(filepath.Join(result.RunDir, logName))
		t.Fatalf("child remained active: %v\n%s", err, log)
	}
}

func TestRunnerRemoteCancellationPreservesEvidence(t *testing.T) {
	fake := newControl(t)
	root := t.TempDir()
	fake.forceAbort = func() bool {
		ready, _ := filepath.Glob(filepath.Join(root, "job-*", "work", "ready"))
		return len(ready) > 0
	}
	adapter := Adapter{Phases: map[string]Command{
		"execute": {Program: "/bin/sh", Args: []string{"-c", "touch ready; sleep 10"}},
	}}
	runner := startRunner(t, fake, adapter, 5*time.Second, 30*time.Millisecond, root)
	result, err := runner.RunOnce(context.Background())
	if !errors.Is(err, ErrJobStateChanged) || result.TerminalReported || result.Status != Aborted {
		t.Fatalf("remote cancellation: %+v, %v", result, err)
	}
	run := readObservation(t, result.RunDir)
	if len(run.Commands) != 1 || run.Commands[0].ExitCode == nil || *run.Commands[0].ExitCode == 0 {
		t.Fatalf("canceled process state: %+v", run.Commands)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !reflect.DeepEqual(fake.calls, []string{"PATCH progress=0&status=2", "PATCH currentPhase=3"}) ||
		fake.job.Status != Aborted {
		t.Fatalf("cancellation changed terminal status: %v, %d", fake.calls, fake.job.Status)
	}
}

func TestRunnerUploadFailureStopsWithoutReexecution(t *testing.T) {
	fake := newControl(t)
	fake.failUpload = true
	adapter := Adapter{Phases: map[string]Command{"execute": {Program: "/bin/echo", Args: []string{"done"}}}}
	runner := startRunner(t, fake, adapter, 5*time.Second, time.Hour, t.TempDir())
	result, err := runner.RunOnce(context.Background())
	if err == nil || result.TerminalReported || result.Status != Running {
		t.Fatalf("unconfirmed upload: %+v, %v", result, err)
	}
	run := readObservation(t, result.RunDir)
	if len(run.Commands) != 1 || run.Commands[0].ExitCode == nil || *run.Commands[0].ExitCode != 0 {
		t.Fatalf("expected one completed command despite publication error: %+v", run.Commands)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := []string{"PATCH progress=0&status=2", "PATCH currentPhase=3", "PATCH progress=100", "LOG", "UPLOAD"}
	if !reflect.DeepEqual(fake.calls, want) {
		t.Fatalf("unexpected upload retry or final PATCH: %v", fake.calls)
	}
	local, err := os.ReadFile(filepath.Join(result.RunDir, zipName))
	if err != nil || !bytes.Equal(local, fake.zip) {
		t.Fatalf("local ZIP differs from uploaded bytes: %v", err)
	}
}
