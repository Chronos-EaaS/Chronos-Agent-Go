package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var attemptWorkerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$`)

// AttemptOptions configures an opt-in chronos-command@1 worker. An empty
// Systems list accepts bound jobs from any system in the selected environment.
type AttemptOptions struct {
	RunnerOptions
	Worker   string
	CacheDir string
	Systems  []string
}

// AttemptRunner claims managed repetitions and executes their verified drivers.
type AttemptRunner struct {
	execution sync.Mutex
	client    *Client
	options   AttemptOptions
}

// AttemptResult identifies the attempt and its local evidence. TerminalReported
// is true when a terminal update was acknowledged or confirmed by a read.
type AttemptResult struct {
	RunResult
	AttemptID int64
}

// NewAttemptRunner validates the worker configuration and prepares the local run directory.
func NewAttemptRunner(client *Client, options AttemptOptions) (*AttemptRunner, error) {
	if client == nil || options.WorkDir == "" || options.CacheDir == "" ||
		options.Timeout <= 0 || options.StatusInterval <= 0 {
		return nil, errors.New("attempt runner requires a client, working directory, artifact cache, timeout, and status interval")
	}
	if client.upload != nil {
		return nil, errors.New("attempt-v1 uses the configured Chronos origin for archive uploads")
	}
	if options.Worker == "" {
		options.Worker, _ = os.Hostname()
	}
	if !attemptWorkerPattern.MatchString(options.Worker) {
		return nil, errors.New("set a worker identifier of 1–100 letters, digits, periods, underscores, colons, or hyphens beginning with a letter or digit")
	}
	if strings.TrimSpace(options.Environment) != options.Environment {
		return nil, errors.New("environment must not start or end with whitespace")
	}
	options.Environment = defaultEnvironment(options.Environment)
	if len(options.Systems) > 100 {
		return nil, errors.New("attempt runner supports up to 100 system identifiers")
	}
	for _, system := range options.Systems {
		if system == "" || strings.TrimSpace(system) != system || strings.Contains(system, ",") {
			return nil, errors.New("supported system identifiers must be nonempty and comma-free")
		}
	}
	if len(options.Systems) == 1 && options.Systems[0] == "demoall" {
		options.Systems = nil
	} else {
		options.Systems = slices.Clone(options.Systems)
	}
	var err error
	options.CacheDir, err = filepath.Abs(options.CacheDir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(options.CacheDir); err != nil || !info.IsDir() {
		return nil, errors.New("artifact cache must be an existing directory")
	}
	options.WorkDir, err = filepath.Abs(options.WorkDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.WorkDir, 0o700); err != nil {
		return nil, err
	}
	return &AttemptRunner{client: client, options: options}, nil
}

// RunOnce claims and executes one compatible repetition. Each new claim uses
// a fresh credential. Retries of that claim use the same credential.
func (r *AttemptRunner) RunOnce(ctx context.Context) (*AttemptResult, error) {
	if !r.execution.TryLock() {
		return nil, errors.New("attempt runner is already handling a job")
	}
	defer r.execution.Unlock()
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return nil, fmt.Errorf("generate attempt credential: %w", err)
	}
	client := attemptClient{r.client, hex.EncodeToString(token[:])}
	segments := []string{"id=next", "environment=" + url.QueryEscape(r.options.Environment),
		"adapter=" + url.QueryEscape(commandAdapter), "worker=" + url.QueryEscape(r.options.Worker)}
	if len(r.options.Systems) != 0 {
		segments = append(segments, "supports="+url.QueryEscape(strings.Join(r.options.Systems, ",")))
	}
	job, err := client.read(ctx, segments...)
	var api *APIError
	if errors.As(err, &api) && api.HTTPStatus == http.StatusOK && api.Code == 601 {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim attempt: %w", err)
	}
	if job.Environment != "system-"+r.options.Environment ||
		(len(r.options.Systems) != 0 && !slices.Contains(r.options.Systems, job.System)) {
		return nil, errors.New("claim returned an attempt outside the requested systems or environment")
	}
	return r.run(ctx, client, job)
}

// RunCEMJob executes a setup-state repetition already reserved by a CEM
// bootstrapper. It uses the credential issued for that reservation.
func (r *AttemptRunner) RunCEMJob(ctx context.Context, id int64, token string) (*AttemptResult, error) {
	if !r.execution.TryLock() {
		return nil, errors.New("attempt runner is already handling a job")
	}
	defer r.execution.Unlock()
	if id <= 0 || !hex32Pattern.MatchString(token) {
		return nil, errors.New("CEM handoff requires a positive job ID and the 64-character lowercase hex attempt credential")
	}
	client := attemptClient{r.client, token}
	job, err := client.read(ctx, jobID(id))
	if err != nil {
		return nil, fmt.Errorf("read CEM attempt: %w", err)
	}
	if job.ID != id || job.Environment != "cem-"+r.options.Environment {
		return nil, errors.New("CEM attempt does not match the requested job and environment")
	}
	return r.run(ctx, client, job)
}

type attemptObservation struct {
	RepetitionID int64                       `json:"repetitionId"`
	AttemptID    int64                       `json:"attemptId"`
	Environment  map[string]string           `json:"observedEnvironment"`
	Artifacts    map[string]verifiedArtifact `json:"observedArtifacts"`
	Command      *commandObservation         `json:"driver,omitempty"`
	Error        string                      `json:"error,omitempty"`
}

func (r *AttemptRunner) run(ctx context.Context, client attemptClient, job *attemptJob) (*AttemptResult, error) {
	result := &AttemptResult{RunResult: RunResult{JobID: job.ID, Status: *job.Status}, AttemptID: job.AttemptID}
	if *job.Status != Setup {
		return result, fmt.Errorf("attempt %d must be awaiting start in setup", job.AttemptID)
	}
	spec, err := parseCommandSpecification(job)
	if err != nil {
		return result, err
	}
	host, _ := os.Hostname()
	observed := attemptObservation{
		RepetitionID: job.RepetitionID,
		AttemptID:    job.AttemptID,
		Environment:  map[string]string{"hostname": host, "os": runtime.GOOS, "architecture": runtime.GOARCH, "goVersion": runtime.Version()},
		Artifacts:    make(map[string]verifiedArtifact),
	}
	dir, localErr := os.MkdirTemp(r.options.WorkDir, fmt.Sprintf("job-%d-attempt-%d-", job.RepetitionID, job.AttemptID))
	result.RunDir = dir
	work := filepath.Join(dir, "work")
	staging := filepath.Join(dir, "artifacts")
	paths := make(map[string]string)
	var workRoot *os.Root
	if localErr == nil {
		defer os.RemoveAll(staging)
		localErr = r.stage(ctx, work, staging, spec, paths, observed.Artifacts)
	}
	if localErr == nil {
		workRoot, localErr = os.OpenRoot(work)
		if localErr == nil {
			defer workRoot.Close()
		}
	}

	environment, _ := json.Marshal(observed.Environment)
	artifacts, _ := json.Marshal(observed.Artifacts)
	evidence := url.Values{"environment": {string(environment)}, "artifacts": {string(artifacts)}}
	var stdout, stderr []byte
	var supplement []driverEvidenceEntry
	if localErr == nil {
		evidence.Set("status", strconv.Itoa(int(Running)))
		if err := client.form(ctx, http.MethodPatch, job, "", evidence); err != nil {
			// A lost start response may hide a committed start. Only the
			// acknowledged transition authorizes this worker to invoke it.
			startErr := fmt.Errorf("start attempt %d: %w", job.AttemptID, err)
			return result, errors.Join(startErr, saveAttemptEvidence(dir, job.Specification, observed, nil, nil, nil, startErr))
		}
		result.Status = Running
		var monitorErr error
		stdout, stderr, localErr, monitorErr = r.execute(ctx, client, job, result, spec, work, paths, &observed)
		var collectErr error
		supplement, collectErr = collectDriverEvidence(workRoot)
		localErr = errors.Join(localErr, collectErr)
		if monitorErr != nil {
			runErr := errors.Join(localErr, monitorErr)
			return result, errors.Join(runErr, saveAttemptEvidence(dir, job.Specification, observed, stdout, stderr, supplement, runErr))
		}
	}
	if dir != "" {
		localErr = errors.Join(localErr, saveAttemptEvidence(dir, job.Specification, observed, stdout, stderr, supplement, localErr))
	}

	// Finalization has its own time budget to preserve failure evidence
	// when the driver times out or the worker receives a shutdown signal.
	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	defer cancel()
	publishErr := client.publish(finalCtx, job, result, stderr, stdout, evidence, localErr)
	return result, errors.Join(localErr, publishErr)
}

func (r *AttemptRunner) stage(ctx context.Context, work, staging string, spec commandSpecification,
	paths map[string]string, artifacts map[string]verifiedArtifact) error {
	if err := os.Mkdir(work, 0o700); err != nil {
		return err
	}
	if err := os.Mkdir(staging, 0o700); err != nil {
		return err
	}
	cache, err := os.OpenRoot(r.options.CacheDir)
	if err != nil {
		return err
	}
	defer cache.Close()
	for _, entry := range spec.Artifacts.Entries {
		path, artifact, err := stageArtifact(ctx, cache, staging, entry)
		if err != nil {
			return fmt.Errorf("stage artifact %q: %w", entry.Role, err)
		}
		paths[entry.Role], artifacts[entry.Role] = path, artifact
	}
	return nil
}

func (r *AttemptRunner) execute(ctx context.Context, client attemptClient, job *attemptJob, result *AttemptResult,
	spec commandSpecification, work string, paths map[string]string, observed *attemptObservation) ([]byte, []byte, error, error) {
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, r.options.Timeout)
	defer cancelTimeout()
	runCtx, cancelDriver := context.WithCancelCause(timeoutCtx)
	defer cancelDriver(nil)
	monitorCtx, stopMonitor := context.WithCancel(runCtx)
	done := make(chan error, 1)
	go r.watch(monitorCtx, client, job, cancelDriver, done)
	observation, stdout, stderr, err := invokeDriver(runCtx, work, job, paths)
	stopMonitor()
	monitorErr := <-done
	observed.Command = &observation
	var state *remoteStateError
	if errors.As(monitorErr, &state) {
		result.Status = state.status
	}
	if cause := context.Cause(runCtx); cause != nil && !errors.Is(err, cause) {
		err = errors.Join(err, cause)
	}
	if observation.Truncated {
		err = errors.Join(err, errors.New("driver stdout or stderr exceeded the 1 MiB evidence limit"))
	}
	if err == nil {
		err = spec.validateResult(stdout)
	}
	return stdout, stderr, err, monitorErr
}

func (r *AttemptRunner) watch(ctx context.Context, client attemptClient, job *attemptJob, stop context.CancelCauseFunc, done chan<- error) {
	var err error
	defer func() { done <- err }()
	for wait(ctx, r.options.StatusInterval) == nil {
		current, readErr := client.current(ctx, job)
		if ctx.Err() != nil {
			return
		}
		if readErr != nil {
			err = fmt.Errorf("check attempt while running: %w", readErr)
		} else if *current.Status != Running {
			err = &remoteStateError{status: *current.Status}
		}
		if err != nil {
			stop(err)
			return
		}
	}
}

func saveAttemptEvidence(dir string, specification []byte, observed attemptObservation, stdout, stderr []byte,
	supplement []driverEvidenceEntry, runErr error) error {
	if runErr != nil {
		observed.Error = runErr.Error()
	}
	metadata, err := json.MarshalIndent(observed, "", "  ")
	if err != nil {
		return err
	}
	metadata = append(metadata, '\n')
	entries := []driverEvidenceEntry{
		{name: attemptSpecName, data: specification},
		{name: driverStdout, data: stdout},
		{name: driverStderr, data: stderr},
		{name: evidenceName, data: metadata},
	}
	var localErr error
	for _, entry := range entries {
		file, err := os.OpenFile(filepath.Join(dir, entry.name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			_, writeErr := file.Write(entry.data)
			err = errors.Join(writeErr, file.Close())
		}
		localErr = errors.Join(localErr, err)
	}
	return errors.Join(localErr, saveAttemptArchive(dir, append(entries, supplement...)))
}

func (a attemptClient) publish(ctx context.Context, job *attemptJob, result *AttemptResult, stderr, stdout []byte,
	evidence url.Values, runErr error) error {
	logErr := a.appendLog(ctx, job, stderr, runErr)
	var uploadErr error
	if result.RunDir != "" {
		path := filepath.Join(result.RunDir, zipName)
		if _, err := os.Stat(path); err == nil {
			uploadErr = a.upload(ctx, job, path)
		} else if runErr == nil || !os.IsNotExist(err) {
			uploadErr = err
		}
		if uploadErr != nil {
			uploadErr = fmt.Errorf("upload attempt ZIP: %w", uploadErr)
		}
	}
	publishErr := errors.Join(logErr, uploadErr)
	failure := errors.Join(runErr, publishErr)
	if failure == nil {
		finish := url.Values{"status": {strconv.Itoa(int(Finished))}, "result": {string(stdout)}}
		rejected, err := a.terminal(ctx, job, result, Finished, finish)
		if !rejected {
			return err
		}
		publishErr, failure = err, err
	}
	if result.Status != Setup {
		evidence = url.Values{}
	}
	evidence.Set("status", strconv.Itoa(int(Failed)))
	evidence.Set("reason", attemptReason(failure))
	_, err := a.terminal(ctx, job, result, Failed, evidence)
	return errors.Join(publishErr, err)
}

func (a attemptClient) appendLog(ctx context.Context, job *attemptJob, stderr []byte, runErr error) error {
	text := strings.ToValidUTF8(string(stderr), "\uFFFD")
	if runErr != nil {
		text += "\n[agent] " + attemptReason(runErr) + "\n"
	}
	for len(text) > 0 {
		end := min(maxLogBytes, len(text))
		for end < len(text) && !utf8.RuneStart(text[end]) {
			end--
		}
		if err := a.form(ctx, http.MethodPost, job, "appendLog", url.Values{"log": {text[:end]}}); err != nil {
			return fmt.Errorf("append attempt log: %w", err)
		}
		text = text[end:]
	}
	return nil
}

func attemptReason(err error) string {
	text := strings.TrimSpace(strings.Map(func(r rune) rune {
		if !validXMLRune(r) || r == 0x7f {
			return '\uFFFD'
		}
		return r
	}, err.Error()))
	runes := []rune(text)
	if len(runes) > 4000 {
		text = string(runes[:4000])
	}
	if text == "" {
		return "The agent could not complete the attempt."
	}
	return text
}

// terminal reconciles ambiguous responses on the original attempt. Only a
// definite v1 rejection of completion permits a subsequent failure report.
func (a attemptClient) terminal(ctx context.Context, job *attemptJob, result *AttemptResult, status Status, fields url.Values) (bool, error) {
	err := a.form(ctx, http.MethodPatch, job, "", fields)
	if err != nil {
		current, readErr := a.current(ctx, job)
		if readErr != nil {
			return false, fmt.Errorf("set attempt status %d: %w", status, errors.Join(err, readErr))
		}
		result.Status = *current.Status
		if result.Status != status {
			var api *APIError
			rejected := result.Status == Running && errors.As(err, &api) && api.HTTPStatus == http.StatusOK && api.Code == 600
			return rejected, fmt.Errorf("set attempt status %d (observed %d): %w", status, result.Status, err)
		}
	}
	result.Status = status
	result.TerminalReported = true
	return false, nil
}
