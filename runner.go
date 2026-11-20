package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ErrJobStateChanged means Control reported a status other than the one
// expected by the runner. The runner stops and preserves local evidence.
var ErrJobStateChanged = errors.New("job changed on Chronos Control")

type remoteStateError struct{ status Status }

func (e *remoteStateError) Error() string {
	return fmt.Sprintf("%v: expected status %d, observed %d", ErrJobStateChanged, Running, e.status)
}
func (e *remoteStateError) Unwrap() error { return ErrJobStateChanged }

type RunnerOptions struct {
	Environment    string
	WorkDir        string
	Timeout        time.Duration
	StatusInterval time.Duration
}

// Runner executes at most one job at a time. The system queue must be
// exclusive to this runner because v1 system polling does not claim jobs.
type Runner struct {
	execution sync.Mutex
	client    *Client
	adapters  map[string]Adapter
	systems   []string
	options   RunnerOptions
}

// NewRunner validates the local adapters and creates the run directory.
// Adapters and their contents must remain immutable while the runner uses them.
func NewRunner(client *Client, adapters map[string]Adapter, options RunnerOptions) (*Runner, error) {
	if client == nil || len(adapters) == 0 || options.WorkDir == "" ||
		options.Timeout <= 0 || options.StatusInterval <= 0 {
		return nil, errors.New("runner requires a client, adapters, work directory, timeout, and status interval")
	}
	runner := &Runner{client: client, adapters: adapters, options: options}
	for system, adapter := range adapters {
		if system == "" || system == "demoall" || strings.TrimSpace(system) != system || strings.Contains(system, ",") {
			return nil, fmt.Errorf("invalid adapter system %q", system)
		}
		if err := adapter.validate(); err != nil {
			return nil, fmt.Errorf("adapter %q: %w", system, err)
		}
		runner.systems = append(runner.systems, system)
	}
	slices.Sort(runner.systems)
	abs, err := filepath.Abs(options.WorkDir)
	if err != nil {
		return nil, err
	}
	runner.options.WorkDir = abs
	runner.options.Environment = defaultEnvironment(options.Environment)
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return runner, nil
}

type RunResult struct {
	JobID  int64
	RunDir string
	// Status is the last server status confirmed by this runner.
	Status Status
	// TerminalReported indicates that the terminal PATCH was acknowledged.
	// False after a write error means the server outcome may be uncertain.
	TerminalReported bool
}

// RunOnce polls and runs one scheduled system job. An empty queue returns
// (nil, nil). Transport retries on reads never restart the local execution.
func (r *Runner) RunOnce(ctx context.Context) (*RunResult, error) {
	if !r.execution.TryLock() {
		return nil, errors.New("runner is already handling a job")
	}
	defer r.execution.Unlock()
	job, err := r.client.Poll(ctx, r.systems, r.options.Environment)
	if err != nil || job == nil {
		return nil, err
	}
	return r.run(ctx, job)
}

// RunCEMJob runs an explicit job already moved to setup by the CEM bootstrapper.
// The bootstrapper owns CEM reservation, setup, and node termination.
func (r *Runner) RunCEMJob(ctx context.Context, id int64) (*RunResult, error) {
	if !r.execution.TryLock() {
		return nil, errors.New("runner is already handling a job")
	}
	defer r.execution.Unlock()
	job, err := r.client.Job(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.Status != Setup || job.Environment != "cem-"+r.options.Environment {
		return nil, fmt.Errorf("job %d must be in setup in environment cem-%s", id, r.options.Environment)
	}
	return r.run(ctx, job)
}

type phaseCommand struct {
	phase Phase
	cmd   Command
}

func planCommands(job *Job, adapter Adapter) ([]phaseCommand, *Command, error) {
	cdl, err := ParseCDL(job.CDL)
	if err != nil {
		return nil, nil, err
	}
	var plan []phaseCommand
	for phase := Prepare; phase <= Clean; phase++ {
		command, ok := adapter.Phases[phaseNames[phase]]
		if !ok || !job.Includes(phase) {
			continue
		}
		cmd, err := command.resolve(cdl)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", phaseNames[phase], err)
		}
		plan = append(plan, phaseCommand{phase, cmd})
	}
	if len(plan) == 0 {
		return nil, nil, errors.New("job has no enabled phase with a local command")
	}
	var version *Command
	if adapter.Version != nil {
		probe, err := adapter.Version.resolve(cdl)
		if err != nil {
			return nil, nil, fmt.Errorf("version: %w", err)
		}
		version = &probe
	}
	return plan, version, nil
}

func (r *Runner) run(ctx context.Context, job *Job) (*RunResult, error) {
	adapter, ok := r.adapters[job.System]
	if !ok {
		return nil, fmt.Errorf("no local adapter for %q", job.System)
	}
	files, err := createRun(r.options.WorkDir, job)
	result := &RunResult{JobID: job.ID, RunDir: files.dir, Status: job.Status}
	if err != nil {
		return result, err
	}

	outputRoot, err := os.OpenRoot(files.work)
	if err != nil {
		return result, err
	}
	defer outputRoot.Close()

	if err := files.save(); err != nil {
		return result, err
	}
	plan, version, planErr := planCommands(job, adapter)
	// Re-read just before the first mutation. This reduces the stale poll
	// window, but v1 has no compare-and-swap or attempt token.
	current, err := r.client.Job(ctx, job.ID)
	if err != nil {
		return result, err
	}
	result.Status = current.Status
	if current.Status != job.Status || current.System != job.System || current.Environment != job.Environment ||
		current.CDL != job.CDL || current.PhaseMask != job.PhaseMask {
		return result, fmt.Errorf("%w: job %d changed after polling", ErrJobStateChanged, job.ID)
	}
	start := Running
	zero := 0
	if err := r.client.Update(ctx, job.ID, JobUpdate{Status: &start, Progress: &zero}); err != nil {
		return result, fmt.Errorf("set running: %w", err)
	}
	result.Status = Running

	runCtx, cancelTimeout := context.WithTimeout(ctx, r.options.Timeout)
	defer cancelTimeout()
	runCtx, cancelRun := context.WithCancelCause(runCtx)
	defer cancelRun(nil)
	monitorCtx, stopMonitor := context.WithCancel(runCtx)
	observed := make(chan struct{})
	go r.watch(monitorCtx, job.ID, cancelRun, observed)

	executionErr := planErr
	var controlErr error
	if executionErr == nil {
		executionErr, controlErr = r.execute(runCtx, job.ID, &files, plan, version)
	}
	stopMonitor()
	<-observed
	cause := context.Cause(runCtx)
	if cause != nil && !errors.Is(executionErr, cause) {
		executionErr = errors.Join(executionErr, cause)
	}
	cancelTimeout()

	outputs := adapter
	if planErr != nil {
		// No process was invoked and there are no declared outputs to collect.
		outputs = Adapter{}
	}
	payload, evidenceErr := files.buildArchive(outputRoot, outputs, errors.Join(executionErr, controlErr))
	runErr := errors.Join(executionErr, evidenceErr)
	if controlErr != nil {
		return result, errors.Join(runErr, controlErr)
	}
	var state *remoteStateError
	if errors.As(cause, &state) {
		result.Status = state.status
		return result, runErr
	}

	status := Finished
	switch {
	case ctx.Err() != nil:
		status = Aborted
		runErr = errors.Join(runErr, ctx.Err())
	case runErr != nil:
		status = Failed
	}

	// Use a separate bounded context to report outcomes after cancellation.
	finalCtx, cancelFinal := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	defer cancelFinal()
	if err := r.publish(finalCtx, result, status, payload, runErr); err != nil {
		return result, errors.Join(runErr, err)
	}
	return result, runErr
}

func (r *Runner) watch(ctx context.Context, id int64, stop context.CancelCauseFunc, done chan<- struct{}) {
	defer close(done)
	for wait(ctx, r.options.StatusInterval) == nil {
		job, err := r.client.Job(ctx, id)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			stop(fmt.Errorf("check job status: %w", err))
			return
		}
		if job.Status != Running {
			stop(&remoteStateError{status: job.Status})
			return
		}
	}
}

func (r *Runner) execute(ctx context.Context, id int64, files *runFiles, plan []phaseCommand, version *Command) (executionErr, controlErr error) {
	cdlFile := filepath.Join(files.dir, cdlName)
	if version != nil {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		step, stdout, stderr, _ := runCommand(probeCtx, files.work, cdlFile, "version", *version)
		cancel()
		if err := files.record(step, stdout, stderr); err != nil {
			return err, nil
		}
	}
	for _, step := range plan {
		if ctx.Err() != nil {
			return context.Cause(ctx), nil
		}
		if err := r.client.Update(ctx, id, JobUpdate{Phase: &step.phase}); err != nil {
			return nil, fmt.Errorf("set phase %s: %w", phaseNames[step.phase], err)
		}
		observation, stdout, stderr, runErr := runCommand(ctx, files.work, cdlFile, phaseNames[step.phase], step.cmd)
		if err := errors.Join(runErr, files.record(observation, stdout, stderr)); err != nil {
			return err, nil
		}
		if ctx.Err() != nil {
			return context.Cause(ctx), nil
		}
		progress := 100
		if err := r.client.Update(ctx, id, JobUpdate{Progress: &progress}); err != nil {
			return nil, fmt.Errorf("set progress: %w", err)
		}
	}
	return nil, nil
}

func (r *Runner) stillRunning(ctx context.Context, result *RunResult) error {
	job, err := r.client.Job(ctx, result.JobID)
	if err != nil {
		return fmt.Errorf("check job before publication: %w", err)
	}
	result.Status = job.Status
	if job.Status != Running {
		return &remoteStateError{status: job.Status}
	}
	return nil
}

func (r *Runner) publish(ctx context.Context, result *RunResult, status Status, payload []byte, runErr error) error {
	if err := r.stillRunning(ctx, result); err != nil {
		return err
	}
	log, err := os.ReadFile(filepath.Join(result.RunDir, logName))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	text := strings.ToValidUTF8(string(log), "\uFFFD")
	if runErr != nil {
		text += "[agent] " + runErr.Error() + "\n"
	}
	for len(text) > 0 {
		size := min(maxLogBytes, len(text))
		for size < len(text) && !utf8.RuneStart(text[size]) {
			size--
		}
		chunk := text[:size]
		if chunk == "0" {
			chunk += "\n"
		}
		if err := r.client.AppendLog(ctx, result.JobID, chunk); err != nil {
			return fmt.Errorf("append log: %w", err)
		}
		text = text[size:]
	}

	update := JobUpdate{Status: &status}
	if status == Finished {
		archive, err := os.Open(filepath.Join(result.RunDir, zipName))
		if err != nil {
			return err
		}
		defer archive.Close()
		stat, err := archive.Stat()
		if err != nil {
			return err
		}
		if err := r.client.UploadZIP(ctx, result.JobID, zipName, archive, stat.Size()); err != nil {
			return fmt.Errorf("upload result ZIP: %w", err)
		}
		update.Result = payload
	}
	if err := r.stillRunning(ctx, result); err != nil {
		return err
	}
	if err := r.client.Update(ctx, result.JobID, update); err != nil {
		return fmt.Errorf("set terminal status %d: %w", status, err)
	}
	result.Status = status
	result.TerminalReported = true
	return nil
}
