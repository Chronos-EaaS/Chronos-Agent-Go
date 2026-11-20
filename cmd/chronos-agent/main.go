package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	agent "github.com/Chronos-EaaS/Chronos-Agent-Go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("chronos-agent", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: chronos-agent -url URL -config FILE [options]")
		fmt.Fprintln(stderr, "Poll a Chronos Control v1 system queue or run a bootstrapped CEM job.")
		fmt.Fprintln(stderr, "Optional gateway credentials are read from CHRONOS_AUTHORIZATION.")
		flags.PrintDefaults()
	}
	base := flags.String("url", "", "Chronos Control origin or /api/v1 URL")
	config := flags.String("config", "", "JSON file mapping supported system identifiers to local adapters")
	environment := flags.String("environment", "default", "Chronos environment name without system- or cem-")
	work := flags.String("work-dir", ".chronos-runs", "directory for local evidence and workload output")
	timeout := flags.Duration("timeout", 30*time.Minute, "maximum time for job commands and phase updates")
	status := flags.Duration("status-interval", 2*time.Second, "interval between remote cancellation checks")
	poll := flags.Duration("poll-interval", 5*time.Second, "interval before the next queue poll")
	upload := flags.String("upload-endpoint", "", "optional HTTP(S) /api/v1/job endpoint for result ZIP uploads")
	once := flags.Bool("once", false, "poll once and run at most one job")
	cemJob := flags.Int64("cem-job-id", 0, "run a specific CEM job already in setup and exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *base == "" || *config == "" || *poll <= 0 || *cemJob < 0 || (*once && *cemJob != 0) {
		flags.Usage()
		return 2
	}
	adapters, err := loadAdapters(*config)
	if err != nil {
		fmt.Fprintln(stderr, "read adapter configuration:", err)
		return 2
	}
	client, err := agent.NewClient(*base, agent.ClientOptions{
		Authorization:  os.Getenv("CHRONOS_AUTHORIZATION"),
		UploadEndpoint: *upload,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	runner, err := agent.NewRunner(client, adapters, agent.RunnerOptions{
		Environment: *environment, WorkDir: *work, Timeout: *timeout, StatusInterval: *status,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}

	for {
		var result *agent.RunResult
		if *cemJob != 0 {
			result, err = runner.RunCEMJob(ctx, *cemJob)
		} else {
			result, err = runner.RunOnce(ctx)
		}
		if result != nil {
			fmt.Fprintf(stdout, "job %d: status %d, terminal report acknowledged %t, local evidence %s\n",
				result.JobID, result.Status, result.TerminalReported, result.RunDir)
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
		}
		if ctx.Err() != nil {
			return 130
		}
		if *once || *cemJob != 0 {
			if result == nil && err == nil {
				fmt.Fprintln(stdout, "No job in queue.")
			}
			return exitCode(err)
		}
		// An unreconciled write or read error stops the worker. A reported
		// job failure can be followed by a later scheduled job.
		if err != nil && (result == nil || !result.TerminalReported) {
			return exitCode(err)
		}
		timer := time.NewTimer(*poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 130
		case <-timer.C:
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var process *agent.ProcessError
	if errors.As(err, &process) && process.ExitCode != nil && *process.ExitCode > 0 && *process.ExitCode <= 125 {
		return *process.ExitCode
	}
	return 1
}

func loadAdapters(path string) (map[string]agent.Adapter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	const limit = 1 << 20
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, errors.New("configuration exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var adapters map[string]agent.Adapter
	if err := decoder.Decode(&adapters); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("expected exactly one JSON configuration object")
	}
	return adapters, nil
}
