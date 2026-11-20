package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Command names a trusted local executable and its argument vector. A whole
// argument of the form {{evaluation.key}} or {{setup.key}} binds CDL text.
type Command struct {
	Program string   `json:"program"`
	Args    []string `json:"args,omitempty"`
}

type Artifact struct {
	Role string `json:"role"`
	Path string `json:"path"`
}

// Adapter describes the operator-configured commands and expected outputs
// for one system. Output paths are relative to the process working directory.
type Adapter struct {
	Phases    map[string]Command `json:"phases"`
	Version   *Command           `json:"version,omitempty"`
	Result    string             `json:"result,omitempty"`
	Artifacts []Artifact         `json:"artifacts,omitempty"`
}

func (a Adapter) validate() error {
	if len(a.Phases) == 0 {
		return errors.New("adapter needs at least one phase command")
	}
	for name, command := range a.Phases {
		switch name {
		case "prepare", "warmup", "execute", "analyze", "clean":
		default:
			return fmt.Errorf("unknown phase %q", name)
		}
		if err := validateCommand(command); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if a.Version != nil {
		if err := validateCommand(*a.Version); err != nil {
			return fmt.Errorf("version: %w", err)
		}
	}
	if a.Result != "" && !outputPath(a.Result) {
		return errors.New("result must be a portable relative file path")
	}
	if len(a.Artifacts) > 128 {
		return errors.New("adapter supports at most 128 artifacts")
	}
	seen := map[string]bool{a.Result: true}
	for _, artifact := range a.Artifacts {
		if strings.TrimSpace(artifact.Role) == "" || !outputPath(artifact.Path) || seen[artifact.Path] {
			return fmt.Errorf("invalid or duplicate artifact path %q or missing role", artifact.Path)
		}
		seen[artifact.Path] = true
	}
	return nil
}

func outputPath(name string) bool {
	return name != "." && len(name) <= 512 && filepath.IsLocal(name) && path.Clean(name) == name &&
		!strings.ContainsAny(name, "\\:\r\n\x00")
}

func validateCommand(c Command) error {
	if len(c.Program) == 0 || len(c.Program) > 4096 || strings.ContainsAny(c.Program, "\x00\r\n{}") ||
		(!filepath.IsAbs(c.Program) && filepath.Base(c.Program) != c.Program) {
		return errors.New("program must be an absolute path or a bare executable name")
	}
	if len(c.Args) > 256 {
		return errors.New("command supports at most 256 arguments")
	}
	return nil
}

func (c Command) resolve(cdl CDL) (Command, error) {
	resolved := Command{Program: c.Program, Args: make([]string, len(c.Args))}
	total := 0
	for index, arg := range c.Args {
		if strings.HasPrefix(arg, "{{") && strings.HasSuffix(arg, "}}") {
			reference := strings.TrimSuffix(strings.TrimPrefix(arg, "{{"), "}}")
			section, key, _ := strings.Cut(reference, ".")
			var found bool
			switch section {
			case "evaluation":
				arg, found = cdl.Evaluation[key]
			case "setup":
				arg, found = cdl.Setup[key]
			}
			if !found {
				return Command{}, fmt.Errorf("missing CDL argument %q", reference)
			}
		}
		total += len(arg) + 1
		if strings.ContainsRune(arg, 0) || total > 64<<10 {
			return Command{}, errors.New("arguments contain NUL or exceed 64 KiB")
		}
		resolved.Args[index] = arg
	}
	return resolved, nil
}

// ProcessError exposes the execution failure and the observed exit status.
// A nil exit code means that the process did not start. A negative exit code
// means that the process terminated without an ordinary exit code.
type ProcessError struct {
	Step     string
	ExitCode *int
	Err      error
}

func (e *ProcessError) Error() string { return fmt.Sprintf("%s command: %v", e.Step, e.Err) }
func (e *ProcessError) Unwrap() error { return e.Err }

type commandObservation struct {
	Step      string    `json:"step"`
	Argv      []string  `json:"argv"`
	Path      string    `json:"resolvedPath,omitempty"`
	Started   time.Time `json:"startedAt"`
	Ended     time.Time `json:"endedAt"`
	ExitCode  *int      `json:"exitCode,omitempty"`
	State     string    `json:"processState,omitempty"`
	Error     string    `json:"error,omitempty"`
	Truncated bool      `json:"outputTruncated,omitempty"`
}

func runCommand(ctx context.Context, dir, cdlFile, step string, command Command) (commandObservation, []byte, []byte, error) {
	cmd := exec.CommandContext(ctx, command.Program, command.Args...)
	cmd.Dir = dir
	cmd.Env = []string{"CHRONOS_CDL_FILE=" + cdlFile}
	for _, name := range []string{"PATH", "LANG", "LC_ALL", "TZ", "TMPDIR"} {
		if value, ok := os.LookupEnv(name); ok {
			cmd.Env = append(cmd.Env, name+"="+value)
		}
	}
	configureProcess(cmd)
	cmd.WaitDelay = 2 * time.Second
	stdout := &commandOutput{}
	stderr := &commandOutput{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	observation := commandObservation{
		Step: step, Path: cmd.Path, Argv: cmd.Args, Started: time.Now().UTC(),
	}
	err := cmd.Run()
	observation.Ended = time.Now().UTC()
	if cmd.ProcessState != nil {
		exitCode := cmd.ProcessState.ExitCode()
		observation.ExitCode = &exitCode
		observation.State = cmd.ProcessState.String()
	}
	observation.Truncated = stdout.truncated || stderr.truncated
	if err != nil {
		if cause := context.Cause(ctx); cause != nil && !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
		observation.Error = err.Error()
		err = &ProcessError{Step: step, ExitCode: observation.ExitCode, Err: err}
	}
	return observation, stdout.Bytes(), stderr.Bytes(), err
}

type commandOutput struct {
	bytes.Buffer
	truncated bool
}

func (o *commandOutput) Write(data []byte) (int, error) {
	remaining := 256<<10 - o.Len()
	if len(data) > remaining {
		o.truncated = true
	}
	_, _ = o.Buffer.Write(data[:min(remaining, len(data))])
	return len(data), nil
}
