package agent

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"time"
)

const (
	cdlName      = "job.cdl"
	logName      = "steps.log"
	evidenceName = "observations.json"
	manifestName = "artifact-manifest.json"
	zipName      = "result.zip"
)

type observations struct {
	SchemaVersion  int                  `json:"schemaVersion"`
	LocalRun       string               `json:"localRun"`
	JobID          int64                `json:"jobId"`
	System         string               `json:"system"`
	Environment    string               `json:"environment"`
	CDLSHA256      string               `json:"cdlSha256"`
	Host           string               `json:"hostname,omitempty"`
	OS             string               `json:"os"`
	Arch           string               `json:"arch"`
	GoVersion      string               `json:"goVersion"`
	AgentVersion   string               `json:"agentVersion,omitempty"`
	AgentRevision  string               `json:"agentRevision,omitempty"`
	AgentModified  *bool                `json:"agentModified,omitempty"`
	Started        time.Time            `json:"startedAt"`
	Ended          *time.Time           `json:"endedAt,omitempty"`
	Commands       []commandObservation `json:"commands"`
	ExecutionError string               `json:"executionError,omitempty"`
}

type artifactEntry struct {
	Role      string `json:"role"`
	URI       string `json:"uri"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
}

type manifest struct {
	SchemaVersion int             `json:"schemaVersion"`
	Artifacts     []artifactEntry `json:"artifacts"`
}

type runFiles struct {
	dir  string
	work string
	info observations
}

func createRun(root string, job *Job) (runFiles, error) {
	dir, err := os.MkdirTemp(root, fmt.Sprintf("job-%d-", job.ID))
	if err != nil {
		return runFiles{}, err
	}
	host, _ := os.Hostname()
	cdlSHA256 := sha256.Sum256([]byte(job.CDL))
	files := runFiles{
		dir: dir, work: filepath.Join(dir, "work"),
		info: observations{
			SchemaVersion: 1, LocalRun: filepath.Base(dir), JobID: job.ID, System: job.System,
			Environment: job.Environment, CDLSHA256: hex.EncodeToString(cdlSHA256[:]),
			Host: host, OS: runtime.GOOS, Arch: runtime.GOARCH, GoVersion: runtime.Version(),
			Started: time.Now().UTC(), Commands: []commandObservation{},
		},
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		if build.Main.Version != "(devel)" {
			files.info.AgentVersion = build.Main.Version
		}
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				files.info.AgentRevision = setting.Value
			case "vcs.modified":
				modified := setting.Value == "true"
				files.info.AgentModified = &modified
			}
		}
	}
	err = os.Mkdir(files.work, 0o700)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, cdlName), []byte(job.CDL), 0o600)
	}
	return files, err
}

func (r *runFiles) save() error {
	return writeJSON(r.dir, evidenceName, r.info)
}

func (r *runFiles) record(step commandObservation, stdout, stderr []byte) error {
	r.info.Commands = append(r.info.Commands, step)
	log, err := os.OpenFile(filepath.Join(r.dir, logName), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return errors.Join(err, r.save())
	}
	_, err = fmt.Fprintf(log, "[%s] %s\n--- stdout ---\n%s\n--- stderr ---\n%s\n", step.Step, step.State, stdout, stderr)
	if step.Error != "" && err == nil {
		_, err = fmt.Fprintf(log, "[%s] %s\n", step.Step, step.Error)
	}
	if step.Truncated && err == nil {
		_, err = fmt.Fprintln(log, "[stdout or stderr truncated at 256 KiB]")
	}
	return errors.Join(err, log.Close(), r.save())
}

func writeJSON(dir, name string, value any) error {
	file, err := os.CreateTemp(dir, "."+name+"-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := errors.Join(encoder.Encode(value), file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(dir, name))
}

func openRegular(root *os.Root, name string, limit int64) (*os.File, int64, error) {
	file, err := openOutput(root, name)
	if err != nil {
		return nil, 0, err
	}
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > limit {
		file.Close()
		if err == nil {
			err = fmt.Errorf("%q must be a regular file within the size limit", name)
		}
		return nil, 0, err
	}
	return file, stat.Size(), nil
}

// buildArchive copies each output into a private ZIP and hashes the bytes
// written. Result JSON is read once and reused for both the ZIP and the PATCH.
func (r *runFiles) buildArchive(root *os.Root, adapter Adapter, runErr error) (json.RawMessage, error) {
	file, err := os.CreateTemp(r.dir, ".result-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	archive := zip.NewWriter(file)
	manifest := manifest{SchemaVersion: 1, Artifacts: []artifactEntry{}}
	remaining := int64(maxArtifactBytes)
	var outputErr error

	add := func(role, name string, input io.Reader, size int64) error {
		entry, err := archive.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		if err != nil {
			return err
		}
		hash := sha256.New()
		n, err := io.Copy(io.MultiWriter(entry, hash), io.LimitReader(input, size))
		if err != nil {
			return err
		}
		uri := (&url.URL{Path: name}).EscapedPath()
		manifest.Artifacts = append(manifest.Artifacts, artifactEntry{role, uri, hex.EncodeToString(hash.Sum(nil)), n})
		remaining -= n
		if n != size {
			return fmt.Errorf("%q changed size during collection", name)
		}
		return nil
	}

	var result json.RawMessage
	if adapter.Result != "" {
		resultFile, _, err := openRegular(root, adapter.Result, maxResultBytes)
		if err == nil {
			result, err = io.ReadAll(io.LimitReader(resultFile, maxResultBytes+1))
			resultFile.Close()
		}
		if err == nil {
			err = validateResult(result)
		}
		if err == nil {
			err = add("result", "artifacts/"+adapter.Result, bytes.NewReader(result), int64(len(result)))
		}
		if err != nil {
			outputErr = errors.Join(outputErr, fmt.Errorf("collect result: %w", err))
		}
	}
	// Store logs before the workload outputs so space is reserved for diagnostics.
	log, err := os.Open(filepath.Join(r.dir, logName))
	if err == nil {
		stat, err := log.Stat()
		if err == nil {
			err = add("log", logName, log, stat.Size())
		}
		log.Close()
		if err != nil {
			outputErr = errors.Join(outputErr, err)
		}
	} else if !os.IsNotExist(err) {
		outputErr = errors.Join(outputErr, err)
	}
	for _, output := range adapter.Artifacts {
		input, size, err := openRegular(root, output.Path, remaining)
		if err == nil {
			err = add(output.Role, "artifacts/"+output.Path, input, size)
			input.Close()
		}
		if err != nil {
			outputErr = errors.Join(outputErr, fmt.Errorf("collect artifact %q: %w", output.Path, err))
		}
	}

	ended := time.Now().UTC()
	r.info.Ended = &ended
	if err := errors.Join(runErr, outputErr); err != nil {
		r.info.ExecutionError = err.Error()
	}
	if err := errors.Join(r.save(), writeJSON(r.dir, manifestName, manifest)); err != nil {
		return nil, errors.Join(outputErr, err)
	}
	for _, name := range []string{evidenceName, manifestName} {
		input, err := os.Open(filepath.Join(r.dir, name))
		if err != nil {
			return nil, errors.Join(outputErr, err)
		}
		entry, copyErr := archive.Create(name)
		if copyErr == nil {
			_, copyErr = io.Copy(entry, input)
		}
		input.Close()
		if copyErr != nil {
			return nil, errors.Join(outputErr, copyErr)
		}
	}
	if err := errors.Join(archive.Close(), file.Close()); err != nil {
		return nil, errors.Join(outputErr, err)
	}
	return result, errors.Join(outputErr, os.Rename(file.Name(), filepath.Join(r.dir, zipName)))
}
