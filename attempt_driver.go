package agent

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	attemptSpecName = "specification.json"
	driverStdout    = "stdout.json"
	driverStderr    = "stderr.log"
)

var (
	hex32Pattern        = regexp.MustCompile(`^[a-f0-9]{64}$`)
	artifactRolePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,99}$`)
)

type commandSpecification struct {
	SchemaVersion int    `json:"schemaVersion"`
	Protocol      string `json:"protocol"`
	Adapter       string `json:"adapter"`
	Environment   string `json:"environment"`
	System        struct {
		Identifier string `json:"identifier"`
	} `json:"system"`
	Artifacts struct {
		SchemaVersion int                `json:"schemaVersion"`
		Entries       []expectedArtifact `json:"artifacts"`
	} `json:"artifacts"`
	Benchmark struct {
		Contract struct {
			Results map[string]struct {
				Type string `json:"type"`
			} `json:"results"`
		} `json:"contract"`
	} `json:"benchmark"`
}

type expectedArtifact struct {
	Role      string `json:"role"`
	SHA256    string `json:"sha256"`
	SizeBytes *int64 `json:"sizeBytes"`
}

type verifiedArtifact struct {
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
}

func parseCommandSpecification(job *attemptJob) (commandSpecification, error) {
	var spec commandSpecification
	err := json.Unmarshal(job.Specification, &spec)
	if err != nil || spec.SchemaVersion != 1 || spec.Protocol != attemptProtocol || spec.Adapter != commandAdapter ||
		spec.System.Identifier != job.System || spec.Environment != job.Environment ||
		spec.Artifacts.SchemaVersion != 1 || len(spec.Artifacts.Entries) == 0 || len(spec.Artifacts.Entries) > 30 ||
		len(spec.Benchmark.Contract.Results) == 0 {
		return spec, errors.New("invalid frozen chronos-command@1 specification")
	}
	roles := make(map[string]bool)
	for _, item := range spec.Artifacts.Entries {
		if !artifactRolePattern.MatchString(item.Role) || item.Role == "run" || roles[item.Role] ||
			!hex32Pattern.MatchString(item.SHA256) || item.SizeBytes == nil || *item.SizeBytes < 0 {
			return spec, fmt.Errorf("invalid manifest entry for artifact role %q", item.Role)
		}
		roles[item.Role] = true
	}
	if !roles["driver"] {
		return spec, errors.New("artifact manifest does not contain a driver")
	}
	for field, rule := range spec.Benchmark.Contract.Results {
		switch rule.Type {
		case "string", "integer", "number", "boolean":
		default:
			return spec, fmt.Errorf("unsupported type for result field %q", field)
		}
	}
	return spec, nil
}

func stageArtifact(ctx context.Context, cache *os.Root, dir string, artifact expectedArtifact) (string, verifiedArtifact, error) {
	var verified verifiedArtifact
	info, err := cache.Lstat(artifact.SHA256)
	size := *artifact.SizeBytes
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		return "", verified, errors.New("expected a regular cache file of the declared size")
	}
	source, err := openOutput(cache, artifact.SHA256)
	if err != nil {
		return "", verified, err
	}
	defer source.Close()
	openInfo, err := source.Stat()
	if err != nil || !os.SameFile(info, openInfo) {
		return "", verified, errors.New("cache file changed while opening it")
	}

	path := filepath.Join(dir, artifact.Role)
	target, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", verified, err
	}
	defer target.Close()
	hash := sha256.New()
	input := contextReader{ctx, source}
	n, err := io.CopyN(io.MultiWriter(target, hash), input, size)
	if err != nil {
		return "", verified, err
	}
	if _, err := io.ReadFull(input, make([]byte, 1)); err != io.EOF {
		return "", verified, errors.New("cache file changed size during staging")
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if digest != artifact.SHA256 {
		return "", verified, errors.New("SHA-256 mismatch")
	}
	mode := os.FileMode(0o400)
	if artifact.Role == "driver" {
		mode = 0o500
	}
	if err := target.Chmod(mode); err != nil {
		return "", verified, err
	}
	if err := target.Close(); err != nil {
		return "", verified, err
	}
	return path, verifiedArtifact{SHA256: digest, SizeBytes: n}, nil
}

type contextReader struct {
	ctx context.Context
	in  io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.in.Read(p)
}

func invokeDriver(ctx context.Context, work string, job *attemptJob, paths map[string]string) (commandObservation, []byte, []byte, error) {
	input := struct {
		RepetitionID  int64             `json:"repetitionId"`
		AttemptID     int64             `json:"attemptId"`
		Specification json.RawMessage   `json:"specification"`
		ArtifactPaths map[string]string `json:"artifactPaths"`
	}{job.RepetitionID, job.AttemptID, job.Specification, paths}
	payload, err := json.Marshal(input)
	if err != nil {
		return commandObservation{}, nil, nil, err
	}
	cmd := exec.CommandContext(ctx, paths["driver"])
	cmd.Dir = work
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.Stdin = bytes.NewReader(payload)
	return runProcess(ctx, cmd, "driver", maxResultBytes)
}

// Control requires each declared field and permits additional diagnostics.
func (s commandSpecification) validateResult(raw []byte) error {
	value := bytes.TrimSpace(raw)
	var fields map[string]json.RawMessage
	if len(value) == 0 || value[0] != '{' || !utf8.Valid(raw) || json.Unmarshal(value, &fields) != nil {
		return errors.New("driver stdout must contain a UTF-8 JSON result object")
	}
	for field, rule := range s.Benchmark.Contract.Results {
		value := fields[field]
		if len(value) == 0 {
			return fmt.Errorf("driver result is missing field %q", field)
		}
		if !validResultValue(value, rule.Type) {
			return fmt.Errorf("driver result field %q must be a %s", field, rule.Type)
		}
	}
	return nil
}

func validResultValue(value json.RawMessage, kind string) bool {
	switch kind {
	case "boolean":
		return string(value) == "true" || string(value) == "false"
	case "integer":
		_, err := strconv.ParseInt(string(value), 10, 64)
		return err == nil
	case "number":
		if value[0] != '-' && (value[0] < '0' || value[0] > '9') {
			return false
		}
		_, err := strconv.ParseFloat(string(value), 64)
		return err == nil
	case "string":
		var message string
		return value[0] == '"' && json.Unmarshal(value, &message) == nil && len(message) <= 16384 &&
			strings.IndexFunc(message, func(r rune) bool { return !validXMLRune(r) }) == -1
	default:
		return false
	}
}

func validXMLRune(r rune) bool {
	return r == '\t' || r == '\n' || r == '\r' || (r >= 0x20 && r <= 0xD7FF) ||
		(r >= 0xE000 && r <= 0xFFFD) || r >= 0x10000
}

func saveAttemptArchive(dir string, entries []driverEvidenceEntry) error {
	output, err := os.CreateTemp(dir, ".result-")
	if err != nil {
		return err
	}
	defer os.Remove(output.Name())
	defer output.Close()
	archive := zip.NewWriter(output)
	for _, evidence := range entries {
		entry, err := archive.Create(evidence.name)
		if err != nil {
			return err
		}
		if _, err := entry.Write(evidence.data); err != nil {
			return err
		}
	}
	if err := errors.Join(archive.Close(), output.Close()); err != nil {
		return err
	}
	return os.Rename(output.Name(), filepath.Join(dir, zipName))
}
