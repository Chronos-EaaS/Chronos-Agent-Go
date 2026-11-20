package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	driverEvidenceDir      = "evidence"
	maxEvidenceFiles       = 256
	maxEvidenceBytes       = 16 << 20
	maxEvidenceTreeEntries = 512
	maxEvidenceDepth       = 16
)

type driverEvidenceEntry struct {
	name string
	data []byte
}

type driverEvidenceCollector struct {
	entries   []driverEvidenceEntry
	numPaths  int
	remaining int64
	firstErr  error
}

func collectDriverEvidence(workRoot *os.Root) ([]driverEvidenceEntry, error) {
	info, err := workRoot.Lstat(driverEvidenceDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect driver evidence: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("driver evidence must be a directory")
	}
	root, err := workRoot.OpenRoot(driverEvidenceDir)
	if err != nil {
		return nil, fmt.Errorf("open driver evidence: %w", err)
	}
	defer root.Close()
	if err := checkEvidenceDirectory(workRoot, driverEvidenceDir, root, info); err != nil {
		return nil, err
	}
	collector := driverEvidenceCollector{remaining: maxEvidenceBytes}
	collector.visit(root, "", 0)
	return collector.entries, collector.firstErr
}

func checkEvidenceDirectory(parent *os.Root, name string, dir *os.Root, expected os.FileInfo) error {
	found, err := dir.Stat(".")
	if err != nil || !os.SameFile(expected, found) || !sameEvidencePath(parent, name, expected) {
		return errors.New("driver evidence directory changed while opening it")
	}
	return nil
}

func sameEvidencePath(root *os.Root, name string, expected os.FileInfo) bool {
	info, err := root.Lstat(name)
	return err == nil && os.SameFile(expected, info)
}

func (c *driverEvidenceCollector) visit(root *os.Root, parent string, depth int) {
	dir, err := root.Open(".")
	if err != nil {
		c.record(parent, err)
		return
	}
	defer dir.Close()
	for c.numPaths <= maxEvidenceTreeEntries {
		children, err := dir.ReadDir(32)
		for _, child := range children {
			if c.numPaths >= maxEvidenceTreeEntries {
				c.numPaths++
				c.record("", fmt.Errorf("tree exceeds %d entries", maxEvidenceTreeEntries))
				return
			}
			c.numPaths++
			c.visitEntry(root, parent, child.Name(), depth)
		}
		switch {
		case err == io.EOF:
			return
		case err != nil:
			c.record(parent, err)
			return
		}
	}
}

func (c *driverEvidenceCollector) visitEntry(root *os.Root, parent, name string, depth int) {
	relative := path.Join(parent, name)
	if !outputPath(relative) || !utf8.ValidString(relative) || strings.IndexFunc(relative, unicode.IsControl) >= 0 {
		c.record(relative, errors.New("expected a portable relative path of at most 512 bytes"))
		return
	}
	info, err := root.Lstat(name)
	if err != nil {
		c.record(relative, err)
		return
	}
	if info.IsDir() {
		c.visitDirectory(root, name, relative, info, depth)
		return
	}
	if !info.Mode().IsRegular() || !singleFileLink(info) {
		c.record(relative, errors.New("expected a directory or a regular file with one link"))
		return
	}
	file, err := openOutput(root, name)
	if err != nil {
		c.record(relative, err)
		return
	}
	defer file.Close()

	current, err := file.Stat()
	if err != nil || !os.SameFile(info, current) || !sameEvidencePath(root, name, current) {
		c.record(relative, errors.New("entry changed while opening it"))
		return
	}
	c.collect(file, relative, current)
}

func (c *driverEvidenceCollector) visitDirectory(parent *os.Root, name, relative string, info os.FileInfo, depth int) {
	if depth == maxEvidenceDepth {
		c.record(relative, fmt.Errorf("tree exceeds %d directory levels", maxEvidenceDepth))
		return
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		c.record(relative, err)
		return
	}
	defer root.Close()
	if err := checkEvidenceDirectory(parent, name, root, info); err != nil {
		c.record(relative, err)
		return
	}
	c.visit(root, relative, depth+1)
}

func (c *driverEvidenceCollector) collect(file *os.File, name string, info os.FileInfo) {
	if !singleFileLink(info) {
		c.record(name, errors.New("file has more than one link"))
		return
	}
	if len(c.entries) >= maxEvidenceFiles {
		c.record(name, fmt.Errorf("archive exceeds %d evidence files", maxEvidenceFiles))
		return
	}
	if info.Size() > c.remaining {
		c.record(name, fmt.Errorf("archive exceeds %d MiB of driver evidence", maxEvidenceBytes>>20))
		return
	}
	data, err := io.ReadAll(io.LimitReader(file, info.Size()+1))
	if err != nil {
		c.record(name, err)
		return
	}
	current, err := file.Stat()
	if err != nil || !singleFileLink(current) || int64(len(data)) != info.Size() ||
		current.Size() != info.Size() || !current.ModTime().Equal(info.ModTime()) {
		c.record(name, errors.New("file changed while collecting it"))
		return
	}
	c.remaining -= int64(len(data))
	c.entries = append(c.entries, driverEvidenceEntry{name: driverEvidenceDir + "/" + name, data: data})
}

func (c *driverEvidenceCollector) record(name string, err error) {
	if c.firstErr == nil {
		c.firstErr = fmt.Errorf("collect driver evidence %q: %w", path.Join(driverEvidenceDir, name), err)
	}
}
