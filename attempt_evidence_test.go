//go:build linux || darwin

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestCollectDriverEvidenceRejectsLinksAndSpecialFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(string, string) error
	}{
		{"symlink", func(target, link string) error { return os.Symlink(target, link) }},
		{"hardlink", func(target, link string) error { return os.Link(target, link) }},
		{"fifo", func(_, name string) error { return syscall.Mkfifo(name, 0o600) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, tree := prepareEvidenceTree(t)
			secret := filepath.Join(work, "outside-evidence")
			if err := os.WriteFile(secret, []byte("must stay outside the ZIP"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := tc.make(secret, filepath.Join(tree, "rejected")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(tree, "valid.txt"), []byte("collect me"), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := collectEvidenceAt(t, work)
			if err == nil || !strings.Contains(err.Error(), "evidence/rejected") {
				t.Fatalf("unsafe entry was accepted: %v", err)
			}
			if len(got) != 1 || got[0].name != "evidence/valid.txt" || string(got[0].data) != "collect me" {
				t.Fatalf("safe evidence was lost or unsafe evidence included: %v", got)
			}
		})
	}
}

func TestCollectDriverEvidenceRejectsSymlinkedDirectory(t *testing.T) {
	work := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(work, driverEvidenceDir)); err != nil {
		t.Fatal(err)
	}
	if got, err := collectEvidenceAt(t, work); len(got) != 0 || err == nil || !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("linked root was accepted: %v %v", got, err)
	}
}

func TestDriverEvidenceLimits(t *testing.T) {
	t.Run("file count", func(t *testing.T) {
		work, tree := prepareEvidenceTree(t)
		for i := 0; i < maxEvidenceFiles+1; i++ {
			if err := os.WriteFile(filepath.Join(tree, fmt.Sprintf("part-%d.txt", i)), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		got, err := collectEvidenceAt(t, work)
		if len(got) != maxEvidenceFiles || err == nil || !strings.Contains(err.Error(), "256 evidence files") {
			t.Fatalf("collected %d files, error %v", len(got), err)
		}
	})
	t.Run("total bytes", func(t *testing.T) {
		work, tree := prepareEvidenceTree(t)
		for _, name := range []string{"first", "second"} {
			file, err := os.Create(filepath.Join(tree, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(maxEvidenceBytes/2 + 1); err != nil {
				file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
		got, err := collectEvidenceAt(t, work)
		if len(got) != 1 || len(got[0].data) != maxEvidenceBytes/2+1 ||
			err == nil || !strings.Contains(err.Error(), "16 MiB") {
			t.Fatalf("unexpected collection result with %d files, error %v", len(got), err)
		}
	})
	t.Run("directory depth", func(t *testing.T) {
		work, tree := prepareEvidenceTree(t)
		for i := 0; i < maxEvidenceDepth+1; i++ {
			tree = filepath.Join(tree, "d")
		}
		if err := os.MkdirAll(tree, 0o700); err != nil {
			t.Fatal(err)
		}
		got, err := collectEvidenceAt(t, work)
		if len(got) != 0 || err == nil || !strings.Contains(err.Error(), "16 directory levels") {
			t.Fatalf("unexpected collection result with %d files, error %v", len(got), err)
		}
	})
	t.Run("tree entries", func(t *testing.T) {
		work, tree := prepareEvidenceTree(t)
		for i := 0; i < maxEvidenceTreeEntries+1; i++ {
			if err := os.Mkdir(filepath.Join(tree, fmt.Sprintf("subdir-%d", i)), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		got, err := collectEvidenceAt(t, work)
		if len(got) != 0 || err == nil || !strings.Contains(err.Error(), "512 entries") {
			t.Fatalf("unexpected collection result with %d files, error %v", len(got), err)
		}
	})
}

func prepareEvidenceTree(t *testing.T) (string, string) {
	t.Helper()
	work := t.TempDir()
	tree := filepath.Join(work, driverEvidenceDir)
	if err := os.Mkdir(tree, 0o700); err != nil {
		t.Fatal(err)
	}
	return work, tree
}

func collectEvidenceAt(t *testing.T, work string) ([]driverEvidenceEntry, error) {
	t.Helper()
	root, err := os.OpenRoot(work)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	return collectDriverEvidence(root)
}

func TestCollectionKeepsOriginalWorkDirectory(t *testing.T) {
	parent := t.TempDir()
	work := filepath.Join(parent, "work")
	if err := os.Mkdir(work, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(work)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.Rename(work, filepath.Join(parent, "moved")); err != nil {
		t.Fatal(err)
	}
	outside, tree := prepareEvidenceTree(t)
	if err := os.WriteFile(filepath.Join(tree, "outside"), []byte("external data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, work); err != nil {
		t.Fatal(err)
	}
	if got, err := collectDriverEvidence(root); len(got) != 0 || err != nil {
		t.Fatalf("unexpected collection from the replacement path: %v %v", got, err)
	}
}
