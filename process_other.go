//go:build !linux && !darwin

package agent

import (
	"os"
	"os/exec"
)

func configureProcess(_ *exec.Cmd) {}

func openOutput(root *os.Root, name string) (*os.File, error) {
	return root.Open(name)
}
