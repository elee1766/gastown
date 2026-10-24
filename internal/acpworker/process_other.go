//go:build !linux

package acpworker

import "os/exec"

func configureProcess(cmd *exec.Cmd) {}
func stopProcess(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
func killProcess(cmd *exec.Cmd) { stopProcess(cmd) }
