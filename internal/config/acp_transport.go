package config

import (
	"strings"

	"github.com/steveyegge/gastown/internal/acpworker"
	"github.com/steveyegge/gastown/internal/cli"
)

func (rc *RuntimeConfig) acpArgs(prompt string) []string {
	args := []string{cli.Name(), "acp-worker"}
	acp := GetACPConfigFromRuntime(rc)
	if acp == nil {
		return append(args, "--missing-acp-configuration")
	}
	if prompt == "" {
		prompt = rc.InitialPrompt
	}
	if prompt != "" {
		args = append(args, "--initial-prompt", prompt)
	}
	args = append(args, "--", rc.Command)
	args = append(args, rc.Args...)
	if acp.Mode != ACPModeNative && acp.Command != "" {
		args = append(args, acp.Command)
	}
	if acp.Mode != ACPModeNative {
		args = append(args, acp.Args...)
	}
	return args
}

func shellCommand(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = ShellQuote(arg)
	}
	return strings.Join(quoted, " ")
}

// RuntimeTransportEnv is shared by shell launch and tmux metadata creation.
func RuntimeTransportEnv(env map[string]string, rc *RuntimeConfig) {
	env["GT_RUNTIME_TRANSPORT"] = "tmux"
	env["GT_ACP_SOCKET"] = ""
	if rc != nil && rc.Transport == "acp" {
		env["GT_RUNTIME_TRANSPORT"] = "acp"
		if env["GT_ROOT"] != "" && env["GT_SESSION"] != "" {
			env["GT_ACP_SOCKET"] = acpworker.SocketPath(env["GT_ROOT"], env["GT_SESSION"])
		}
	}
}
