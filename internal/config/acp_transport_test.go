package config

import (
	"reflect"
	"testing"

	"github.com/steveyegge/gastown/internal/cli"
)

func TestACPLaunchSeparatesPromptFromAgentArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		acp  ACPConfig
		tail []string
	}{
		{"subcommand", ACPConfig{Command: "acp", Args: []string{"--profile", "full"}}, []string{"acp", "--profile", "full"}},
		{"flag", ACPConfig{Mode: ACPModeFlag, Args: []string{"--acp"}}, []string{"--acp"}},
		{"native", ACPConfig{Mode: ACPModeNative}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc := &RuntimeConfig{Provider: "generic", Transport: "acp", Command: "agent-wrapper", Args: []string{"--model", "selected"}, PromptMode: "none", ACP: &tc.acp}
			got := fillRuntimeDefaults(rc).BuildArgsWithPrompt("task with 'quotes'\nand lines")
			want := []string{cli.Name(), "acp-worker", "--initial-prompt", "task with 'quotes'\nand lines", "--", "agent-wrapper", "--model", "selected"}
			want = append(want, tc.tail...)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("launch argv\ngot  %q\nwant %q", got, want)
			}
		})
	}
}
