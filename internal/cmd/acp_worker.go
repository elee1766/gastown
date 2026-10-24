package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/acpworker"
)

func init() {
	var prompt string
	worker := &cobra.Command{Use: "acp-worker -- <agent> [args...]", Short: "Run a persistent ACP worker", Hidden: true, Args: cobra.MinimumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		defer stop()
		err = acpworker.Run(ctx, acpworker.Options{TownRoot: os.Getenv("GT_ROOT"), Session: os.Getenv("GT_SESSION"), Cwd: cwd, Socket: os.Getenv("GT_ACP_SOCKET"), InitialPrompt: prompt, Command: args})
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}}
	worker.Flags().StringVar(&prompt, "initial-prompt", "", "Initial ACP prompt")
	rootCmd.AddCommand(worker)
	control := &cobra.Command{Use: "acp-control <status|cancel|prompt|receipt> <session> [message-or-id]", Short: "Query or control an ACP worker", Hidden: true, Args: cobra.MinimumNArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		root := os.Getenv("GT_ROOT")
		if root == "" {
			return fmt.Errorf("GT_ROOT is required")
		}
		client := acpworker.NewClient(acpworker.SocketPath(root, args[1]))
		ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
		defer cancel()
		var result any
		var err error
		switch args[0] {
		case "status":
			result, err = client.Status(ctx)
		case "cancel":
			err = client.Cancel(ctx)
			result = map[string]bool{"cancel_requested": err == nil}
		case "prompt":
			if len(args) != 3 {
				return fmt.Errorf("provide one quoted message")
			}
			result, err = client.Prompt(ctx, acpworker.NewPromptID(), args[2])
		case "receipt":
			if len(args) != 3 {
				return fmt.Errorf("provide receipt ID")
			}
			result, err = client.Receipt(ctx, args[2])
		default:
			return fmt.Errorf("unknown ACP operation %q", args[0])
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
	}}
	rootCmd.AddCommand(control)
}
