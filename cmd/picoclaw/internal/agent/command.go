package agent

import (
	"github.com/spf13/cobra"
)

func NewAgentCommand() *cobra.Command {
	var (
		message    string
		sessionKey string
		model      string
		agentID    string
		debug      bool
	)

	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Interact with the agent directly",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return agentCmd(message, sessionKey, model, agentID, debug)
		},
	}

	cmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	cmd.Flags().StringVarP(&message, "message", "m", "", "Send a single message (non-interactive mode)")
	cmd.Flags().StringVarP(&sessionKey, "session", "s", "cli:default", "Session key")
	cmd.Flags().StringVarP(&model, "model", "", "", "Model to use")
	cmd.Flags().StringVarP(&agentID, "agent", "a", "", "Agent ID to use (from agents.list)")

	return cmd
}
