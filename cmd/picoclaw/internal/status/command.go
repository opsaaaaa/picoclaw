package status

import (
	"github.com/spf13/cobra"
)

func NewStatusCommand() *cobra.Command {
	var agentID string

	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"s"},
		Short:   "Show picoclaw status",
		Run: func(cmd *cobra.Command, args []string) {
			statusCmd(agentID)
		},
	}

	cmd.Flags().StringVarP(&agentID, "agent", "a", "", "Show details for specific agent ID")

	return cmd
}
