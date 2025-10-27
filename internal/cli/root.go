// Package cli handles the command-line interface logic
// using the Cobra library.
package cli

import (
	"github.com/spf13/cobra"
)

// NewRootCmd creates and configures the main "root" command
// for the application. It attaches all sub-commands.
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ida",
		Short: "IDA - project for SQL to MongoDB migration",
		Long: `IDA is a CLI tool for migrating data between SQL databases and MongoDB.
It supports bidirectional migration with configurable field mappings and type conversions.`,
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			// If no sub-command is provided, show help
			cmd.Help()
		},
	}

	// Attach sub-commands
	rootCmd.AddCommand(NewMigrateCmd())

	return rootCmd
}
