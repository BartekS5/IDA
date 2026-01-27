package cli

import (
	"github.com/spf13/cobra"
)

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ida",
		Short: "IDA - project for SQL to MongoDB migration",
		Long: `IDA is a CLI tool for migrating data between SQL databases and MongoDB.
It supports bidirectional migration with configurable field mappings and type conversions.`,
		SilenceUsage: true,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	rootCmd.AddCommand(NewMigrateCmd())

	return rootCmd
}
