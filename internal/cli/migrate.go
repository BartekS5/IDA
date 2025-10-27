package cli

import (
	"github.com/spf13/cobra"
)

// MigrateOptions holds the flags for the migrate command
type MigrateOptions struct {
	MappingFile string
	TaskName    string
}

// NewMigrateCmd creates and configures the "migrate" sub-command.
func NewMigrateCmd() *cobra.Command {
	opts := &MigrateOptions{}

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run an ETL migration task",
		Long: `Execute a migration task defined in the mapping configuration file.
The task will migrate data between SQL and MongoDB based on the field mappings.`,
		Example: `  ida migrate -f configs/mapping.json -t Users-Migration
  ida migrate --mapping-file configs/mapping.json --task-name Orders-Migration`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Delegate to the handler
			return runMigrate(opts)
		},
	}

	// Define flags
	cmd.Flags().StringVarP(&opts.MappingFile, "mapping-file", "f", "", "Path to the mapping.json file (required)")
	cmd.Flags().StringVarP(&opts.TaskName, "task-name", "t", "", "The specific migration task name to run (required)")

	// Mark flags as required
	cmd.MarkFlagRequired("mapping-file")
	cmd.MarkFlagRequired("task-name")

	return cmd
}
