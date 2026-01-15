package cli

import (
	"github.com/spf13/cobra"
)

// MigrateOptions holds CLI flags
type MigrateOptions struct {
	MappingFile string
	BatchSize   int
	DryRun      bool
	Resume      bool
	Validate    bool
}

// NewMigrateCmd creates the migrate command
func NewMigrateCmd() *cobra.Command {
	opts := &MigrateOptions{}

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "ETL operations with enhanced features",
		Long: `Migrate data between SQL Server and MongoDB.`,
	}

	cmd.PersistentFlags().StringVarP(&opts.MappingFile, "mapping", "m", "configs/user_mapping.json", "Path to mapping configuration file")
	cmd.PersistentFlags().IntVarP(&opts.BatchSize, "batch-size", "b", 100, "Number of records per batch")
	cmd.PersistentFlags().BoolVar(&opts.DryRun, "dry-run", false, "Validate without making changes")
	cmd.PersistentFlags().BoolVar(&opts.Resume, "resume", true, "Resume from last checkpoint")
	cmd.PersistentFlags().BoolVar(&opts.Validate, "validate", false, "Validate data before migration")

	sqlToMongo := &cobra.Command{
		Use:   "sql-to-mongo",
		Short: "Migrate data from SQL Server to MongoDB",
		RunE: func(c *cobra.Command, args []string) error {
			return runMigration(opts, "sql-to-mongo")
		},
	}

	mongoToSql := &cobra.Command{
		Use:   "mongo-to-sql",
		Short: "Migrate data from MongoDB to SQL Server",
		RunE: func(c *cobra.Command, args []string) error {
			return runMigration(opts, "mongo-to-sql")
		},
	}

	cmd.AddCommand(sqlToMongo, mongoToSql)
	return cmd
}
