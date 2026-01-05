package cli

import (
	"github.com/spf13/cobra"
)

type MigrateOptions struct {
	MappingFile string
	BatchSize   int
}

func NewMigrateCmd() *cobra.Command {
	opts := &MigrateOptions{}

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "ETL operations",
	}

	cmd.PersistentFlags().StringVarP(&opts.MappingFile, "mapping", "m", "configs/mapping.json", "Path to mapping file")
	cmd.PersistentFlags().IntVarP(&opts.BatchSize, "batch-size", "b", 100, "Batch size")

	sqlToMongo := &cobra.Command{
		Use:   "sql-to-mongo",
		Short: "Run migration from SQL to MongoDB",
		RunE: func(c *cobra.Command, args []string) error {
			return runMigration(opts, "sql-to-mongo")
		},
	}

	mongoToSql := &cobra.Command{
		Use:   "mongo-to-sql",
		Short: "Run migration from MongoDB to SQL",
		RunE: func(c *cobra.Command, args []string) error {
			return runMigration(opts, "mongo-to-sql")
		},
	}

	cmd.AddCommand(sqlToMongo, mongoToSql)
	return cmd
}
