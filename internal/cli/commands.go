// Package cli handles the command-line interface logic
// using the Cobra library.
package cli

import (
	"log"

	"github.com/BartekS5/IDA/internal/config"
	"github.com/BartekS5/IDA/pkg/models"
	"github.com/spf13/cobra"
)

// NewRootCmd creates and configures the main "root" command
// for the application. It attaches all sub-commands.
func NewRootCmd() *cobra.Command {
	// --- Root Command ---
	rootCmd := &cobra.Command{
		Use:   "your-app",
		Short: "A CLI tool for SQL to Mongo (and back) migration.",
		Run: func(cmd *cobra.Command, args []string) {
			// If the user just runs "your-app" with no sub-command,
			// show them the help menu.
			cmd.Help()
		},
	}

	// --- Attach Sub-commands ---
	// We call our new function to create and attach the migrate command
	rootCmd.AddCommand(newMigrateCmd())
	// You could add more commands here later, e.g.:
	// rootCmd.AddCommand(newValidateCmd())

	return rootCmd
}

// newMigrateCmd creates and configures the "migrate" sub-command.
func newMigrateCmd() *cobra.Command {
	// --- Define flag variables ---
	// These are scoped just to the migrate command
	var mappingFile string
	var taskName string

	// --- Migrate Command ---
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run an ETL migration task defined in the mapping file",
		Run: func(cmd *cobra.Command, args []string) {
			// The Run function is now just a clean wrapper
			// that calls the actual logic function.
			runMigrateCmd(cmd, args, mappingFile, taskName)
		},
	}

	// --- Add Flags to Migrate Command ---
	migrateCmd.Flags().StringVarP(&mappingFile, "mapping-file", "f", "", "Path to the mapping.json file")
	migrateCmd.Flags().StringVarP(&taskName, "task-name", "t", "", "The specific migration task name to run")

	// Mark flags as required
	migrateCmd.MarkFlagRequired("mapping-file")
	migrateCmd.MarkFlagRequired("task-name")

	return migrateCmd
}

// runMigrateCmd contains the actual execution logic for the migrate command.
func runMigrateCmd(cmd *cobra.Command, args []string, mappingFile string, taskName string) {
	// This is the code that runs when a user types:
	// "go run ./cmd/main/main.go migrate -f configs/mapping.json -t Users-Migration"

	// 1. Call your config loader
	log.Println("Attempting to load mapping file:", mappingFile)
	conf, err := config.LoadMapping(mappingFile)
	if err != nil {
		// log.Fatalf will print the error and exit the application
		log.Fatalf("ERROR: Could not load config: %v", err)
	}

	log.Println("Successfully loaded configuration version:", conf.Version)

	// 2. Find the requested task
	var taskFound *models.MigrationTask
	for i := range conf.MigrationTasks {
		if conf.MigrationTasks[i].Name == taskName {
			// Found it! Store a pointer to it.
			taskFound = &conf.MigrationTasks[i]
			break
		}
	}

	// 3. Report the result
	if taskFound != nil {
		log.Printf("----------------------------------")
		log.Printf("Successfully found task: '%s'", taskFound.Name)
		log.Printf("Source Table: %s", taskFound.SQLEntity.TableName)
		log.Printf("Target Collection: %s", taskFound.MongoEntity.CollectionName)
		log.Printf("----------------------------------")
		// Later, you will pass 'taskFound' to Student A's ETL engine.
	} else {
		log.Fatalf("ERROR: Could not find task with name '%s' in mapping file.", taskName)
	}
}
