package cli

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/BartekS5/IDA/internal/config"
	"github.com/BartekS5/IDA/pkg/database"
	"github.com/BartekS5/IDA/pkg/models"
)

func runMigrate(opts *MigrateOptions) error {
	log.Printf("Loading mapping file: %s", opts.MappingFile)

	// Load the mapping configuration
	mappingConf, err := config.LoadMapping(opts.MappingFile)
	if err != nil {
		return fmt.Errorf("failed to load mapping config: %w", err)
	}

	// Validate the mapping configuration
	if err := validateMappingConfig(mappingConf); err != nil {
		return fmt.Errorf("invalid mapping config: %w", err)
	}

	log.Printf("Successfully loaded configuration version: %.1f", mappingConf.Version)

	// Find the requested task
	task, err := findTask(mappingConf, opts.TaskName)
	if err != nil {
		return err
	}

	// Load database connection configuration
	dbConf, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load database config: %w", err)
	}

	// Display task information
	printTaskInfo(task)

	// --- Connect to Databases ---
	log.Printf("Connecting to MS SQL: %s", maskConnectionString(dbConf.SQLConnString))
	sqlDB, err := database.ConnectSQL(dbConf.SQLConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to SQL database: %w", err)
	}
	defer sqlDB.Close() // Ensure connection is closed when function returns

	log.Printf("Connecting to MongoDB: %s", maskConnectionString(dbConf.MongoConnString))
	mongoClient, err := database.ConnectMongo(dbConf.MongoConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	// Ensure client is disconnected when function returns
	defer func() {
		log.Println("Disconnecting from MongoDB...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = mongoClient.Disconnect(ctx); err != nil {
			log.Printf("Warning: error disconnecting from MongoDB: %v", err)
		}
	}()

	// TODO: Pass connections (sqlDB, mongoClient) and task to the ETL engine
	log.Println("Ready to execute migration (ETL engine not yet implemented)")

	return nil
}

// findTask searches for a migration task by name in the configuration
func findTask(conf *models.MappingConfig, taskName string) (*models.MigrationTask, error) {
	for i := range conf.MigrationTasks {
		if conf.MigrationTasks[i].Name == taskName {
			return &conf.MigrationTasks[i], nil
		}
	}
	return nil, fmt.Errorf("task '%s' not found in mapping file", taskName)
}

// printTaskInfo displays information about the migration task
func printTaskInfo(task *models.MigrationTask) {
	log.Println("----------------------------------")
	log.Printf("Migration Task: '%s'", task.Name)
	log.Printf("Source Table: %s (PK: %s)", task.SQLEntity.TableName, task.SQLEntity.PrimaryKey)
	log.Printf("Target Collection: %s (Upsert Key: %s)", task.MongoEntity.CollectionName, task.MongoEntity.UpsertKey)
	log.Printf("Field Mappings: %d", len(task.FieldMappings))
	log.Println("----------------------------------")
}

// maskConnectionString masks sensitive parts of connection strings for logging
func maskConnectionString(connStr string) string {
	if len(connStr) < 10 {
		return "***"
	}
	return connStr[:10] + "...***"
}

// validateMappingConfig performs basic validation on the mapping configuration
func validateMappingConfig(conf *models.MappingConfig) error {
	// Check that we have at least one task
	if len(conf.MigrationTasks) == 0 {
		return fmt.Errorf("no migration tasks defined")
	}

	// Validate each task
	for i, task := range conf.MigrationTasks {
		if task.Name == "" {
			return fmt.Errorf("task #%d: name is required", i)
		}
		if task.SQLEntity.TableName == "" {
			return fmt.Errorf("task '%s': SQL table name is required", task.Name)
		}
		if task.SQLEntity.PrimaryKey == "" {
			return fmt.Errorf("task '%s': SQL primary key is required", task.Name)
		}
		if task.MongoEntity.CollectionName == "" {
			return fmt.Errorf("task '%s': MongoDB collection name is required", task.Name)
		}
		if task.MongoEntity.UpsertKey == "" {
			return fmt.Errorf("task '%s': MongoDB upsert key is required", task.Name)
		}
	}

	// Validate global settings
	validNullHandlers := map[string]bool{
		"omit_field":  true,
		"set_null":    true,
		"use_default": true,
	}
	if !validNullHandlers[conf.GlobalSettings.OnSQLNull] {
		return fmt.Errorf("invalid on_sql_null value: %s", conf.GlobalSettings.OnSQLNull)
	}

	validMissingHandlers := map[string]bool{
		"set_to_sql_null": true,
		"skip_field":      true,
		"use_default":     true,
	}
	if !validMissingHandlers[conf.GlobalSettings.OnMongoMissing] {
		return fmt.Errorf("invalid on_mongo_missing value: %s", conf.GlobalSettings.OnMongoMissing)
	}

	return nil
}
