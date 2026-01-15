package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/BartekS5/IDA/internal/config"
	"github.com/BartekS5/IDA/internal/etl"
	"github.com/BartekS5/IDA/pkg/database"
	"github.com/BartekS5/IDA/pkg/logger"
	"github.com/BartekS5/IDA/pkg/models"
)

// runMigration executes the migration with enhanced features
func runMigration(opts *MigrateOptions, direction string) error {
	// Initialize logger
	logFile := fmt.Sprintf("migration_%s_%s.log", direction, getCurrentTimestamp())
	if err := logger.InitLogger(logFile, logger.INFO); err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Close()
	
	logger.Info("=== Starting %s Migration ===", direction)
	logger.Info("Mapping file: %s", opts.MappingFile)
	logger.Info("Batch size: %d", opts.BatchSize)
	if opts.DryRun {
		logger.Info("DRY RUN MODE - No data will be modified")
	}
	
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Failed to load configuration: %v", err)
		return err
	}
	
	// Load mapping
	mappingData, err := os.ReadFile(opts.MappingFile)
	if err != nil {
		logger.Error("Failed to read mapping file: %v", err)
		return fmt.Errorf("failed to read mapping file: %w", err)
	}
	
	mappingSchema, err := models.LoadMapping(mappingData)
	if err != nil {
		logger.Error("Failed to parse mapping JSON: %v", err)
		return fmt.Errorf("failed to parse mapping JSON: %w", err)
	}
	
	logger.Info("Loaded mapping for entity: %s", mappingSchema.Entity)
	
	// Connect to databases
	logger.Info("Connecting to databases...")
	
	sqlDB, err := database.ConnectSQL(cfg.SQLConnString)
	if err != nil {
		logger.Error("Failed to connect to SQL: %v", err)
		return err
	}
	defer sqlDB.Close()
	
	mongoClient, err := database.ConnectMongo(cfg.MongoConnString)
	if err != nil {
		logger.Error("Failed to connect to MongoDB: %v", err)
		return err
	}
	defer mongoClient.Disconnect(context.Background())
	
	logger.Info("Database connections established")
	
	// Setup pipeline
	var extractor etl.Extractor
	var loader etl.Loader
	
	if direction == "sql-to-mongo" {
		extractor = &etl.SQLToMongoExtractor{DB: sqlDB, Config: mappingSchema}
		// Use NewMongoLoader to ensure transformer is initialized
		loader = etl.NewMongoLoader(mongoClient, mappingSchema)
	} else {
		extractor = &etl.MongoToSQLExtractor{Client: mongoClient, Config: mappingSchema}
		// Use NewSQLLoader
		loader = etl.NewSQLLoader(sqlDB, mappingSchema)
	}
	
	// Passed opts.DryRun to the pipeline
	pipeline := etl.NewEnhancedPipeline(extractor, loader, opts.BatchSize, opts.DryRun)
	
	// Run migration
	logger.Info("Starting migration execution...")
	if err := pipeline.Run(); err != nil {
		logger.Error("Migration failed: %v", err)
		return err
	}
	
	logger.Info("Migration completed successfully!")
	return nil
}

// getCurrentTimestamp returns current timestamp for file naming
func getCurrentTimestamp() string {
	return time.Now().Format("20060102_150405")
}
