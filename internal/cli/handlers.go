package cli

import (
	"fmt"
	"os"

	"github.com/BartekS5/IDA/internal/config"
	"github.com/BartekS5/IDA/internal/etl"
	"github.com/BartekS5/IDA/pkg/database"
	"github.com/BartekS5/IDA/pkg/models"
)

func runMigration(opts *MigrateOptions, direction string) error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return err
	}

	mappingData, err := os.ReadFile(opts.MappingFile)
	if err != nil {
		return fmt.Errorf("failed to read mapping file: %w", err)
	}
	mappingSchema, err := models.LoadMapping(mappingData)
	if err != nil {
		return fmt.Errorf("failed to parse mapping JSON: %w", err)
	}

	sqlDB, err := database.ConnectSQL(cfg.SQLConnString)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	mongoClient, err := database.ConnectMongo(cfg.MongoConnString)
	if err != nil {
		return err
	}
	defer func() {
	}()

	var extractor etl.Extractor
	var loader etl.Loader

	if direction == "sql-to-mongo" {
		extractor = &etl.SQLToMongoExtractor{DB: sqlDB, Config: mappingSchema}
		loader = &etl.MongoLoader{Client: mongoClient, Config: mappingSchema}
	} else {
		extractor = &etl.MongoToSQLExtractor{Client: mongoClient, Config: mappingSchema}
		loader = &etl.SQLLoader{DB: sqlDB, Config: mappingSchema}
	}

	pipeline := etl.NewPipeline(extractor, loader, opts.BatchSize)

	fmt.Printf("Starting %s migration for entity %s...\n", direction, mappingSchema.Entity)
	if err := pipeline.Run(); err != nil {
		return err
	}

	fmt.Println("Migration finished successfully.")
	return nil
}
