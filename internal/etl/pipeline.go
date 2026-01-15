package etl

import (
	"fmt"
	"os"
	"time"

	"github.com/BartekS5/IDA/pkg/logger"
)

type Pipeline struct {
	Extractor Extractor
	Loader    Loader
	BatchSize int
	DryRun    bool
}

// NewEnhancedPipeline creates a pipeline with dry-run support
// Note: Transformer is passed to Loaders, not Pipeline, to keep concerns separated.
func NewEnhancedPipeline(ext Extractor, loader Loader, batchSize int, dryRun bool) *Pipeline {
	return &Pipeline{
		Extractor: ext,
		Loader:    loader,
		BatchSize: batchSize,
		DryRun:    dryRun,
	}
}

// Deprecated: Use NewEnhancedPipeline
func NewPipeline(ext Extractor, loader Loader, batchSize int) *Pipeline {
	return NewEnhancedPipeline(ext, loader, batchSize, false)
}

func (p *Pipeline) Run() error {
	checkpointFile := "checkpoint.txt"
	startOffset := loadCheckpoint(checkpointFile)
	
	logger.Infof("Starting pipeline. Batch Size: %d, Start Offset: %v, DryRun: %v", p.BatchSize, startOffset, p.DryRun)
	
	offset := startOffset
	totalProcessed := 0
	startTime := time.Now()

	for {
		// 1. Extract
		data, newOffset, err := p.Extractor.Extract(p.BatchSize, offset)
		if err != nil {
			logger.Errorf("Extraction failed at offset %v: %v", offset, err)
			return err
		}
		
		count := len(data)
		if count == 0 {
			logger.Info("No more data to process. Migration complete.")
			break
		}

		// 2. Load (Skip if DryRun)
		if !p.DryRun {
			if err := p.Loader.Load(data); err != nil {
				logger.Errorf("Loading failed at offset %v: %v", offset, err)
				return err
			}
		} else {
			logger.Infof("[DRY RUN] Would load %d records", count)
		}

		// 3. Checkpoint & Stats
		totalProcessed += count
		offset = newOffset
		
		if !p.DryRun {
			saveCheckpoint(checkpointFile, offset)
		}
		
		duration := time.Since(startTime)
		rate := 0.0
		if duration.Seconds() > 0 {
			rate = float64(totalProcessed) / duration.Seconds()
		}
		logger.Infof("Batch done. Total: %d. Rate: %.2f docs/sec. New Offset: %v", totalProcessed, rate, offset)
	}
	
	if !p.DryRun {
		os.Remove(checkpointFile)
	}
	logger.Info("Pipeline finished successfully.")
	return nil
}

func loadCheckpoint(filename string) interface{} {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil // Return nil to let Extractor decide default
	}
	return string(data)
}

func saveCheckpoint(filename string, offset interface{}) {
	str := fmt.Sprintf("%v", offset)
	_ = os.WriteFile(filename, []byte(str), 0644)
}
