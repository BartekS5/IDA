package etl

import (
	"fmt"
	"log"
	"os"
)

// Pipeline manages the ETL process.
type Pipeline struct {
	Extractor Extractor
	Loader    Loader
	BatchSize int
}

func NewPipeline(ext Extractor, loader Loader, batchSize int) *Pipeline {
	return &Pipeline{
		Extractor: ext,
		Loader:    loader,
		BatchSize: batchSize,
	}
}

func (p *Pipeline) Run() error {
	checkpointFile := "checkpoint.txt"
	startOffset := loadCheckpoint(checkpointFile)
	log.Printf("Starting migration from checkpoint/offset: %v", startOffset)

	offset := startOffset
	for {
		// 1. Extract
		data, newOffset, err := p.Extractor.Extract(p.BatchSize, offset)
		if err != nil {
			return fmt.Errorf("extraction failed: %w", err)
		}
		if len(data) == 0 {
			log.Println("Migration completed. No more data.")
			break
		}

		// 2. Load (Transformation happens inside Adapter for simplicity in this dynamic schema)
		if err := p.Loader.Load(data); err != nil {
			return fmt.Errorf("loading failed at offset %v: %w", offset, err)
		}

		// 3. Checkpoint
		offset = newOffset
		saveCheckpoint(checkpointFile, offset)
		log.Printf("Processed batch. New offset: %v", offset)
	}
	
	// Cleanup
	os.Remove(checkpointFile)
	return nil
}

func loadCheckpoint(filename string) interface{} {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil // No checkpoint implies start from beginning
	}
	return string(data)
}

func saveCheckpoint(filename string, offset interface{}) {
	str := fmt.Sprintf("%v", offset)
	_ = os.WriteFile(filename, []byte(str), 0644)
}
