package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/BartekS5/IDA/pkg/models"
)

// LoadMapping reads and parses the mapping.json file from the given path.
// It returns a pointer to a fully parsed MappingConfig struct or an error
// if the file cannot be read or parsed.
func LoadMapping(filePath string) (*models.MappingConfig, error) {
	// Read the file from disk
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read mapping file '%s': %w", filePath, err)
	}

	// Unmarshal (parse) the JSON
	var config models.MappingConfig
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, fmt.Errorf("failed to parse mapping file '%s': %w", filePath, err)
	}

	return &config, nil
}
