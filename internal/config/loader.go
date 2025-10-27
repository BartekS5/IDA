// Package config handles loading and parsing of configuration files
// for the application, such as the main mapping.json file.
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
	// 1. Read the file from disk
	// os.ReadFile returns the file's content as a byte slice.
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		// Return a more user-friendly error
		return nil, fmt.Errorf("failed to read mapping file '%s': %w", filePath, err)
	}

	// 2. Unmarshal (parse) the JSON
	// We create an empty variable 'config'
	var config models.MappingConfig

	// We pass a *pointer* (&config) to json.Unmarshal.
	// This allows it to modify the 'config' variable directly.
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse mapping file '%s': %w", filePath, err)
	}

	// 3. Return the loaded config (and no error)
	return &config, nil
}
