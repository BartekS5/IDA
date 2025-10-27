// Package config handles loading and parsing of configuration files
// for the application, such as the main mapping.json file.
package config

import (
	"errors"
	"os"
)

// Config holds all configuration for the application,
// typically loaded from environment variables.
type Config struct {
	SQLConnString   string
	MongoConnString string
}

// LoadConfig loads application settings from environment variables
// (which should be populated by the .env file in main.go).
func LoadConfig() (*Config, error) {
	sqlConn := os.Getenv("SQL_CONNECTION_STRING")
	if sqlConn == "" {
		return nil, errors.New("SQL_CONNECTION_STRING environment variable not set")
	}

	mongoConn := os.Getenv("MONGO_CONNECTION_STRING")
	if mongoConn == "" {
		return nil, errors.New("MONGO_CONNECTION_STRING environment variable not set")
	}

	return &Config{
		SQLConnString:   sqlConn,
		MongoConnString: mongoConn,
	}, nil
}
