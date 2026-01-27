package config

import (
	"errors"
	"os"
)

type Config struct {
	SQLConnString   string
	MongoConnString string
}

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
