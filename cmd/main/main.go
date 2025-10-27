package main

import (
	"log"
	"os"

	"github.com/BartekS5/IDA/internal/cli"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// Create and execute the root command
	rootCmd := cli.NewRootCmd()
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
