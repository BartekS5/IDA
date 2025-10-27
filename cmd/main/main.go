package main

import (
	"fmt"
	"log"
	"os"

	"github.com/BartekS5/IDA/internal/cli"

	"github.com/joho/godotenv"
)

func main() {
	// 1. Load .env file
	// This runs first, populating environment variables
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	// 2. Create the root command
	// All Cobra logic is now inside the cli package
	rootCmd := cli.NewRootCmd()

	// 3. Execute the application
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
