package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	rootCmd := &cobra.Command{
		Use:   "your-app",
		Short: "Your application description",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to your Go application!")
		},
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
