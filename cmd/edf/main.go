package main

import (
	"os"

	"expired-domain-finder/internal/cli"
)

func main() {
	os.Exit(cli.ExecuteWithInput(os.Args[1:], os.Stdout, os.Stderr, os.Stdin))
}
