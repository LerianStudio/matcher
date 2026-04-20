// Package main renders the Casdoor RBAC seed file from Matcher's auth catalog.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LerianStudio/matcher/internal/auth"
)

const (
	casdoorDirMode  = 0o755
	casdoorFileMode = 0o644
)

func main() {
	output := flag.String("output", filepath.Join("config", "casdoor", "init_data.json"), "output file path")

	flag.Parse()

	if err := run(*output); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func run(output string) error {
	contents, err := auth.MarshalCasdoorInitData()
	if err != nil {
		return fmt.Errorf("generate casdoor init data: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(output), casdoorDirMode); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	if err := os.WriteFile(output, contents, casdoorFileMode); err != nil {
		return fmt.Errorf("write output file: %w", err)
	}

	return nil
}
