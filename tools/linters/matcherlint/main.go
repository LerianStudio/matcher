// Package main provides a combined linter for Matcher-specific patterns.
//
// This can be used with golangci-lint via custom linters or run standalone.
//
// Usage:
//
//	go run ./tools/linters/matcherlint/... ./...
//
// Or build as a plugin:
//
//	go build -buildmode=plugin -o matcherlint.so ./tools/linters/matcherlint
package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/LerianStudio/matcher/tools/linters/entityconstructor"
	"github.com/LerianStudio/matcher/tools/linters/observability"
	"github.com/LerianStudio/matcher/tools/linters/repositorytx"
)

func main() {
	multichecker.Main(
		entityconstructor.Analyzer,
		observability.Analyzer,
		repositorytx.Analyzer,
	)
}
