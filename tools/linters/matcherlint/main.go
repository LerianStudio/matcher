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
	"os"

	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/LerianStudio/matcher/tools/linters/entityconstructor"
	"github.com/LerianStudio/matcher/tools/linters/goroutineleak"
	"github.com/LerianStudio/matcher/tools/linters/observability"
	"github.com/LerianStudio/matcher/tools/linters/repositorytx"
)

func main() {
	// Goroutine-leak coverage is gated behind lint-custom-strict until every
	// flagged package has goleak.VerifyTestMain wired. Enable via the
	// MATCHER_GOLEAK_LINTER env var, which the Makefile sets for the
	// strict target.
	if os.Getenv("MATCHER_GOLEAK_LINTER") == "1" {
		multichecker.Main(
			entityconstructor.Analyzer,
			observability.Analyzer,
			repositorytx.Analyzer,
			goroutineleak.Analyzer,
		)

		return
	}

	multichecker.Main(
		entityconstructor.Analyzer,
		observability.Analyzer,
		repositorytx.Analyzer,
	)
}
