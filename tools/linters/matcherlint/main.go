// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/LerianStudio/matcher/tools/linters/determinism"
	"github.com/LerianStudio/matcher/tools/linters/entityconstructor"
	"github.com/LerianStudio/matcher/tools/linters/goroutineleak"
	"github.com/LerianStudio/matcher/tools/linters/observability"
	"github.com/LerianStudio/matcher/tools/linters/repositorytx"
)

func main() {
	// Goroutine-leak coverage is gated behind lint-custom-strict via
	// MATCHER_GOLEAK_LINTER — that package has been cleaned up and is a
	// hard gate.
	//
	// Determinism is still advisory: it fires on existing violations that
	// have not been cleaned up yet. A dedicated MATCHER_DETERMINISM_LINTER
	// env var lets lint-custom opt in without failing CI. Once the codebase
	// is clean the gate graduates to MATCHER_GOLEAK_LINTER alongside
	// goroutineleak.
	determinismEnabled := os.Getenv("MATCHER_DETERMINISM_LINTER") == "1"

	if os.Getenv("MATCHER_GOLEAK_LINTER") == "1" {
		analyzers := []*analysis.Analyzer{
			entityconstructor.Analyzer,
			observability.Analyzer,
			repositorytx.Analyzer,
			goroutineleak.Analyzer,
		}

		if determinismEnabled {
			analyzers = append(analyzers, determinism.Analyzer)
		}

		multichecker.Main(analyzers...)

		return
	}

	analyzers := []*analysis.Analyzer{
		entityconstructor.Analyzer,
		observability.Analyzer,
		repositorytx.Analyzer,
	}

	if determinismEnabled {
		analyzers = append(analyzers, determinism.Analyzer)
	}

	multichecker.Main(analyzers...)
}
