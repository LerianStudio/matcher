//go:build unit

package tools

import (
	"testing"
)

func TestToolsPackageImports(t *testing.T) {
	t.Parallel()
	// This test ensures the tools package compiles correctly.
	// The package exists only to track tool dependencies in go.mod.
}
