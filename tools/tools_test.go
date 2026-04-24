// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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
