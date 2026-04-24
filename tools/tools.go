// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package tools imports tool dependencies for go mod management.
// This file is not compiled into the binary but ensures tool
// dependencies are tracked in go.mod.
package tools

import (
	_ "golang.org/x/tools/go/analysis"
	_ "golang.org/x/tools/go/analysis/multichecker"
	_ "golang.org/x/tools/go/analysis/passes/inspect"
	_ "golang.org/x/tools/go/ast/inspector"
)
