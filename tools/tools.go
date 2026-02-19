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
