// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package goroutineleak provides a linter that flags packages spawning
// goroutines without a corresponding TestMain that installs
// goleak.VerifyTestMain.
//
// A package is flagged when it contains at least one file with either:
//   - A `go func(` or `go <identifier>(` spawn expression, or
//   - A call to runtime.SafeGo* helper (lib-commons panic-safe goroutine
//     helper, typically SafeGoWithContext / SafeGoWithContextAndComponent).
//
// …AND no test file in the same package declares a TestMain that
// references goleak.VerifyTestMain.
//
// The linter is intentionally NOT wired into the default lint-custom
// target — it runs only under `make lint-custom-strict` until every
// package it flags has leak coverage.
package goroutineleak

import (
	"go/ast"
	"go/token"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the goroutine-leak coverage analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "goroutineleak",
	Doc:      "flags packages spawning goroutines that lack a goleak.VerifyTestMain TestMain",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

//nolint:nilnil // analysis.Analyzer.Run requires (any, error) return
func run(pass *analysis.Pass) (any, error) {
	if skipPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	var (
		spawnPos        []token.Pos
		hasTestMain     bool
		hasGoleakVerify bool
	)

	nodeFilter := []ast.Node{
		(*ast.GoStmt)(nil),
		(*ast.FuncDecl)(nil),
		(*ast.CallExpr)(nil),
	}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		switch node := n.(type) {
		case *ast.GoStmt:
			if isTestFile(pass, node.Pos()) {
				return
			}

			spawnPos = append(spawnPos, node.Pos())
		case *ast.CallExpr:
			if !isTestFile(pass, node.Pos()) {
				if isSafeGoCall(node) {
					spawnPos = append(spawnPos, node.Pos())
				}

				return
			}

			if isGoleakVerifyTestMain(node) {
				hasGoleakVerify = true
			}
		case *ast.FuncDecl:
			if !isTestFile(pass, node.Pos()) {
				return
			}

			if node.Name != nil && node.Name.Name == "TestMain" {
				hasTestMain = true
			}
		}
	})

	if len(spawnPos) == 0 {
		return nil, nil
	}

	if hasTestMain && hasGoleakVerify {
		return nil, nil
	}

	pass.Reportf(
		spawnPos[0],
		"package %q spawns %d goroutine(s) but has no TestMain with goleak.VerifyTestMain — add one in a _test.go file using testutil.LeakOptions()",
		pass.Pkg.Path(),
		len(spawnPos),
	)

	return nil, nil
}

func isTestFile(pass *analysis.Pass, pos token.Pos) bool {
	file := pass.Fset.File(pos)
	if file == nil {
		return false
	}

	return strings.HasSuffix(file.Name(), "_test.go")
}

func isSafeGoCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if sel.Sel == nil {
		return false
	}

	return strings.HasPrefix(sel.Sel.Name, "SafeGo")
}

func isGoleakVerifyTestMain(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if sel.Sel == nil || sel.Sel.Name != "VerifyTestMain" {
		return false
	}

	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}

	return ident.Name == "goleak"
}

// skipPackage returns true for packages that should not be checked.
func skipPackage(path string) bool {
	switch {
	case strings.Contains(path, "/testutil"):
		return true
	case strings.Contains(path, "/tools/"):
		return true
	case strings.HasSuffix(path, "/mocks"):
		return true
	}

	return false
}
