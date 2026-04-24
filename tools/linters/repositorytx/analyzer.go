// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositorytx provides a linter to enforce repository transaction patterns.
//
// Repository write methods (Create, Update, Delete, MarkMatched, etc.) must:
// - Have a corresponding *WithTx variant that accepts a transaction parameter
// - Non-WithTx methods should use common.WithTenantTx wrapper internally
//
// This ensures transaction safety for financial data while maintaining tenant isolation.
package repositorytx

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the repository transaction pattern analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "repositorytx",
	Doc:      "checks that repository write methods have corresponding WithTx variants",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

var writeMethodPrefixes = []string{
	"Create",
	"Update",
	"Delete",
	"Save",
	"Insert",
	"Upsert",
	"MarkMatched",
	"MarkPending",
}

//nolint:nilnil // analysis.Analyzer.Run requires (any, error) return
func run(pass *analysis.Pass) (any, error) {
	if !isPostgresAdapter(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	methods := make(map[string]ast.Node)
	nodeFilter := []ast.Node{(*ast.FuncDecl)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)

		if fn.Recv == nil || fn.Name == nil {
			return
		}

		if strings.HasSuffix(pass.Fset.File(fn.Pos()).Name(), "_test.go") {
			return
		}

		methods[fn.Name.Name] = fn
	})

	for name, node := range methods {
		if !isWriteMethod(name) {
			continue
		}

		if strings.HasSuffix(name, "WithTx") {
			continue
		}

		withTxName := name + "WithTx"
		if _, exists := methods[withTxName]; !exists {
			pass.Reportf(
				node.Pos(),
				"repository method %s: missing %s variant for transaction safety",
				name,
				withTxName,
			)
		}
	}

	return nil, nil
}

func isPostgresAdapter(pkgPath string) bool {
	return strings.Contains(pkgPath, "/adapters/postgres")
}

func isWriteMethod(name string) bool {
	for _, prefix := range writeMethodPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}
