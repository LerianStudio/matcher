// Package observability provides a linter to enforce observability patterns in services.
//
// Service Execute/Run methods must:
// - Extract tracking via libCommons.NewTrackingFromContext(ctx)
// - Create a span with tracer.Start(ctx, "operation.name")
// - Defer span.End()
//
// This ensures all service operations are properly traced for production debugging.
package observability

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the observability pattern analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "observability",
	Doc:      "checks that service methods use proper tracking and span patterns",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

//nolint:nilnil // analysis.Analyzer.Run requires (any, error) return
func run(pass *analysis.Pass) (any, error) {
	if !isServicePackage(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.FuncDecl)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)

		if fn.Recv == nil || fn.Name == nil {
			return
		}

		name := fn.Name.Name
		if !isServiceMethod(name) {
			return
		}

		if strings.HasSuffix(pass.Fset.File(fn.Pos()).Name(), "_test.go") {
			return
		}

		if fn.Body == nil {
			return
		}

		checks := &observabilityChecks{}

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				checkCallExpr(node, checks)
			case *ast.DeferStmt:
				checkDeferStmt(node, checks)
			}

			return true
		})

		reportMissingPatterns(pass, fn, name, checks)
	})

	return nil, nil
}

type observabilityChecks struct {
	hasTrackingExtraction bool
	hasSpanStart          bool
	hasDeferSpanEnd       bool
}

func isServicePackage(pkgPath string) bool {
	return strings.Contains(pkgPath, "/services/command") ||
		strings.Contains(pkgPath, "/services/query")
}

func isServiceMethod(name string) bool {
	return name == "Execute" || name == "Run" || name == "Handle"
}

func checkCallExpr(call *ast.CallExpr, checks *observabilityChecks) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}

	if sel.Sel.Name == "NewTrackingFromContext" {
		checks.hasTrackingExtraction = true
	}

	if sel.Sel.Name == "Start" {
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "tracer" {
			checks.hasSpanStart = true
		}
	}
}

func checkDeferStmt(d *ast.DeferStmt, checks *observabilityChecks) {
	call, ok := d.Call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}

	if call.Sel.Name == "End" {
		if ident, ok := call.X.(*ast.Ident); ok && ident.Name == "span" {
			checks.hasDeferSpanEnd = true
		}
	}
}

func reportMissingPatterns(
	pass *analysis.Pass,
	fn *ast.FuncDecl,
	name string,
	checks *observabilityChecks,
) {
	if !checks.hasTrackingExtraction {
		pass.Reportf(
			fn.Pos(),
			"service method %s: missing NewTrackingFromContext call for observability",
			name,
		)
	}

	if !checks.hasSpanStart {
		pass.Reportf(fn.Pos(), "service method %s: missing tracer.Start() span creation", name)
	}

	if !checks.hasDeferSpanEnd {
		pass.Reportf(
			fn.Pos(),
			"service method %s: missing defer span.End() for proper span cleanup",
			name,
		)
	}
}
