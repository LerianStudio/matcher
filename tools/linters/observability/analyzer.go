// Package observability provides a linter to enforce observability patterns in services.
//
// Exported methods on a *UseCase receiver declared under services/command or
// services/query must:
//   - Extract tracking via libCommons.NewTrackingFromContext(ctx)
//   - Create a span with tracer.Start(ctx, "operation.name")
//   - Defer span.End()
//
// Detection is path-based (services/command|query + *UseCase receiver + exported
// name) rather than name-based, because matcher's service methods use
// domain-specific verbs (RunMatch, ManualMatch, OpenDispute, AdjustEntry, …)
// rather than generic Execute/Run/Handle. A name allow-list produces
// false-negative clean passes by silently ignoring every real service method.
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
	Doc:      "checks that exported *UseCase methods use proper tracking and span patterns",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

// useCaseReceiverTypeName is the struct name every service entry point hangs
// off of in matcher. Keeping this as a constant (rather than detecting any
// receiver) prevents the linter from flagging exported helpers on adapter or
// value-object structs that happen to live under services/command.
const useCaseReceiverTypeName = "UseCase"

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

		if !isServiceMethod(fn) {
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

		reportMissingPatterns(pass, fn, fn.Name.Name, checks)
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

// isServiceMethod reports whether fn is an exported method on a *UseCase
// receiver whose first parameter is context.Context. Detection is structural
// (receiver type + exported name + ctx signature) rather than name-based so
// domain-specific verbs like RunMatch/ManualMatch/OpenDispute/AdjustEntry
// are all covered automatically.
//
// The ctx-as-first-param gate excludes two legitimate non-traceable patterns:
//   - Functional-option setters (With* methods called at bootstrap time with
//     no ctx, no I/O, no errors — pure field mutation).
//   - Trivial getters (SupportsStreaming, IsReady, …) that return internal
//     state without touching I/O.
//
// Both categories have no tracing value and fail naturally by lacking a ctx
// parameter. If a future caller needs tracing in an option setter (e.g.,
// because it starts performing I/O), adding ctx as the first argument will
// re-enable the analyzer automatically.
func isServiceMethod(fn *ast.FuncDecl) bool {
	if fn.Name == nil || !fn.Name.IsExported() {
		return false
	}

	if !hasUseCaseReceiver(fn) {
		return false
	}

	return hasContextFirstParam(fn)
}

// hasContextFirstParam reports whether fn's first non-receiver parameter has
// the syntactic form `context.Context`. We match by AST shape rather than
// resolving types because the analyzer runs before go/types on fixture code
// and the syntactic form is the canonical Go convention — no ambiguity.
func hasContextFirstParam(fn *ast.FuncDecl) bool {
	if fn.Type == nil || fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
		return false
	}

	first := fn.Type.Params.List[0].Type

	sel, ok := first.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}

	return pkg.Name == "context" && sel.Sel.Name == "Context"
}

// hasUseCaseReceiver reports whether fn's receiver is *UseCase or UseCase.
// Pointer and value receivers are both accepted; in practice matcher always
// uses pointer receivers, but accepting both makes the linter robust to
// future stylistic changes.
func hasUseCaseReceiver(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}

	recvType := fn.Recv.List[0].Type

	// Strip pointer receiver: *UseCase -> UseCase.
	if star, ok := recvType.(*ast.StarExpr); ok {
		recvType = star.X
	}

	ident, ok := recvType.(*ast.Ident)
	if !ok {
		return false
	}

	return ident.Name == useCaseReceiverTypeName
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
