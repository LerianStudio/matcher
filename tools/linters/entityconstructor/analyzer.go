// Package entityconstructor provides a linter to enforce entity constructor patterns.
//
// Entity constructors in domain/entities packages must:
// - Be named New<EntityName> matching the returned type
// - Return (*EntityName, error) tuple
// - Accept context.Context as first parameter
// - Use pkg/assert for validation (not panic)
//
// This enforces DDD best practices where entities maintain their invariants
// through constructor validation.
package entityconstructor

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the entity constructor pattern analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "entityconstructor",
	Doc:      "checks that entity constructors follow the New<Type>(ctx, ...) (*Type, error) pattern",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

//nolint:nilnil // analysis.Analyzer.Run requires (any, error) return
func run(pass *analysis.Pass) (any, error) {
	if !isEntityPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.FuncDecl)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)

		if fn.Name == nil || !fn.Name.IsExported() {
			return
		}

		name := fn.Name.Name
		if !strings.HasPrefix(name, "New") {
			return
		}

		if fn.Recv != nil {
			return
		}

		if strings.HasSuffix(pass.Fset.File(fn.Pos()).Name(), "_test.go") {
			return
		}

		expectedTypeName := strings.TrimPrefix(name, "New")
		if expectedTypeName == "" {
			return
		}

		// Skip empty factory functions (e.g., NewEmptyTrendMetrics) - these create
		// zero-value structs without validation requirements
		if strings.HasPrefix(expectedTypeName, "Empty") {
			return
		}

		// Skip event constructors (e.g., NewIngestionCompletedEvent) - these are
		// DTOs/events, not domain entities requiring invariant validation
		if strings.HasSuffix(expectedTypeName, "Event") {
			return
		}

		checkContextParam(pass, fn, name)
		checkReturnType(pass, fn, name, expectedTypeName)
	})

	return nil, nil
}

func isEntityPackage(pkgPath string) bool {
	return strings.Contains(pkgPath, "/domain/entities") ||
		strings.Contains(pkgPath, "/domain/dispute") ||
		strings.Contains(pkgPath, "/domain/fee")
}

func checkContextParam(pass *analysis.Pass, fn *ast.FuncDecl, funcName string) {
	if fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
		pass.Reportf(
			fn.Pos(),
			"entity constructor %s must have context.Context as first parameter",
			funcName,
		)

		return
	}

	firstParam := fn.Type.Params.List[0]

	paramType := pass.TypesInfo.TypeOf(firstParam.Type)
	if paramType == nil {
		return
	}

	if !isContextType(paramType) {
		pass.Reportf(
			firstParam.Pos(),
			"entity constructor %s: first parameter must be context.Context, got %s",
			funcName,
			paramType.String(),
		)
	}
}

func isContextType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}

	obj := named.Obj()
	if obj == nil {
		return false
	}

	return obj.Pkg() != nil && obj.Pkg().Path() == "context" && obj.Name() == "Context"
}

func checkReturnType(pass *analysis.Pass, fn *ast.FuncDecl, funcName, expectedTypeName string) {
	results := fn.Type.Results
	if results == nil || len(results.List) != 2 {
		pass.Reportf(
			fn.Pos(),
			"entity constructor %s must return (*%s, error)",
			funcName,
			expectedTypeName,
		)

		return
	}

	firstResult := results.List[0]

	firstType := pass.TypesInfo.TypeOf(firstResult.Type)
	if firstType == nil {
		return
	}

	ptr, ok := firstType.(*types.Pointer)
	if !ok {
		pass.Reportf(
			firstResult.Pos(),
			"entity constructor %s: first return must be *%s, got %s",
			funcName,
			expectedTypeName,
			firstType.String(),
		)

		return
	}

	elemType := ptr.Elem()
	actualTypeName := getTypeName(elemType)

	if actualTypeName == "" {
		pass.Reportf(
			firstResult.Pos(),
			"entity constructor %s: first return must be *%s",
			funcName,
			expectedTypeName,
		)

		return
	}

	if !strings.EqualFold(actualTypeName, expectedTypeName) {
		if matchesASTTypeName(firstResult.Type, expectedTypeName) {
			goto checkError
		}

		// Allow variant constructors (e.g., NewMatchItemWithPolicy returning *MatchItem)
		// These are factory variants that create the base type with specific configuration
		if isVariantConstructor(expectedTypeName, actualTypeName) {
			goto checkError
		}

		pass.Reportf(
			firstResult.Pos(),
			"entity constructor %s: expected *%s, got *%s",
			funcName,
			expectedTypeName,
			actualTypeName,
		)
	}

checkError:
	secondResult := results.List[1]

	secondType := pass.TypesInfo.TypeOf(secondResult.Type)
	if secondType == nil {
		return
	}

	if !isErrorType(secondType) {
		pass.Reportf(
			secondResult.Pos(),
			"entity constructor %s: second return must be error, got %s",
			funcName,
			secondType.String(),
		)
	}
}

func matchesASTTypeName(expr ast.Expr, expectedTypeName string) bool {
	starExpr, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}

	ident, ok := starExpr.X.(*ast.Ident)
	if !ok {
		return false
	}

	return strings.EqualFold(ident.Name, expectedTypeName)
}

func getTypeName(t types.Type) string {
	switch typ := t.(type) {
	case *types.Named:
		return typ.Obj().Name()
	case *types.Alias:
		return getTypeName(types.Unalias(typ))
	default:
		return ""
	}
}

func isErrorType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}

	return named.Obj().Pkg() == nil && named.Obj().Name() == "error"
}

// isVariantConstructor checks if the function name suggests it's a variant factory
// that creates a base type. E.g., NewMatchItemWithPolicy (expectedTypeName="MatchItemWithPolicy")
// returning *MatchItem (actualTypeName="MatchItem") is valid because the function name
// contains the base type name.
func isVariantConstructor(expectedTypeName, actualTypeName string) bool {
	return strings.Contains(expectedTypeName, actualTypeName)
}
