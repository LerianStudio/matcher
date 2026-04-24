// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package determinism provides a linter that flags non-deterministic
// time and UUID calls inside test files that also construct entities
// via New{Entity} constructors.
//
// Tests that build entities with `time.Now()` or `uuid.New()` are flaky
// whenever assertions touch the resulting timestamp or ID — the value
// changes on every run. Matcher already ships deterministic helpers
// (internal/testutil.WithFixedTime for time, fixed-seed uuid pools for IDs);
// the linter nudges new tests toward those helpers.
//
// Scope:
//   - Only *_test.go files are checked.
//   - Inside each test function body, if the body contains at least one
//     call shaped like `New{CapitalName}(...)` (heuristic for entity
//     constructors), every `time.Now()` or `uuid.New()` call in the
//     same function is flagged.
//   - `time.Now().Add(...)` is NOT flagged — that's the deadline/TTL
//     idiom, not entity construction.
//
// The linter is intentionally advisory — wired into `lint-custom` but
// NOT into `lint-custom-strict` yet, following the goroutineleak
// precedent. Existing violations are cleaned up before the linter
// graduates to strict mode.
package determinism

import (
	"go/ast"
	"go/token"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the determinism analyzer.
var Analyzer = &analysis.Analyzer{
	Name:     "determinism",
	Doc:      "flags time.Now() / uuid.New() in tests that construct entities with New{Entity} (suggests testutil.WithFixedTime / deterministic UUID helpers)",
	Run:      run,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
}

//nolint:nilnil // analysis.Analyzer.Run requires (any, error) return
func run(pass *analysis.Pass) (any, error) {
	if skipPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.FuncDecl)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)
		if fn.Body == nil {
			return
		}

		file := pass.Fset.File(fn.Pos())
		if file == nil || !strings.HasSuffix(file.Name(), "_test.go") {
			return
		}

		if !bodyConstructsEntity(fn.Body) {
			return
		}

		deadlineTimeNows := collectDeadlineTimeNows(fn.Body)

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			switch {
			case isTimeNow(call):
				if _, skip := deadlineTimeNows[call.Pos()]; skip {
					return true
				}

				pass.Reportf(
					call.Pos(),
					"test uses time.Now() while constructing an entity — prefer testutil.WithFixedTime or a fixed time.Date literal for deterministic assertions",
				)
			case isUUIDNew(call):
				pass.Reportf(
					call.Pos(),
					"test uses uuid.New() while constructing an entity — prefer a deterministic UUID helper (e.g. a fixed uuid.MustParse literal or per-test pool) for stable assertions",
				)
			}

			return true
		})
	})

	return nil, nil
}

// bodyConstructsEntity reports whether body contains at least one call whose
// function name starts with "New" followed by a capital letter — the matcher
// entity-constructor convention (NewMatchGroup, NewFeeSchedule, …). This is a
// heuristic, not a type check — the linter intentionally errs toward false
// negatives rather than chasing every selector expression.
func bodyConstructsEntity(body *ast.BlockStmt) bool {
	var found bool

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		name := callFuncName(call)
		if isNewEntityCall(name) {
			found = true
			return false
		}

		return true
	})

	return found
}

// collectDeadlineTimeNows walks body and returns the set of positions where
// `time.Now()` appears as the receiver of a `.Add(...)` call — the deadline
// idiom (`time.Now().Add(30 * time.Second)`). These positions are suppressed
// from the main scan because they do not participate in entity construction.
func collectDeadlineTimeNows(body *ast.BlockStmt) map[token.Pos]struct{} {
	out := make(map[token.Pos]struct{})

	ast.Inspect(body, func(n ast.Node) bool {
		outer, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		sel, ok := outer.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		if sel.Sel == nil || sel.Sel.Name != "Add" {
			return true
		}

		inner, ok := sel.X.(*ast.CallExpr)
		if !ok {
			return true
		}

		if isTimeNow(inner) {
			out[inner.Pos()] = struct{}{}
		}

		return true
	})

	return out
}

// callFuncName returns the final name of the function being called. For a
// plain identifier (New(...)) it returns the identifier; for a selector
// expression (pkg.New(...) or obj.NewXxx(...)) it returns the trailing name.
// Anything else returns "".
func callFuncName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		if fun.Sel != nil {
			return fun.Sel.Name
		}
	}

	return ""
}

// isNewEntityCall returns true when name matches "New" + capital letter + at
// least one additional character. "New" alone (stdlib readers etc.) is not
// enough to trigger; the entity convention requires the type name to follow.
func isNewEntityCall(name string) bool {
	if len(name) < 4 {
		return false
	}

	if !strings.HasPrefix(name, "New") {
		return false
	}

	// The character after "New" must be uppercase ASCII (A-Z). This excludes
	// identifiers like "Newline", "NewsItem" that happen to start with "New".
	c := name[3]

	return c >= 'A' && c <= 'Z'
}

// isTimeNow reports whether call is syntactically `time.Now()`.
func isTimeNow(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if sel.Sel == nil || sel.Sel.Name != "Now" {
		return false
	}

	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}

	return ident.Name == "time"
}

// isUUIDNew reports whether call is syntactically `uuid.New()`.
func isUUIDNew(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	if sel.Sel == nil || sel.Sel.Name != "New" {
		return false
	}

	ident, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}

	return ident.Name == "uuid"
}

// skipPackage returns true for packages that should not be checked.
// Testutil fixtures and the linter's own testdata directories are allowed
// to use non-deterministic helpers — testutil defines them; testdata
// intentionally contains violations for the linter's own tests.
func skipPackage(path string) bool {
	switch {
	case strings.Contains(path, "/testutil"):
		return true
	case strings.Contains(path, "/tools/"):
		return true
	case strings.HasSuffix(path, "/mocks"):
		return true
	case strings.Contains(path, "/testdata"):
		return true
	}

	return false
}
