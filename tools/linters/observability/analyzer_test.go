//go:build unit

package observability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, Analyzer)
	assert.Equal(t, "observability", Analyzer.Name)
	assert.NotEmpty(t, Analyzer.Doc)
	assert.NotNil(t, Analyzer.Run)
}

func TestIsServicePackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pkgPath  string
		expected bool
	}{
		{"command service", "github.com/example/internal/services/command", true},
		{"query service", "github.com/example/internal/services/query", true},
		{"nested command service", "github.com/example/internal/matching/services/command", true},
		{"adapters", "github.com/example/internal/adapters/http", false},
		{"domain", "github.com/example/internal/domain", false},
		{"services root", "github.com/example/internal/services", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isServicePackage(tt.pkgPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsServiceMethod exercises the path-based detection that replaced the
// historical Execute/Run/Handle allow-list. Domain verbs (RunMatch,
// ManualMatch, OpenDispute, AdjustEntry, ForceMatch, CreateAdjustment) must
// all be recognized; unexported helpers and methods on non-UseCase receivers
// must not be.
func TestIsServiceMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		src      string
		expected bool
	}{
		{
			name:     "exported RunMatch on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) RunMatch(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "exported ManualMatch on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) ManualMatch(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "exported OpenDispute on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) OpenDispute(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "exported AdjustEntry on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) AdjustEntry(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "exported ForceMatch on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) ForceMatch(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "exported CreateAdjustment on *UseCase with ctx",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) CreateAdjustment(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "value receiver UseCase is also accepted when ctx is first",
			src:      `package p; import "context"; type UseCase struct{}; func (uc UseCase) RunMatch(ctx context.Context) {}`,
			expected: true,
		},
		{
			name:     "unexported helper on *UseCase is skipped",
			src:      `package p; import "context"; type UseCase struct{}; func (uc *UseCase) prepareMatchRun(ctx context.Context) {}`,
			expected: false,
		},
		{
			name:     "exported method on different receiver is skipped",
			src:      `package p; import "context"; type Adapter struct{}; func (a *Adapter) RunMatch(ctx context.Context) {}`,
			expected: false,
		},
		{
			name:     "free function is skipped",
			src:      `package p; import "context"; func RunMatch(ctx context.Context) {}`,
			expected: false,
		},
		{
			name: "With*-style option setter without ctx is skipped",
			// Functional-option setters with no ctx, no I/O, no errors —
			// tracing them is ceremony theater, so the analyzer exempts
			// them by requiring context.Context as the first parameter.
			src:      `package p; type Cache struct{}; type UseCase struct{}; func (uc *UseCase) WithSchemaCache(c *Cache) {}`,
			expected: false,
		},
		{
			name: "trivial getter without ctx is skipped",
			// Same rationale — a getter returning internal state has no
			// tracing value and can't accept a ctx without breaking the
			// signature contract with its callers.
			src:      `package p; type UseCase struct{}; func (uc *UseCase) SupportsStreaming() bool { return false }`,
			expected: false,
		},
		{
			name: "exported method with wrong first param is skipped",
			// Defensive: a non-ctx first param (e.g., a domain ID) means
			// the method either wraps a traced inner method or is being
			// driven from a synchronous caller that already holds ctx.
			// Requiring ctx for tracing is the idiomatic gate.
			src:      `package p; type UseCase struct{}; func (uc *UseCase) SetWidget(id string) {}`,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn := firstFuncDecl(t, tt.src)
			assert.Equal(t, tt.expected, isServiceMethod(fn))
		})
	}
}

func TestObservabilityChecks(t *testing.T) {
	t.Parallel()

	checks := &observabilityChecks{}

	assert.False(t, checks.hasTrackingExtraction)
	assert.False(t, checks.hasSpanStart)
	assert.False(t, checks.hasDeferSpanEnd)

	checks.hasTrackingExtraction = true
	checks.hasSpanStart = true
	checks.hasDeferSpanEnd = true

	assert.True(t, checks.hasTrackingExtraction)
	assert.True(t, checks.hasSpanStart)
	assert.True(t, checks.hasDeferSpanEnd)
}

// TestAnalyzer_Fixture runs the analyzer against a testdata fixture that
// exercises every domain-verb method name the matcher codebase uses plus a
// deliberately broken method that omits tracer.Start. The fixture is placed
// under a path containing /services/command/ so isServicePackage matches.
func TestAnalyzer_Fixture(t *testing.T) {
	t.Parallel()

	testdata := testdataDir(t)
	analysistest.Run(t, testdata, Analyzer, "example.com/internal/matching/services/command")
}

// firstFuncDecl parses src and returns the first *ast.FuncDecl, or fails the
// test. Used by TestIsServiceMethod to exercise the receiver/export logic
// without spinning up the full go/types pipeline.
func firstFuncDecl(t *testing.T, src string) *ast.FuncDecl {
	t.Helper()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x.go", src, 0)
	require.NoError(t, err)

	for _, decl := range f.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			return fn
		}
	}

	t.Fatalf("no FuncDecl in src: %q", src)

	return nil
}

// testdataDir resolves the absolute path of the testdata directory alongside
// this file. analysistest.Run requires an absolute path; using runtime.Caller
// keeps the test portable across working directories (go test, bazel, etc).
func testdataDir(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")

	return filepath.Join(filepath.Dir(thisFile), "testdata")
}
