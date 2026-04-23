//go:build unit

package goroutineleak

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyzer_Metadata(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, Analyzer)
	assert.Equal(t, "goroutineleak", Analyzer.Name)
	assert.NotEmpty(t, Analyzer.Doc)
	assert.NotNil(t, Analyzer.Run)
}

func TestSkipPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		skipped bool
	}{
		{"shared testutil", "github.com/LerianStudio/matcher/internal/shared/testutil", true},
		{"matching testutil", "github.com/LerianStudio/matcher/internal/matching/testutil", true},
		{"tools entityconstructor", "github.com/LerianStudio/matcher/tools/linters/entityconstructor", true},
		{"mocks dir", "github.com/LerianStudio/matcher/internal/some/mocks", true},
		{"regular package", "github.com/LerianStudio/matcher/internal/bootstrap", false},
		{"adapter package", "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.skipped, skipPackage(tt.path))
		})
	}
}

func TestIsSafeGoCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{
			name: "SafeGoWithContext matches",
			src:  "package p\nfunc f() { runtime.SafeGoWithContext() }",
			want: true,
		},
		{
			name: "SafeGoWithContextAndComponent matches",
			src:  "package p\nfunc f() { runtime.SafeGoWithContextAndComponent() }",
			want: true,
		},
		{
			name: "unrelated method does not match",
			src:  "package p\nfunc f() { obj.DoThing() }",
			want: false,
		},
		{
			name: "non-selector call does not match",
			src:  "package p\nfunc f() { bare() }",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			call := findFirstCall(t, tt.src)
			if call == nil {
				assert.False(t, tt.want, "expected no match when no call found")
				return
			}
			assert.Equal(t, tt.want, isSafeGoCall(call))
		})
	}
}

func TestIsGoleakVerifyTestMain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{
			name: "goleak.VerifyTestMain matches",
			src:  "package p\nfunc f() { goleak.VerifyTestMain(m) }",
			want: true,
		},
		{
			name: "other.VerifyTestMain does not match",
			src:  "package p\nfunc f() { other.VerifyTestMain(m) }",
			want: false,
		},
		{
			name: "goleak.Something else does not match",
			src:  "package p\nfunc f() { goleak.VerifyNone(t) }",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			call := findFirstCall(t, tt.src)
			require.NotNil(t, call, "test requires a call expression in src")
			assert.Equal(t, tt.want, isGoleakVerifyTestMain(call))
		})
	}
}

// findFirstCall parses src and returns the first *ast.CallExpr encountered.
func findFirstCall(t *testing.T, src string) *ast.CallExpr {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, 0)
	require.NoError(t, err)

	var found *ast.CallExpr

	ast.Inspect(file, func(n ast.Node) bool {
		if found != nil {
			return false
		}

		if call, ok := n.(*ast.CallExpr); ok {
			found = call
			return false
		}

		return true
	})

	return found
}
