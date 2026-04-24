// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package determinism

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
	assert.Equal(t, "determinism", Analyzer.Name)
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
		{"tools directory", "github.com/LerianStudio/matcher/tools/linters/determinism", true},
		{"mocks dir", "github.com/LerianStudio/matcher/internal/some/mocks", true},
		{"testdata dir", "github.com/LerianStudio/matcher/internal/x/testdata", true},
		{"regular package", "github.com/LerianStudio/matcher/internal/matching/domain/entities", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.skipped, skipPackage(tt.path))
		})
	}
}

func TestIsNewEntityCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"NewMatchGroup matches", "NewMatchGroup", true},
		{"NewFeeSchedule matches", "NewFeeSchedule", true},
		{"NewA is accepted (4 chars, capital after New)", "NewA", true},
		{"Nex (under 4 chars) is rejected", "Nex", false},
		{"Newline is not an entity constructor (lowercase after New)", "Newline", false},
		{"news is not a constructor", "news", false},
		{"empty string", "", false},
		{"New alone is not enough", "New", false},
		{"bare CreateX is not flagged", "CreateUser", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isNewEntityCall(tt.in))
		})
	}
}

func TestIsTimeNow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{
			name: "time.Now matches",
			src:  "package p\nfunc f() { _ = time.Now() }",
			want: true,
		},
		{
			name: "time.Date does not match",
			src:  "package p\nfunc f() { _ = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }",
			want: false,
		},
		{
			name: "clock.Now (non-time receiver) does not match",
			src:  "package p\nfunc f() { _ = clock.Now() }",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			call := findFirstCall(t, tt.src)
			require.NotNil(t, call, "expected a call expression in src")
			assert.Equal(t, tt.want, isTimeNow(call))
		})
	}
}

func TestIsUUIDNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{
			name: "uuid.New matches",
			src:  "package p\nfunc f() { _ = uuid.New() }",
			want: true,
		},
		{
			name: "uuid.MustParse does not match",
			src:  "package p\nfunc f() { _ = uuid.MustParse(\"abc\") }",
			want: false,
		},
		{
			name: "other.New does not match",
			src:  "package p\nfunc f() { _ = other.New() }",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			call := findFirstCall(t, tt.src)
			require.NotNil(t, call, "expected a call expression in src")
			assert.Equal(t, tt.want, isUUIDNew(call))
		})
	}
}

func TestBodyConstructsEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want bool
	}{
		{
			name: "calls NewMatchGroup",
			src:  "package p\nfunc Test() { _ = NewMatchGroup(ctx) }",
			want: true,
		},
		{
			name: "calls pkg.NewFeeSchedule via selector",
			src:  "package p\nfunc Test() { _ = fee.NewFeeSchedule(ctx) }",
			want: true,
		},
		{
			name: "no New-prefixed call",
			src:  "package p\nfunc Test() { _ = doThing() }",
			want: false,
		},
		{
			name: "Newline-prefixed name is not an entity",
			src:  "package p\nfunc Test() { _ = Newline() }",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fn := findFirstFunc(t, tt.src)
			require.NotNil(t, fn.Body)
			assert.Equal(t, tt.want, bodyConstructsEntity(fn.Body))
		})
	}
}

func TestCollectDeadlineTimeNows(t *testing.T) {
	t.Parallel()

	t.Run("time.Now().Add is collected as deadline", func(t *testing.T) {
		t.Parallel()
		src := "package p\nfunc Test() { _ = time.Now().Add(30 * time.Second) }"
		fn := findFirstFunc(t, src)
		deadlines := collectDeadlineTimeNows(fn.Body)
		assert.Len(t, deadlines, 1, "Add chain should mark its inner time.Now() as deadline")
	})

	t.Run("bare time.Now is not collected", func(t *testing.T) {
		t.Parallel()
		src := "package p\nfunc Test() { _ = time.Now() }"
		fn := findFirstFunc(t, src)
		deadlines := collectDeadlineTimeNows(fn.Body)
		assert.Empty(t, deadlines)
	})
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

// findFirstFunc parses src and returns the first *ast.FuncDecl encountered.
func findFirstFunc(t *testing.T, src string) *ast.FuncDecl {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, 0)
	require.NoError(t, err)

	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			return fn
		}
	}

	return nil
}
