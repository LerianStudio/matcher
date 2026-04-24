// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"

	matchererrors "github.com/LerianStudio/matcher/pkg"
)

func TestAsProductError_ReturnsTypedMatcherError(t *testing.T) {
	t.Parallel()

	apiError, ok := asProductError(NewError(defNotFound, "missing", nil))
	require.True(t, ok)
	require.Equal(t, defNotFound.Code, apiError.ProductCode())

	_, ok = asProductError(errors.New("boom"))
	require.False(t, ok)
}

func TestAsProductError_IgnoresTypedNilMatcherError(t *testing.T) {
	t.Parallel()

	var typedNil *matchererrors.BaseError

	apiError, ok := asProductError(typedNil)
	require.False(t, ok)
	require.Nil(t, apiError)
}

func TestRespondError_UsesLegacySlugMapping(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/", func(c *fiber.Ctx) error {
		return RespondError(c, http.StatusConflict, "duplicate_name", "already exists")
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, defConfigurationDuplicateName.Code, body.Code)
	require.Equal(t, "already exists", body.Message)
}

func TestRespondError_UnknownSlugFallsBackToStatusDefinition(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	unknownSlug := "unknown_slug"
	app.Get("/", func(c *fiber.Ctx) error {
		return RespondError(c, http.StatusGone, unknownSlug, "gone")
	})

	resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/", http.NoBody))
	require.NoError(t, err)
	defer resp.Body.Close()

	var body ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, http.StatusGone, resp.StatusCode)
	require.Equal(t, "gone", body.Message)
	require.Equal(t, http.StatusText(http.StatusGone), body.Title)
}

func TestLegacySlugRegistry_CoversRespondErrorLiterals(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	fileSet := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, dirEntry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if dirEntry.IsDir() {
			switch dirEntry.Name() {
			case ".git", "vendor", "docs", "worktrees":
				return filepath.SkipDir
			}
			return nil
		}

		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_generated.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fileSet, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		ast.Inspect(file, func(node ast.Node) bool {
			callExpr, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}

			argumentIndex, ok := legacySlugArgumentIndex(callExpr.Fun)
			if !ok || len(callExpr.Args) <= argumentIndex {
				return true
			}

			literal, ok := callExpr.Args[argumentIndex].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}

			slug, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr != nil {
				t.Fatalf("unquote slug literal in %s: %v", path, unquoteErr)
			}

			if _, exists := legacyDefinitionsBySlug[slug]; !exists {
				t.Errorf("legacy slug %q used in %s is missing from registry", slug, path)
			}

			return true
		})

		return nil
	})
	require.NoError(t, err)
}

func TestNewError_EmptyMessageFallsBackToTitle(t *testing.T) {
	t.Parallel()

	apiError := NewError(defNotFound, "", nil)
	require.Equal(t, defNotFound.Title, apiError.ProductMessage())
}

func TestIsNilError_AllVariants(t *testing.T) {
	t.Parallel()

	var typedNil *matchererrors.BaseError

	require.True(t, isNilError(nil))
	require.True(t, isNilError(typedNil))
	require.False(t, isNilError(errors.New("boom")))
}

func repoRoot(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	require.NoError(t, err)

	return filepath.Clean(filepath.Join(workingDir, "..", "..", "..", ".."))
}

func legacySlugArgumentIndex(expression ast.Expr) (int, bool) {
	switch typedExpr := expression.(type) {
	case *ast.Ident:
		if typedExpr.Name == "respondError" || typedExpr.Name == "RespondError" {
			return 2, true
		}
		if typedExpr.Name == "WithContextNotFound" || typedExpr.Name == "WithHiddenContextOwnershipAsNotFound" {
			return 0, true
		}
	case *ast.SelectorExpr:
		if typedExpr.Sel == nil {
			return 0, false
		}

		switch typedExpr.Sel.Name {
		case "RespondError":
			return 2, true
		case "WithContextNotFound", "WithHiddenContextOwnershipAsNotFound":
			return 0, true
		}
	}

	return 0, false
}
