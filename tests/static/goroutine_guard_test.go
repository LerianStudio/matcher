//go:build unit

// Package static contains static analysis tests that guard codebase invariants.
package static

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoUnwrappedGoroutines walks the entire Matcher codebase and fails if any
// bare `go` statement is found outside the allowlisted files. All goroutines
// must use runtime.SafeGo* (from lib-uncommons) or errgroup.Go (from lib-uncommons)
// to ensure panic recovery.
//
// The SafeGo and errgroup implementations now live in lib-uncommons (vendored),
// so the allowlist points to vendor paths. Application code in internal/ and cmd/
// must never use bare `go` statements.
func TestNoUnwrappedGoroutines(t *testing.T) {
	t.Parallel()

	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root: %v", err)
	}

	var violations []string

	// All goroutine wrappers (SafeGo, errgroup.Go) live in lib-uncommons
	// which is in vendor/ (skipped above). No application code in this
	// repo should contain bare `go` statements.
	allowlist := map[string]struct{}{}

	err = filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			switch d.Name() {
			case ".git",
				"worktrees",
				".worktrees",
				"vendor",
				"docs",
				"bin",
				"migrations",
				"config",
				".references",
				"testdata":
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		if _, ok := allowlist[path]; ok {
			return nil
		}

		fset := token.NewFileSet()

		file, parseErr := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if parseErr != nil {
			return fmt.Errorf("parse file %s: %w", path, parseErr)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				return false
			}

			if stmt, ok := n.(*ast.GoStmt); ok {
				pos := fset.Position(stmt.Go)
				violations = append(violations, pos.String())

				return false
			}

			return true
		})

		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}

	if len(violations) > 0 {
		t.Fatalf("found goroutines without runtime.SafeGo*: %v", violations)
	}
}

func findRepoRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}

	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Dir = cwd
	cmd.Env = filteredGitEnv(os.Environ())
	repoRootBytes, err := cmd.CombinedOutput()
	if err == nil {
		repoRoot := strings.TrimSpace(string(repoRootBytes))
		if repoRoot != "" {
			return repoRoot, nil
		}
	}

	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat go.mod in %s: %w", dir, err)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s", cwd)
		}

		dir = parent
	}
}

func filteredGitEnv(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, "GIT_DIR=") || strings.HasPrefix(entry, "GIT_WORK_TREE=") {
			continue
		}
		filtered = append(filtered, entry)
	}

	return filtered
}
