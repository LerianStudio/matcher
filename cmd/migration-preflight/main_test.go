// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	t.Run("uses environment dsn for up", func(t *testing.T) {
		originalUp := preflightMigrationUp
		defer func() { preflightMigrationUp = originalUp }()

		called := false
		preflightMigrationUp = func(_ context.Context, dsn string) error {
			called = true
			require.Equal(t, "postgres://env", dsn)
			return nil
		}

		exitCode := run([]string{"--action", "up"}, func(key string) string {
			if key == "DATABASE_URL" {
				return "postgres://env"
			}
			return ""
		}, io.Discard)

		require.Equal(t, 0, exitCode)
		require.True(t, called)
	})

	t.Run("parses zero padded goto target as decimal", func(t *testing.T) {
		originalGoto := preflightMigrationGoto
		defer func() { preflightMigrationGoto = originalGoto }()

		called := false
		preflightMigrationGoto = func(_ context.Context, dsn string, target int) error {
			called = true
			require.Equal(t, "postgres://flag", dsn)
			require.Equal(t, 23, target)
			return nil
		}

		exitCode := run([]string{"--dsn", "postgres://flag", "--action", "goto", "--target", "000023"}, func(string) string {
			return ""
		}, io.Discard)

		require.Equal(t, 0, exitCode)
		require.True(t, called)
	})

	t.Run("requires target for goto", func(t *testing.T) {
		output := &strings.Builder{}
		exitCode := run([]string{"--dsn", "postgres://flag", "--action", "goto"}, func(string) string {
			return ""
		}, output)

		require.Equal(t, 1, exitCode)
		require.Contains(t, output.String(), "target is required for goto")
	})

	t.Run("rejects unsupported action", func(t *testing.T) {
		output := &strings.Builder{}
		exitCode := run([]string{"--dsn", "postgres://flag", "--action", "dance"}, func(string) string {
			return ""
		}, output)

		require.Equal(t, 1, exitCode)
		require.Contains(t, output.String(), `unsupported action "dance"`)
	})

	t.Run("prints propagated preflight errors", func(t *testing.T) {
		originalDown := preflightMigrationDownOne
		defer func() { preflightMigrationDownOne = originalDown }()

		preflightMigrationDownOne = func(_ context.Context, _ string) error {
			return errors.New("blocked")
		}

		output := &strings.Builder{}
		exitCode := run([]string{"--dsn", "postgres://flag", "--action", "down"}, func(string) string {
			return ""
		}, output)

		require.Equal(t, 1, exitCode)
		require.Contains(t, output.String(), "blocked")
	})
}
