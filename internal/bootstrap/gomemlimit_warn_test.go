// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestWarnOnMissingGOMEMLIMIT_EmitsWhenContainerizedAndUnset(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}
	reader := func() (int64, string, error) { return 500 << 20, "cgroup v2", nil }

	warnOnMissingGOMEMLIMIT(context.Background(), logger, reader, "")

	assert.Len(t, logger.Messages, 1, "exactly one warning must be emitted")
	assert.Contains(t, logger.Messages[0], "GOMEMLIMIT is not set")
	assert.Contains(t, logger.Messages[0], "cgroup v2")
}

func TestWarnOnMissingGOMEMLIMIT_SilentWhenSet(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}
	reader := func() (int64, string, error) { return 500 << 20, "cgroup v2", nil }

	warnOnMissingGOMEMLIMIT(context.Background(), logger, reader, "450MiB")

	assert.Empty(t, logger.Messages, "must not log when GOMEMLIMIT is set")
}

func TestWarnOnMissingGOMEMLIMIT_SilentOnBareMetal(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}
	reader := func() (int64, string, error) { return 0, "", errors.New("no cgroup file") }

	warnOnMissingGOMEMLIMIT(context.Background(), logger, reader, "")

	assert.Empty(t, logger.Messages, "must not log on bare metal / macOS")
}

func TestWarnOnMissingGOMEMLIMIT_NilLogger(t *testing.T) {
	t.Parallel()

	reader := func() (int64, string, error) { return 500 << 20, "cgroup v2", nil }

	// Must not panic.
	warnOnMissingGOMEMLIMIT(context.Background(), nil, reader, "")
}

func TestWarnOnMissingGOMEMLIMIT_WarnsOnWhitespaceGOMEMLIMIT(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}
	reader := func() (int64, string, error) { return 500 << 20, "cgroup v2", nil }

	warnOnMissingGOMEMLIMIT(context.Background(), logger, reader, "   ")

	assert.Len(t, logger.Messages, 1, "whitespace-only GOMEMLIMIT is treated as unset and must still warn")
}

func TestWarnOnMissingGOMEMLIMIT_SilentWhenCgroupReportsNoLimit(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}
	reader := func() (int64, string, error) { return 0, "cgroup v2", nil }

	warnOnMissingGOMEMLIMIT(context.Background(), logger, reader, "")

	assert.Empty(t, logger.Messages, "must not warn when cgroup reports zero limit")
}

func TestWarnOnMissingGOMEMLIMIT_SilentOnNilReader(t *testing.T) {
	t.Parallel()

	logger := &testutil.TestLogger{}

	warnOnMissingGOMEMLIMIT(context.Background(), logger, nil, "")

	assert.Empty(t, logger.Messages, "must not warn when no reader available")
}
