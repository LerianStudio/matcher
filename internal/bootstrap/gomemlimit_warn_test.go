//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestShouldWarnOnGOMEMLIMIT(t *testing.T) {
	t.Parallel()

	containerReader := func() (int64, string, error) {
		return 500 << 20, "cgroup v2", nil
	}
	bareMetalReader := func() (int64, string, error) {
		return 0, "", errors.New("no cgroup file")
	}
	noLimitReader := func() (int64, string, error) {
		return 0, "cgroup v2", nil
	}

	tests := []struct {
		name       string
		gomemlimit string
		reader     memoryLimitReader
		want       bool
	}{
		{
			name:       "containerized AND GOMEMLIMIT unset → warn",
			gomemlimit: "",
			reader:     containerReader,
			want:       true,
		},
		{
			name:       "whitespace GOMEMLIMIT treated as unset",
			gomemlimit: "   ",
			reader:     containerReader,
			want:       true,
		},
		{
			name:       "GOMEMLIMIT set → do not warn",
			gomemlimit: "450MiB",
			reader:     containerReader,
			want:       false,
		},
		{
			name:       "bare metal (reader error) → do not warn",
			gomemlimit: "",
			reader:     bareMetalReader,
			want:       false,
		},
		{
			name:       "cgroup reports no limit (max) → do not warn",
			gomemlimit: "",
			reader:     noLimitReader,
			want:       false,
		},
		{
			name:       "nil reader → do not warn",
			gomemlimit: "",
			reader:     nil,
			want:       false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldWarnOnGOMEMLIMIT(tc.gomemlimit, tc.reader)
			assert.Equal(t, tc.want, got)
		})
	}
}

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
