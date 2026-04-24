// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateFetcherResultPath_ValidAbsolutePath_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/results/job-1.json")

	assert.NoError(t, err)
}

func TestValidateFetcherResultPath_EmptyPath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathRequired)
}

func TestValidateFetcherResultPath_WhitespacePath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("   ")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathRequired)
}

func TestValidateFetcherResultPath_RelativePath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("data/results/output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathNotAbsolute)
}

func TestValidateFetcherResultPath_URLScheme_NonAbsolute_ReturnsNotAbsolute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{"s3 scheme", "s3://bucket/output.csv"},
		{"ftp scheme", "ftp://server/file.csv"},
		{"https scheme", "https://example.com/output.csv"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFetcherResultPath(tt.path)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFetcherResultPathNotAbsolute)
		})
	}
}

func TestValidateFetcherResultPath_AbsolutePathWithScheme_ReturnsInvalidFormat(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data://bucket/output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_QueryString_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/output.csv?version=2")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_Fragment_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/output.csv#section")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathInvalidFormat)
}

func TestValidateFetcherResultPath_TraversalSegment_ReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{"double dot in middle", "/data/../etc/passwd"},
		{"double dot at start", "/../etc/shadow"},
		{"double dot only", "/.."},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateFetcherResultPath(tt.path)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
		})
	}
}

func TestValidateFetcherResultPath_UncleanPath_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data//output.csv")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
}

func TestValidateFetcherResultPath_TrailingSlash_ReturnsError(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/results/")

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherResultPathTraversal)
}

func TestValidateFetcherResultPath_RootPath_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/")

	assert.NoError(t, err)
}

func TestValidateFetcherResultPath_DeepNestedPath_Success(t *testing.T) {
	t.Parallel()

	err := validateFetcherResultPath("/data/extractions/2026/01/15/job-abc123/output.json")

	assert.NoError(t, err)
}
