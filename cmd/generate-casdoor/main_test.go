//go:build unit

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	matcherAuth "github.com/LerianStudio/matcher/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_WritesCasdoorInitData(t *testing.T) {
	t.Parallel()

	outputPath := filepath.Join(t.TempDir(), "casdoor", "init_data.json")

	err := run(outputPath)
	require.NoError(t, err)

	contents, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	var generated matcherAuth.CasdoorInitData
	require.NoError(t, json.Unmarshal(contents, &generated))

	expected, err := matcherAuth.BuildCasdoorInitData()
	require.NoError(t, err)
	assert.Equal(t, expected, generated)
}
