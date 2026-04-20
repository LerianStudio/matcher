//go:build unit

package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/stretchr/testify/require"
)

var chdirMu sync.Mutex

func TestMainInitLocalEnv(t *testing.T) {
	chdirMu.Lock()
	defer chdirMu.Unlock()
	t.Setenv("ENV_NAME", "local")

	tempDir := t.TempDir()
	envPath := filepath.Join(tempDir, ".env")
	require.NoError(t, os.WriteFile(envPath, []byte("TEST_DOTENV=loaded\n"), 0o600))

	oldValue, hadValue := os.LookupEnv("TEST_DOTENV")
	if hadValue {
		require.NoError(t, os.Unsetenv("TEST_DOTENV"))
	}

	t.Cleanup(func() {
		if hadValue {
			_ = os.Setenv("TEST_DOTENV", oldValue)
		} else {
			_ = os.Unsetenv("TEST_DOTENV")
		}
	})

	oldWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tempDir))
	t.Cleanup(func() {
		_ = os.Chdir(oldWd)
	})

	cfg := libCommons.InitLocalEnvConfig()
	require.NotNil(t, cfg)
	require.True(t, cfg.Initialized)
	require.Equal(t, "loaded", os.Getenv("TEST_DOTENV"))
}
