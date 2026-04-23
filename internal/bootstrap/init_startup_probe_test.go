// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunStartupSelfProbe_ContinuesOnError(t *testing.T) {
	t.Parallel()

	logger := &recordingInitLogger{}
	runnerCalled := false

	runStartupSelfProbe(context.Background(), &HealthDependencies{}, logger,
		func(context.Context, *HealthDependencies, libLog.Logger) error {
			runnerCalled = true
			return errors.New("boom")
		})

	assert.True(t, runnerCalled)

	logger.mu.Lock()
	defer logger.mu.Unlock()

	require.NotEmpty(t, logger.entries)
	assert.Equal(t, libLog.LevelError, logger.entries[len(logger.entries)-1].level)
	assert.True(t, strings.Contains(logger.entries[len(logger.entries)-1].msg, "startup self-probe failed"))
}

// TestInitServersWithOptions_TLSRequiredPlaintextFailsBeforeInfraConnect
// verifies that flipping POSTGRES_TLS_REQUIRED=true while Postgres is still
// configured plaintext (sslmode=disable) aborts bootstrap with the
// ErrTLSRequiredButNotDeclared contract BEFORE any infrastructure connection
// opens.
//
// The earlier incarnation of this test set DEPLOYMENT_MODE=saas to trigger
// the gate. The deployment-mode gate has been removed in favour of explicit
// per-stack X_TLS_REQUIRED flags.
func TestInitServersWithOptions_TLSRequiredPlaintextFailsBeforeInfraConnect(t *testing.T) {
	// No t.Parallel(): mutates process env via t.Setenv.

	t.Setenv("ENV_NAME", "development")
	t.Setenv("POSTGRES_TLS_REQUIRED", "true")
	t.Setenv("POSTGRES_SSLMODE", "disable")

	service, err := InitServersWithOptions(nil)
	require.Error(t, err)
	assert.Nil(t, service)
	assert.ErrorIs(t, err, ErrTLSRequiredButNotDeclared)
	assert.Contains(t, strings.ToLower(err.Error()), "postgres")
}
