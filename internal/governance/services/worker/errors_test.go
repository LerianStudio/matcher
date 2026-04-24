// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package worker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/governance/services/command"
)

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := []error{
		ErrWorkerAlreadyRunning,
		ErrWorkerNotRunning,
		ErrNilArchiveRepo,
		ErrNilPartitionManager,
		ErrNilStorageClient,
		command.ErrNilDB,
		ErrNilRedisClient,
		ErrArchivalInProgress,
		ErrChecksumMismatch,
		ErrRowCountMismatch,
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j {
				assert.False(t, errors.Is(a, b), "sentinel errors %d and %d should be distinct", i, j)
			}
		}
	}
}

func TestSentinelErrors_NotNil(t *testing.T) {
	t.Parallel()

	sentinels := map[string]error{
		"ErrWorkerAlreadyRunning": ErrWorkerAlreadyRunning,
		"ErrWorkerNotRunning":     ErrWorkerNotRunning,
		"ErrNilArchiveRepo":       ErrNilArchiveRepo,
		"ErrNilPartitionManager":  ErrNilPartitionManager,
		"ErrNilStorageClient":     ErrNilStorageClient,
		"ErrNilDB":                command.ErrNilDB,
		"ErrNilRedisClient":       ErrNilRedisClient,
		"ErrArchivalInProgress":   ErrArchivalInProgress,
		"ErrChecksumMismatch":     ErrChecksumMismatch,
		"ErrRowCountMismatch":     ErrRowCountMismatch,
	}

	for name, err := range sentinels {
		assert.NotNil(t, err, "%s should not be nil", name)
		assert.NotEmpty(t, err.Error(), "%s should have a message", name)
	}
}
