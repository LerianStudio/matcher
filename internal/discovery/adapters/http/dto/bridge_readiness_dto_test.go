// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBridgeReadinessSummaryResponse_AllFieldsAndTotal(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	got := NewBridgeReadinessSummaryResponse(4, 3, 2, 1, 5, 90, now, nil, nil, false)

	assert.Equal(t, int64(4), got.ReadyCount)
	assert.Equal(t, int64(3), got.PendingCount)
	assert.Equal(t, int64(2), got.StaleCount)
	assert.Equal(t, int64(1), got.FailedCount)
	assert.Equal(t, int64(5), got.InFlightCount)
	assert.Equal(t, int64(15), got.TotalCount)
	assert.Equal(t, int64(90), got.StaleThresholdSec)
	assert.Equal(t, now, got.GeneratedAt)
}

func TestNewBridgeReadinessSummaryResponse_ZeroCounts(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	got := NewBridgeReadinessSummaryResponse(0, 0, 0, 0, 0, 60, now, nil, nil, false)

	assert.Equal(t, int64(0), got.TotalCount)
}

func TestNewBridgeCandidateResponse_LinkedRow(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	now := time.Now().UTC()
	extractionID := uuid.New()
	connectionID := uuid.New()

	got := NewBridgeCandidateResponse(
		extractionID,
		connectionID,
		"COMPLETE",
		"ready",
		&jobID,
		"fetcher-99",
		now.Add(-30*time.Second),
		now,
		30,
		"",
	)

	assert.Equal(t, extractionID, got.ExtractionID)
	assert.Equal(t, connectionID, got.ConnectionID)
	assert.Equal(t, "COMPLETE", got.Status)
	assert.Equal(t, "ready", got.ReadinessState)
	require.NotNil(t, got.IngestionJobID, "linked row must surface the job pointer")
	assert.Equal(t, jobID, *got.IngestionJobID)
	assert.Equal(t, "fetcher-99", got.FetcherJobID)
	assert.Equal(t, int64(30), got.AgeSeconds)
	assert.Empty(t, got.BridgeLastError, "ready row carries no failure class")
}

func TestNewBridgeCandidateResponse_UnlinkedRow(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	got := NewBridgeCandidateResponse(
		uuid.New(),
		uuid.New(),
		"COMPLETE",
		"pending",
		nil,
		"",
		now,
		now,
		5,
		"",
	)

	assert.Nil(t, got.IngestionJobID, "unlinked row must keep the job pointer nil")
	assert.Equal(t, "pending", got.ReadinessState)
	assert.Empty(t, got.BridgeLastError, "pending row carries no failure class")
}
