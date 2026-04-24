// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewIngestionCompletedEventUsesCompletedAt(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	job, err := NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 10)
	require.NoError(t, err)

	completed := time.Now().UTC().Add(-time.Minute)
	job.CompletedAt = &completed

	event, err := NewIngestionCompletedEvent(
		ctx,
		job,
		5,
		time.Now().UTC().Add(-time.Hour),
		time.Now().UTC(),
		5,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, EventTypeIngestionCompleted, event.EventType)
	require.Equal(t, completed, event.CompletedAt)
}

func TestNewIngestionCompletedEvent_NilJob(t *testing.T) {
	t.Parallel()

	event, err := NewIngestionCompletedEvent(
		context.Background(),
		nil,
		5,
		time.Now().UTC(),
		time.Now().UTC(),
		5,
		0,
	)
	require.ErrorIs(t, err, ErrNilJob)
	require.Nil(t, event)
}

func TestNewIngestionFailedEvent(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	job, err := NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 10)
	require.NoError(t, err)

	job.Metadata.Error = "boom"

	event, err := NewIngestionFailedEvent(ctx, job)
	require.NoError(t, err)
	require.Equal(t, EventTypeIngestionFailed, event.EventType)
	require.Equal(t, job.Metadata.Error, event.Error)
}

func TestNewIngestionFailedEvent_NilJob(t *testing.T) {
	t.Parallel()

	event, err := NewIngestionFailedEvent(context.Background(), nil)
	require.ErrorIs(t, err, ErrNilJob)
	require.Nil(t, event)
}
