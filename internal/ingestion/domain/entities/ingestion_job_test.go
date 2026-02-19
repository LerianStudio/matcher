//go:build unit

package entities_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

func TestIngestionJobLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 128)
	require.NoError(t, err)
	require.Equal(t, value_objects.JobStatusQueued, job.Status)
	require.Equal(t, "file.csv", job.Metadata.FileName)

	require.NoError(t, job.Start(ctx))
	require.Equal(t, value_objects.JobStatusProcessing, job.Status)
	require.False(t, job.StartedAt.IsZero())

	require.NoError(t, job.Complete(ctx, 10, 2))
	require.Equal(t, value_objects.JobStatusCompleted, job.Status)
	require.Error(t, job.Start(ctx))
	require.NotNil(t, job.CompletedAt)
	require.Equal(t, 10, job.Metadata.TotalRows)
	require.Equal(t, 2, job.Metadata.FailedRows)

	queuedJob, err := entities.NewIngestionJob(ctx, contextID, sourceID, "queued.csv", 128)
	require.NoError(t, err)
	require.Error(t, queuedJob.Complete(ctx, 1, 0))

	invalidCountsJob, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 128)
	require.NoError(t, err)
	require.NoError(t, invalidCountsJob.Start(ctx))
	require.Error(t, invalidCountsJob.Complete(ctx, -1, -2))

	tooManyFailed, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 128)
	require.NoError(t, err)
	require.NoError(t, tooManyFailed.Start(ctx))
	require.Error(t, tooManyFailed.Complete(ctx, 1, 2))

	metadata, err := job.MetadataJSON()
	require.NoError(t, err)
	require.NotEmpty(t, metadata)
}

func TestIngestionJobFail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Happy path: PROCESSING -> FAILED
	job, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 256)
	require.NoError(t, err)
	require.NoError(t, job.Start(ctx))
	require.NoError(t, job.Fail(ctx, "parse failed"))
	require.Equal(t, value_objects.JobStatusFailed, job.Status)
	require.NotNil(t, job.CompletedAt)
	require.Equal(t, "parse failed", job.Metadata.Error)
	require.WithinDuration(t, time.Now().UTC(), *job.CompletedAt, time.Second)

	// Idempotent: FAILED -> FAILED is a no-op
	require.NoError(t, job.Fail(ctx, "parse failed again"))

	// QUEUED -> FAILED must be rejected
	queuedJob, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 256)
	require.NoError(t, err)
	require.Equal(t, value_objects.JobStatusQueued, queuedJob.Status)
	err = queuedJob.Fail(ctx, "too early")
	require.ErrorIs(t, err, entities.ErrJobMustBeProcessingToFail)

	// Empty error message must be rejected
	emptyFail, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 256)
	require.NoError(t, err)
	require.NoError(t, emptyFail.Start(ctx))
	require.Error(t, emptyFail.Fail(ctx, " "))

	// COMPLETED -> FAILED must be rejected
	completedJob, err := entities.NewIngestionJob(ctx, uuid.New(), uuid.New(), "file.csv", 256)
	require.NoError(t, err)
	require.NoError(t, completedJob.Start(ctx))
	require.NoError(t, completedJob.Complete(ctx, 1, 0))
	err = completedJob.Fail(ctx, "late fail")
	require.ErrorIs(t, err, entities.ErrJobMustBeProcessingToFail)
}

func TestIngestionJobNilReceiverGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilJob *entities.IngestionJob

	t.Run("Start returns error on nil receiver", func(t *testing.T) {
		t.Parallel()

		err := nilJob.Start(ctx)
		require.Error(t, err)
		require.Equal(t, entities.ErrJobNil, err)
	})

	t.Run("Complete returns error on nil receiver", func(t *testing.T) {
		t.Parallel()

		err := nilJob.Complete(ctx, 10, 2)
		require.Error(t, err)
		require.Equal(t, entities.ErrJobNil, err)
	})

	t.Run("Fail returns error on nil receiver", func(t *testing.T) {
		t.Parallel()

		err := nilJob.Fail(ctx, "some error")
		require.Error(t, err)
		require.Equal(t, entities.ErrJobNil, err)
	})

	t.Run("MetadataJSON handles nil receiver gracefully", func(t *testing.T) {
		t.Parallel()

		data, err := nilJob.MetadataJSON()
		require.NoError(t, err)
		require.Equal(t, []byte("null"), data)
	})
}

func TestNewIngestionJobValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	validContextID := uuid.New()
	validSourceID := uuid.New()

	t.Run("fails with nil context ID", func(t *testing.T) {
		t.Parallel()

		_, err := entities.NewIngestionJob(ctx, uuid.Nil, validSourceID, "file.csv", 128)
		require.Error(t, err)
		require.Equal(t, entities.ErrContextIDRequired, err)
	})

	t.Run("fails with nil source ID", func(t *testing.T) {
		t.Parallel()

		_, err := entities.NewIngestionJob(ctx, validContextID, uuid.Nil, "file.csv", 128)
		require.Error(t, err)
		require.Equal(t, entities.ErrSourceIDRequired, err)
	})

	t.Run("fails with empty file name", func(t *testing.T) {
		t.Parallel()

		_, err := entities.NewIngestionJob(ctx, validContextID, validSourceID, "", 128)
		require.Error(t, err)
		require.Equal(t, entities.ErrFileNameRequired, err)
	})

	t.Run("fails with whitespace file name", func(t *testing.T) {
		t.Parallel()

		_, err := entities.NewIngestionJob(ctx, validContextID, validSourceID, "   ", 128)
		require.Error(t, err)
		require.Equal(t, entities.ErrFileNameRequired, err)
	})

	t.Run("fails with negative file size", func(t *testing.T) {
		t.Parallel()

		_, err := entities.NewIngestionJob(ctx, validContextID, validSourceID, "file.csv", -1)
		require.Error(t, err)
		require.Equal(t, entities.ErrFileSizeInvalid, err)
	})

	t.Run("succeeds with zero file size", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewIngestionJob(ctx, validContextID, validSourceID, "file.csv", 0)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.Equal(t, int64(0), job.Metadata.FileSize)
	})

	t.Run("trims file name whitespace", func(t *testing.T) {
		t.Parallel()

		job, err := entities.NewIngestionJob(
			ctx,
			validContextID,
			validSourceID,
			"  file.csv  ",
			128,
		)
		require.NoError(t, err)
		require.Equal(t, "file.csv", job.Metadata.FileName)
	})
}

func TestRowError(t *testing.T) {
	t.Parallel()

	t.Run("RowError struct has expected fields", func(t *testing.T) {
		t.Parallel()

		rowErr := entities.RowError{
			Row:     15,
			Field:   "amount",
			Message: "invalid decimal",
		}

		require.Equal(t, 15, rowErr.Row)
		require.Equal(t, "amount", rowErr.Field)
		require.Equal(t, "invalid decimal", rowErr.Message)
	})
}

func TestJobMetadata_ErrorDetails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	sourceID := uuid.New()

	job, err := entities.NewIngestionJob(ctx, contextID, sourceID, "file.csv", 100)
	require.NoError(t, err)

	job.Metadata.ErrorDetails = []entities.RowError{
		{Row: 1, Field: "amount", Message: "invalid"},
		{Row: 5, Field: "date", Message: "unparseable"},
	}

	require.Len(t, job.Metadata.ErrorDetails, 2)
	require.Equal(t, 1, job.Metadata.ErrorDetails[0].Row)
	require.Equal(t, "amount", job.Metadata.ErrorDetails[0].Field)
}
