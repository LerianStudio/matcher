//go:build unit

package cross

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Static errors for testing (err113 compliance).
var (
	errDBConnectionFailed = errors.New("db connection failed")
	errJobLookupFailed    = errors.New("job lookup failed")
	errSourceLookupFailed = errors.New("source lookup failed")
)

type stubTransactionFinder struct {
	tx  *shared.Transaction
	err error
}

func (s *stubTransactionFinder) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.Transaction, error) {
	return s.tx, s.err
}

type stubJobFinder struct {
	job *ingestionEntities.IngestionJob
	err error
}

func (s *stubJobFinder) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*ingestionEntities.IngestionJob, error) {
	return s.job, s.err
}

type stubSourceContextFinder struct {
	contextID uuid.UUID
	err       error
}

func (s *stubSourceContextFinder) GetContextIDBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (uuid.UUID, error) {
	return s.contextID, s.err
}

func TestNewTransactionContextLookup(t *testing.T) {
	t.Parallel()

	t.Run("success with both dependencies", func(t *testing.T) {
		t.Parallel()

		txFinder := &stubTransactionFinder{}
		jobFinder := &stubJobFinder{}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)

		require.NoError(t, err)
		assert.NotNil(t, lookup)
	})

	t.Run("returns ErrTransactionFinderRequired when transactionFinder is nil", func(t *testing.T) {
		t.Parallel()

		jobFinder := &stubJobFinder{}

		lookup, err := NewTransactionContextLookup(nil, jobFinder)

		require.ErrorIs(t, err, ErrTransactionFinderRequired)
		assert.Nil(t, lookup)
	})

	t.Run("returns ErrIngestionJobFinderRequired when jobFinder is nil", func(t *testing.T) {
		t.Parallel()

		txFinder := &stubTransactionFinder{}

		lookup, err := NewTransactionContextLookup(txFinder, nil)

		require.ErrorIs(t, err, ErrIngestionJobFinderRequired)
		assert.Nil(t, lookup)
	})
}

func TestTransactionContextLookup_GetContextIDByTransactionID(t *testing.T) {
	t.Parallel()

	t.Run("success returns job.ContextID", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()
		expectedContextID := uuid.New()

		lookup := createLookupWithSuccess(t, transactionID, ingestionJobID, expectedContextID)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		require.NoError(t, err)
		assert.Equal(t, expectedContextID, contextID)
	})

	t.Run("transaction finder error wraps error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		txFinder := &stubTransactionFinder{err: errDBConnectionFailed}
		jobFinder := &stubJobFinder{}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, errDBConnectionFailed, "find transaction")
	})

	t.Run("transaction not found returns ErrTransactionNotFound", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()

		txFinder := &stubTransactionFinder{tx: nil, err: nil}
		jobFinder := &stubJobFinder{}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, ErrTransactionNotFound, "")
	})

	t.Run("job finder error wraps error", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
			},
		}
		jobFinder := &stubJobFinder{err: errJobLookupFailed}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, errJobLookupFailed, "find ingestion job")
	})

	t.Run("job not found returns ErrIngestionJobNotFound", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
			},
		}
		jobFinder := &stubJobFinder{job: nil, err: nil}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, ErrIngestionJobNotFound, "")
	})

	t.Run("nil lookup returns ErrContextLookupNotInitialized", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()

		var lookup *TransactionContextLookup

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		require.ErrorIs(t, err, ErrContextLookupNotInitialized)
		assert.Equal(t, uuid.Nil, contextID)
	})

	t.Run("source fallback succeeds when job lookup fails", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()
		sourceID := uuid.New()
		expectedContextID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
				SourceID:       sourceID,
			},
		}
		jobFinder := &stubJobFinder{err: errJobLookupFailed}
		sourceFinder := &stubSourceContextFinder{contextID: expectedContextID}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)
		lookup.WithSourceFinder(sourceFinder)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		require.NoError(t, err)
		assert.Equal(t, expectedContextID, contextID)
	})

	t.Run("source fallback succeeds when job is nil", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()
		sourceID := uuid.New()
		expectedContextID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
				SourceID:       sourceID,
			},
		}
		jobFinder := &stubJobFinder{job: nil, err: nil}
		sourceFinder := &stubSourceContextFinder{contextID: expectedContextID}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)
		lookup.WithSourceFinder(sourceFinder)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		require.NoError(t, err)
		assert.Equal(t, expectedContextID, contextID)
	})

	t.Run("returns job error when both source and job fail", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()
		sourceID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
				SourceID:       sourceID,
			},
		}
		jobFinder := &stubJobFinder{err: errJobLookupFailed}
		sourceFinder := &stubSourceContextFinder{err: errSourceLookupFailed}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)
		lookup.WithSourceFinder(sourceFinder)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		// Should return the original job error, not the source error
		verifyLookupError(t, err, contextID, errJobLookupFailed, "find ingestion job")
	})

	t.Run("skips source fallback when source finder is not set", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()
		sourceID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
				SourceID:       sourceID,
			},
		}
		jobFinder := &stubJobFinder{err: errJobLookupFailed}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)
		// No WithSourceFinder call

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, errJobLookupFailed, "find ingestion job")
	})

	t.Run("skips source fallback when source ID is nil", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		transactionID := uuid.New()
		ingestionJobID := uuid.New()

		txFinder := &stubTransactionFinder{
			tx: &shared.Transaction{
				ID:             transactionID,
				IngestionJobID: ingestionJobID,
				SourceID:       uuid.Nil, // no source ID
			},
		}
		jobFinder := &stubJobFinder{err: errJobLookupFailed}
		sourceFinder := &stubSourceContextFinder{contextID: uuid.New()}

		lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
		require.NoError(t, err)
		lookup.WithSourceFinder(sourceFinder)

		contextID, err := lookup.GetContextIDByTransactionID(ctx, transactionID)

		verifyLookupError(t, err, contextID, errJobLookupFailed, "find ingestion job")
	})

	t.Run("WithSourceFinder on nil lookup is no-op", func(t *testing.T) {
		t.Parallel()

		var lookup *TransactionContextLookup
		// Should not panic
		lookup.WithSourceFinder(&stubSourceContextFinder{})
	})
}

func createLookupWithSuccess(
	t *testing.T,
	transactionID, ingestionJobID, contextID uuid.UUID,
) *TransactionContextLookup {
	t.Helper()

	txFinder := &stubTransactionFinder{
		tx: &shared.Transaction{
			ID:             transactionID,
			IngestionJobID: ingestionJobID,
		},
	}
	jobFinder := &stubJobFinder{
		job: &ingestionEntities.IngestionJob{
			ID:        ingestionJobID,
			ContextID: contextID,
		},
	}

	lookup, err := NewTransactionContextLookup(txFinder, jobFinder)
	require.NoError(t, err)

	return lookup
}

func verifyLookupError(
	t *testing.T,
	err error,
	contextID uuid.UUID,
	expectedErr error,
	expectedContains string,
) {
	t.Helper()

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)

	if expectedContains != "" {
		assert.Contains(t, err.Error(), expectedContains)
	}

	assert.Equal(t, uuid.Nil, contextID)
}
