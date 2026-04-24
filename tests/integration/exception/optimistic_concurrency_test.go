//go:build integration

package exception

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	exceptionRepoAdapter "github.com/LerianStudio/matcher/internal/exception/adapters/postgres/exception"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	exceptionVO "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_Exception_OptimisticConcurrency_SuccessfulUpdate verifies that a single update
// increments the version from 0 to 1 and persists the field changes.
func TestIntegration_Exception_OptimisticConcurrency_SuccessfulUpdate(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		provider := h.Provider()
		excRepo := exceptionRepoAdapter.NewRepository(provider)

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)
		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID, "OCC-SUCCESS-"+uuid.New().String()[:8], decimal.NewFromFloat(100), "USD")
		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, sharedexception.ExceptionSeverityHigh, "test reason")

		// Load through the repository so we get a fully-hydrated entity with version.
		loaded, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)
		require.Equal(t, int64(0), loaded.Version, "initial version must be 0")

		// Mutate a field and update.
		loaded.Severity = sharedexception.ExceptionSeverityMedium
		updated, err := excRepo.Update(ctx, loaded)
		require.NoError(t, err)

		require.Equal(t, int64(1), updated.Version, "version must be incremented to 1 after update")
		require.Equal(t, sharedexception.ExceptionSeverityMedium, updated.Severity, "severity must be persisted")

		// Re-read to confirm persistence.
		reloaded, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)
		require.Equal(t, int64(1), reloaded.Version)
		require.Equal(t, sharedexception.ExceptionSeverityMedium, reloaded.Severity)
	})
}

// TestIntegration_Exception_OptimisticConcurrency_StaleVersionFails verifies that updating with a stale
// version (version mismatch) returns ErrConcurrentModification.
func TestIntegration_Exception_OptimisticConcurrency_StaleVersionFails(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		provider := h.Provider()
		excRepo := exceptionRepoAdapter.NewRepository(provider)

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)
		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID, "OCC-STALE-"+uuid.New().String()[:8], decimal.NewFromFloat(200), "USD")
		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, sharedexception.ExceptionSeverityHigh, "test reason")

		loaded, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)

		// Make a stale copy that retains version=0.
		staleCopy := *loaded

		// Advance the row to version=1 via a successful update.
		loaded.Severity = sharedexception.ExceptionSeverityMedium
		_, err = excRepo.Update(ctx, loaded)
		require.NoError(t, err)

		// Attempt to update using the stale copy (still version=0).
		staleCopy.Severity = sharedexception.ExceptionSeverityLow
		_, err = excRepo.Update(ctx, &staleCopy)
		require.Error(t, err)
		require.True(t,
			errors.Is(err, exceptionRepoAdapter.ErrConcurrentModification),
			"expected ErrConcurrentModification, got: %v", err,
		)
	})
}

// TestIntegration_Exception_OptimisticConcurrency_ConcurrentUpdateRace launches two goroutines that
// simultaneously try to update the same exception row with the same initial
// version. Exactly one must succeed, the other must fail with
// ErrConcurrentModification.
func TestIntegration_Exception_OptimisticConcurrency_ConcurrentUpdateRace(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		provider := h.Provider()
		excRepo := exceptionRepoAdapter.NewRepository(provider)

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)
		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID, "OCC-RACE-"+uuid.New().String()[:8], decimal.NewFromFloat(300), "USD")
		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, sharedexception.ExceptionSeverityHigh, "test reason")

		loaded, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)

		// Create two independent value copies holding the same version.
		copy1 := *loaded
		copy2 := *loaded

		copy1.Severity = sharedexception.ExceptionSeverityMedium
		copy2.Severity = sharedexception.ExceptionSeverityLow

		type result struct {
			err error
		}

		results := make(chan result, 2)

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()

			_, updateErr := excRepo.Update(ctx, &copy1)
			results <- result{err: updateErr}
		}()

		go func() {
			defer wg.Done()

			_, updateErr := excRepo.Update(ctx, &copy2)
			results <- result{err: updateErr}
		}()

		wg.Wait()
		close(results)

		var successes, failures int

		for r := range results {
			if r.err == nil {
				successes++
			} else {
				require.True(t,
					errors.Is(r.err, exceptionRepoAdapter.ErrConcurrentModification),
					"failing goroutine should return ErrConcurrentModification, got: %v", r.err,
				)

				failures++
			}
		}

		require.Equal(t, 1, successes, "exactly one goroutine must succeed")
		require.Equal(t, 1, failures, "exactly one goroutine must fail with ErrConcurrentModification")
	})
}

// TestIntegration_Exception_OptimisticConcurrency_SequentialUpdatesSucceed verifies that three sequential
// updates (each using the returned entity with the fresh version) all succeed.
// The final version must be 3.
func TestIntegration_Exception_OptimisticConcurrency_SequentialUpdatesSucceed(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		provider := h.Provider()
		excRepo := exceptionRepoAdapter.NewRepository(provider)

		jRepo := ingestionJobRepo.NewRepository(provider)
		txRepo := ingestionTxRepo.NewRepository(provider)
		job := createIngestionJob(t, ctx, jRepo, h.Seed.ContextID, h.Seed.SourceID, 1)
		tx := createTransaction(t, ctx, txRepo, job.ID, h.Seed.SourceID, "OCC-SEQ-"+uuid.New().String()[:8], decimal.NewFromFloat(400), "USD")
		exc := createExceptionForTransaction(t, ctx, h.Connection, tx.ID, sharedexception.ExceptionSeverityHigh, "test reason")

		current, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)
		require.Equal(t, int64(0), current.Version)

		severities := []sharedexception.ExceptionSeverity{
			sharedexception.ExceptionSeverityMedium,
			sharedexception.ExceptionSeverityLow,
			sharedexception.ExceptionSeverityCritical,
		}

		for i, sev := range severities {
			current.Severity = sev

			current, err = excRepo.Update(ctx, current)
			require.NoError(t, err, "sequential update %d must succeed", i+1)
			require.Equal(t, int64(i+1), current.Version, "version after update %d", i+1)
		}

		require.Equal(t, int64(3), current.Version, "final version after 3 sequential updates")

		// Final persistence check.
		final, err := excRepo.FindByID(ctx, exc.ID)
		require.NoError(t, err)
		require.Equal(t, int64(3), final.Version)
		require.Equal(t, sharedexception.ExceptionSeverityCritical, final.Severity)
	})
}

// TestIntegration_Exception_OptimisticConcurrency_UpdateNonExistentException verifies that updating an
// exception that does not exist returns ErrExceptionNotFound, not
// ErrConcurrentModification.
func TestIntegration_Exception_OptimisticConcurrency_UpdateNonExistentException(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)

		provider := h.Provider()
		excRepo := exceptionRepoAdapter.NewRepository(provider)

		// Fabricate a plausible entity that does not exist in the database.
		phantom := &entities.Exception{
			ID:            uuid.New(),
			TransactionID: uuid.New(),
			Severity:      sharedexception.ExceptionSeverityLow,
			Status:        exceptionVO.ExceptionStatusOpen,
			Version:       0,
		}

		_, err := excRepo.Update(ctx, phantom)
		require.Error(t, err)
		require.True(t,
			errors.Is(err, entities.ErrExceptionNotFound),
			"updating a non-existent exception must return ErrExceptionNotFound, got: %v", err,
		)
		require.False(t,
			errors.Is(err, exceptionRepoAdapter.ErrConcurrentModification),
			"must NOT return ErrConcurrentModification for a missing row",
		)
	})
}
