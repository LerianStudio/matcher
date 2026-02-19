//go:build e2e

package journeys

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	eclient "github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestContextLifecycle_CreatedInDraft verifies that newly created contexts have DRAFT status.
func TestContextLifecycle_CreatedInDraft(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create context without activation to verify initial status
		created := f.Context.NewContext().
			WithName("draft-check").
			WithoutActivation().
			MustCreate(ctx)

		require.NotEmpty(t, created.ID)
		require.Equal(t, "DRAFT", created.Status, "new context should start in DRAFT status")

		// Verify via GET as well
		fetched, err := apiClient.Configuration.GetContext(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, "DRAFT", fetched.Status)
	})
}

// TestContextLifecycle_ActivateFromDraft verifies the DRAFT → ACTIVE transition.
func TestContextLifecycle_ActivateFromDraft(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		created := f.Context.NewContext().
			WithName("activate-test").
			WithoutActivation().
			MustCreate(ctx)

		require.Equal(t, "DRAFT", created.Status)

		// Activate
		activeStatus := "ACTIVE"
		activated, err := apiClient.Configuration.UpdateContext(ctx, created.ID, eclient.UpdateContextRequest{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, "ACTIVE", activated.Status)

		// Verify persistence
		fetched, err := apiClient.Configuration.GetContext(ctx, activated.ID)
		require.NoError(t, err)
		require.Equal(t, "ACTIVE", fetched.Status)
	})
}

// TestContextLifecycle_PauseAndResume verifies ACTIVE ↔ PAUSED transitions.
func TestContextLifecycle_PauseAndResume(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Start with an ACTIVE context (factory default)
		created := f.Context.NewContext().
			WithName("pause-resume").
			MustCreate(ctx)

		require.Equal(t, "ACTIVE", created.Status)

		// Pause
		pausedStatus := "PAUSED"
		paused, err := apiClient.Configuration.UpdateContext(ctx, created.ID, eclient.UpdateContextRequest{
			Status: &pausedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, "PAUSED", paused.Status)

		// Resume (re-activate)
		activeStatus := "ACTIVE"
		resumed, err := apiClient.Configuration.UpdateContext(ctx, created.ID, eclient.UpdateContextRequest{
			Status: &activeStatus,
		})
		require.NoError(t, err)
		require.Equal(t, "ACTIVE", resumed.Status)
	})
}

// TestContextLifecycle_ArchiveFromActive verifies ACTIVE → ARCHIVED transition.
func TestContextLifecycle_ArchiveFromActive(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		created := f.Context.NewContext().
			WithName("archive-test").
			MustCreate(ctx)

		require.Equal(t, "ACTIVE", created.Status)

		archivedStatus := "ARCHIVED"
		archived, err := apiClient.Configuration.UpdateContext(ctx, created.ID, eclient.UpdateContextRequest{
			Status: &archivedStatus,
		})
		require.NoError(t, err)
		require.Equal(t, "ARCHIVED", archived.Status)

		// Verify archived context is still readable
		fetched, err := apiClient.Configuration.GetContext(ctx, archived.ID)
		require.NoError(t, err)
		require.Equal(t, "ARCHIVED", fetched.Status)
	})
}

// TestContextLifecycle_InvalidTransitions verifies that invalid state transitions are rejected.
func TestContextLifecycle_InvalidTransitions(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		t.Run("DRAFT cannot be archived directly", func(t *testing.T) {
			draft := f.Context.NewContext().
				WithName("draft-no-archive").
				WithoutActivation().
				MustCreate(ctx)

			archivedStatus := "ARCHIVED"
			_, err := apiClient.Configuration.UpdateContext(ctx, draft.ID, eclient.UpdateContextRequest{
				Status: &archivedStatus,
			})
			require.Error(t, err, "DRAFT → ARCHIVED should be rejected")
		})

		t.Run("DRAFT cannot be paused", func(t *testing.T) {
			draft := f.Context.NewContext().
				WithName("draft-no-pause").
				WithoutActivation().
				MustCreate(ctx)

			pausedStatus := "PAUSED"
			_, err := apiClient.Configuration.UpdateContext(ctx, draft.ID, eclient.UpdateContextRequest{
				Status: &pausedStatus,
			})
			require.Error(t, err, "DRAFT → PAUSED should be rejected")
		})

		t.Run("ACTIVE cannot go back to DRAFT", func(t *testing.T) {
			active := f.Context.NewContext().
				WithName("active-no-draft").
				MustCreate(ctx)

			draftStatus := "DRAFT"
			_, err := apiClient.Configuration.UpdateContext(ctx, active.ID, eclient.UpdateContextRequest{
				Status: &draftStatus,
			})
			require.Error(t, err, "ACTIVE → DRAFT should be rejected")
		})
	})
}

// TestContextLifecycle_DraftRejectsIngestion verifies that DRAFT contexts reject file uploads.
func TestContextLifecycle_DraftRejectsIngestion(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create DRAFT context with source + field map (config CRUD works on DRAFT)
		draft := f.Context.NewContext().
			WithName("draft-reject-ingest").
			WithoutActivation().
			MustCreate(ctx)

		source := f.Source.NewSource(draft.ID).AsLedger().MustCreate(ctx)
		f.Source.NewFieldMap(draft.ID, source.ID).WithStandardMapping().MustCreate(ctx)

		// Attempt to upload — should be rejected with 403
		csvData := "id,amount,currency,date\nTX1,100.00,USD,2025-01-15"
		_, err := apiClient.Ingestion.UploadCSV(ctx, draft.ID, source.ID, "test.csv", []byte(csvData))
		require.Error(t, err, "DRAFT context should reject ingestion")
		assert.True(t, strings.Contains(err.Error(), "403"),
			"expected 403 status for DRAFT context ingestion, got: %v", err)
	})
}

// TestContextLifecycle_DraftRejectsMatching verifies that DRAFT contexts reject match runs.
func TestContextLifecycle_DraftRejectsMatching(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		draft := f.Context.NewContext().
			WithName("draft-reject-match").
			WithoutActivation().
			MustCreate(ctx)

		// Attempt to run matching — should be rejected
		_, err := apiClient.Matching.RunMatch(ctx, draft.ID, "COMMIT", "")
		require.Error(t, err, "DRAFT context should reject matching")
		assert.True(t, strings.Contains(err.Error(), "403"),
			"expected 403 status for DRAFT context matching, got: %v", err)
	})
}

// TestContextLifecycle_PausedRejectsIngestion verifies that PAUSED contexts reject file uploads.
func TestContextLifecycle_PausedRejectsIngestion(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		// Create and activate, then pause
		active := f.Context.NewContext().
			WithName("paused-reject-ingest").
			MustCreate(ctx)

		source := f.Source.NewSource(active.ID).AsLedger().MustCreate(ctx)
		f.Source.NewFieldMap(active.ID, source.ID).WithStandardMapping().MustCreate(ctx)

		pausedStatus := "PAUSED"
		_, err := apiClient.Configuration.UpdateContext(ctx, active.ID, eclient.UpdateContextRequest{
			Status: &pausedStatus,
		})
		require.NoError(t, err)

		// Attempt to upload — should be rejected with 403
		csvData := "id,amount,currency,date\nTX1,100.00,USD,2025-01-15"
		_, err = apiClient.Ingestion.UploadCSV(ctx, active.ID, source.ID, "test.csv", []byte(csvData))
		require.Error(t, err, "PAUSED context should reject ingestion")
		assert.True(t, strings.Contains(err.Error(), "403"),
			"expected 403 status for PAUSED context ingestion, got: %v", err)
	})
}

// TestContextLifecycle_ConfigCRUDWorksOnDraft verifies that configuration operations
// (sources, rules, field maps) work regardless of context status.
func TestContextLifecycle_ConfigCRUDWorksOnDraft(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		draft := f.Context.NewContext().
			WithName("draft-config-crud").
			WithoutActivation().
			MustCreate(ctx)

		require.Equal(t, "DRAFT", draft.Status)

		// Source CRUD should work on DRAFT
		source := f.Source.NewSource(draft.ID).
			WithName("ledger").
			AsLedger().
			MustCreate(ctx)
		require.NotEmpty(t, source.ID)

		// Field map CRUD should work on DRAFT
		fieldMap := f.Source.NewFieldMap(draft.ID, source.ID).
			WithStandardMapping().
			MustCreate(ctx)
		require.NotEmpty(t, fieldMap.ID)

		// Rule CRUD should work on DRAFT
		rule := f.Rule.NewRule(draft.ID).
			Exact().
			WithExactConfig(true, true).
			MustCreate(ctx)
		require.NotEmpty(t, rule.ID)

		// List operations should work on DRAFT
		sources, err := apiClient.Configuration.ListSources(ctx, draft.ID)
		require.NoError(t, err)
		require.Len(t, sources, 1)

		rules, err := apiClient.Configuration.ListMatchRules(ctx, draft.ID)
		require.NoError(t, err)
		require.Len(t, rules, 1)
	})
}

// TestContextLifecycle_DeleteContextWithChildren verifies that deleting a context
// with children returns 409 and appropriate error message.
func TestContextLifecycle_DeleteContextWithChildren(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		created := f.Context.NewContext().
			WithName("delete-with-children").
			WithoutActivation().
			MustCreate(ctx)

		// Create a child source
		f.Source.NewSource(created.ID).AsLedger().MustCreate(ctx)

		// Attempt direct delete — should fail with 409
		err := apiClient.Configuration.DeleteContext(ctx, created.ID)
		require.Error(t, err, "deleting context with children should fail")
		assert.True(t, strings.Contains(err.Error(), "409"),
			"expected 409 for context with children, got: %v", err)
	})
}

// TestContextLifecycle_DeleteSourceWithFieldMap verifies that deleting a source
// with a field map returns 409.
func TestContextLifecycle_DeleteSourceWithFieldMap(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		created := f.Context.NewContext().
			WithName("delete-source-fieldmap").
			WithoutActivation().
			MustCreate(ctx)

		source := f.Source.NewSource(created.ID).AsLedger().MustCreate(ctx)
		f.Source.NewFieldMap(created.ID, source.ID).WithStandardMapping().MustCreate(ctx)

		// Attempt to delete source — should fail with 409
		err := apiClient.Configuration.DeleteSource(ctx, created.ID, source.ID)
		require.Error(t, err, "deleting source with field map should fail")
		assert.True(t, strings.Contains(err.Error(), "409"),
			"expected 409 for source with field map, got: %v", err)
	})
}

// TestContextLifecycle_FactoryDefaultIsActive verifies that the standard factory path
// produces ACTIVE contexts.
func TestContextLifecycle_FactoryDefaultIsActive(t *testing.T) {
	e2e.RunE2E(t, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		ctx := context.Background()
		f := factories.New(tc, apiClient)

		created := f.Context.NewContext().
			WithName("factory-active").
			MustCreate(ctx)

		require.Equal(t, "ACTIVE", created.Status, "factory should produce ACTIVE contexts by default")

		// Verify via API
		fetched, err := apiClient.Configuration.GetContext(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, "ACTIVE", fetched.Status)
	})
}
