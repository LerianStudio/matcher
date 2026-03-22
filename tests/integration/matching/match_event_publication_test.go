//go:build integration

package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	matchingCommand "github.com/LerianStudio/matcher/internal/matching/services/command"
	outboxEntities "github.com/LerianStudio/matcher/internal/outbox/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

func TestIntegrationE4T9_MatchConfirmedOutboxAndDispatch(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedE4T9Config(t, h)
		registerFailureDiagnostics(t, h, seed.ContextID)

		wired := wireE4T9UseCases(t, h)

		ledgerCSV := buildCSV("REF-EVT", "9.99", "USD", "2026-01-03", "ledger")
		_, err := wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)

		bankCSV := buildCSV("ref-evt", "9.99", "USD", "2026-01-03", "bank")
		_, err = wired.IngestionUC.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)

		_, groups, err := wired.MatchingUC.RunMatch(ctx, matchingCommand.RunMatchInput{
			TenantID:  h.Seed.TenantID,
			ContextID: seed.ContextID,
			Mode:      matchingVO.MatchRunModeCommit,
		})
		require.NoError(t, err)
		require.Len(t, groups, 1)

		require.Equal(t, 1, assertMatchConfirmedPending(t, ctx, h))

		cap := &capturePublishers{}
		dispatcher := newDispatcher(t, h, cap)

		dispatcher.DispatchOnce(ctx)

		require.Equal(t, 1, cap.matchConfirmed)
		require.NotNil(t, cap.last)
		require.Equal(t, shared.EventTypeMatchConfirmed, cap.last.EventType)
		require.Equal(t, h.Seed.TenantID, cap.last.TenantID)
		require.Equal(t, seed.ContextID, cap.last.ContextID)
		require.Equal(t, groups[0].ID, cap.last.MatchID)

		require.Equal(t, 0, countInt(t, ctx, h.Connection,
			"SELECT count(*) FROM outbox_events WHERE event_type=$1 AND status=$2",
			shared.EventTypeMatchConfirmed,
			outboxEntities.OutboxStatusPending,
		))
	})
}
