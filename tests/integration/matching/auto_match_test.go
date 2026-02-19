//go:build integration

package matching

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	configContextRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/context"
	configFieldMapRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/field_map"
	configMatchRuleRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/match_rule"
	configSourceRepo "github.com/LerianStudio/matcher/internal/configuration/adapters/postgres/source"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	ingestionParsers "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionRedis "github.com/LerianStudio/matcher/internal/ingestion/adapters/redis"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	outboxRepo "github.com/LerianStudio/matcher/internal/outbox/adapters/postgres"
	sharedCross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	tenantAdapters "github.com/LerianStudio/matcher/internal/shared/infrastructure/tenant/adapters"
	"github.com/LerianStudio/matcher/tests/integration"
)

// capturingMatchTrigger records whether TriggerMatchForContext was called
// and with which contextID. Thread-safe for use from async goroutines.
type capturingMatchTrigger struct {
	mu         sync.Mutex
	calls      []capturedTriggerCall
	callCount  int
	blockUntil chan struct{} // optional: blocks trigger until closed
}

type capturedTriggerCall struct {
	TenantID  uuid.UUID
	ContextID uuid.UUID
}

func newCapturingMatchTrigger() *capturingMatchTrigger {
	return &capturingMatchTrigger{}
}

func (c *capturingMatchTrigger) TriggerMatchForContext(_ context.Context, tenantID, contextID uuid.UUID) {
	if c.blockUntil != nil {
		<-c.blockUntil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.callCount++
	c.calls = append(c.calls, capturedTriggerCall{
		TenantID:  tenantID,
		ContextID: contextID,
	})
}

func (c *capturingMatchTrigger) wasCalled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.callCount > 0
}

func (c *capturingMatchTrigger) totalCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.callCount
}

func (c *capturingMatchTrigger) lastCall() (capturedTriggerCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.calls) == 0 {
		return capturedTriggerCall{}, false
	}

	return c.calls[len(c.calls)-1], true
}

func (c *capturingMatchTrigger) allCalls() []capturedTriggerCall {
	c.mu.Lock()
	defer c.mu.Unlock()

	result := make([]capturedTriggerCall, len(c.calls))
	copy(result, c.calls)

	return result
}

// stubContextProvider returns a fixed auto-match enabled value.
type stubContextProvider struct {
	enabled bool
	err     error
}

func (s *stubContextProvider) IsAutoMatchEnabled(_ context.Context, _ uuid.UUID) (bool, error) {
	return s.enabled, s.err
}

// autoMatchSeed holds the IDs for a context configured with auto-match support.
type autoMatchSeed struct {
	ContextID         uuid.UUID
	LedgerSourceID    uuid.UUID
	NonLedgerSourceID uuid.UUID
}

// seedAutoMatchConfig creates a reconciliation context with the given AutoMatchOnUpload
// setting, two sources (ledger + bank), field maps, and a match rule. Returns IDs for use
// in ingestion calls.
func seedAutoMatchConfig(
	t *testing.T,
	h *integration.TestHarness,
	autoMatchEnabled bool,
) autoMatchSeed {
	t.Helper()

	ctx := e4t9Ctx(t, h)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(h.Connection, nil)

	ctxRepo := configContextRepo.NewRepository(provider)
	srcRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)

	fmRepo := configFieldMapRepo.NewRepository(provider)
	ruleRepo := configMatchRuleRepo.NewRepository(provider)

	// Create context with the specified auto-match setting.
	contextEntity, err := configEntities.NewReconciliationContext(
		ctx,
		h.Seed.TenantID,
		configEntities.CreateReconciliationContextInput{
			Name:              "Auto Match Test " + uuid.New().String()[:8],
			Type:              configVO.ContextTypeOneToOne,
			Interval:          "0 0 * * *",
			AutoMatchOnUpload: &autoMatchEnabled,
		},
	)
	require.NoError(t, err)
	require.NoError(t, contextEntity.Activate(ctx))

	createdCtx, err := ctxRepo.Create(ctx, contextEntity)
	require.NoError(t, err)

	// Create ledger source.
	ledgerSrc, err := configEntities.NewReconciliationSource(
		ctx,
		createdCtx.ID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "AutoMatch Ledger",
			Type:   configVO.SourceTypeLedger,
			Config: map[string]any{"format": "csv"},
		},
	)
	require.NoError(t, err)

	createdLedger, err := srcRepo.Create(ctx, ledgerSrc)
	require.NoError(t, err)

	// Create bank source.
	bankSrc, err := configEntities.NewReconciliationSource(
		ctx,
		createdCtx.ID,
		configEntities.CreateReconciliationSourceInput{
			Name:   "AutoMatch Bank",
			Type:   configVO.SourceTypeBank,
			Config: map[string]any{"format": "csv"},
		},
	)
	require.NoError(t, err)

	createdBank, err := srcRepo.Create(ctx, bankSrc)
	require.NoError(t, err)

	// Create field maps for both sources.
	mapping := map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
		"description": "description",
	}

	ledgerFM, err := configEntities.NewFieldMap(
		ctx,
		createdCtx.ID,
		createdLedger.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, ledgerFM)
	require.NoError(t, err)

	bankFM, err := configEntities.NewFieldMap(
		ctx,
		createdCtx.ID,
		createdBank.ID,
		configEntities.CreateFieldMapInput{Mapping: mapping},
	)
	require.NoError(t, err)
	_, err = fmRepo.Create(ctx, bankFM)
	require.NoError(t, err)

	// Create match rule.
	rule, err := configEntities.NewMatchRule(
		ctx,
		createdCtx.ID,
		configEntities.CreateMatchRuleInput{
			Priority: 1,
			Type:     configVO.RuleTypeExact,
			Config: map[string]any{
				"matchAmount":     true,
				"matchCurrency":   true,
				"matchDate":       true,
				"datePrecision":   "DAY",
				"matchReference":  true,
				"caseInsensitive": true,
				"matchScore":      100,
			},
		},
	)
	require.NoError(t, err)
	_, err = ruleRepo.Create(ctx, rule)
	require.NoError(t, err)

	return autoMatchSeed{
		ContextID:         createdCtx.ID,
		LedgerSourceID:    createdLedger.ID,
		NonLedgerSourceID: createdBank.ID,
	}
}

// wireIngestionWithTrigger creates an ingestion UseCase wired with a capturing
// MatchTrigger and a ContextProvider. The ContextProvider can be either a stub
// (for unit-like control) or a real DB-backed adapter (for full integration).
func wireIngestionWithTrigger(
	t *testing.T,
	h *integration.TestHarness,
	trigger *capturingMatchTrigger,
	ctxProvider *stubContextProvider,
) *ingestionCommand.UseCase {
	t.Helper()

	redisConn := mustRedisConn(t, h.RedisAddr)
	provider := tenantAdapters.NewSingleTenantInfrastructureProvider(h.Connection, redisConn)

	jobRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTxRepo.NewRepository(provider)
	dedupe := ingestionRedis.NewDedupeService(provider)
	outbox := outboxRepo.NewRepository(provider)

	parserRegistry := ingestionParsers.NewParserRegistry()
	parserRegistry.Register(ingestionParsers.NewCSVParser())

	cfgSourceRepo, err := configSourceRepo.NewRepository(provider)
	require.NoError(t, err)

	cfgFieldMapRepo := configFieldMapRepo.NewRepository(provider)
	fieldMapAdapter, err := sharedCross.NewFieldMapRepositoryAdapter(cfgFieldMapRepo)
	require.NoError(t, err)

	sourceAdapter, err := sharedCross.NewSourceRepositoryAdapter(cfgSourceRepo)
	require.NoError(t, err)

	uc, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         jobRepo,
		TransactionRepo: txRepo,
		Dedupe:          dedupe,
		Publisher:       &noopIngestionPublisher{},
		OutboxRepo:      outbox,
		Parsers:         parserRegistry,
		FieldMapRepo:    fieldMapAdapter,
		SourceRepo:      sourceAdapter,
		MatchTrigger:    trigger,
		ContextProvider: ctxProvider,
	})
	require.NoError(t, err)

	return uc
}

// TestAutoMatch_TriggeredOnUpload verifies that when AutoMatchOnUpload is enabled,
// a successful ingestion fires the MatchTrigger with the correct contextID.
func TestAutoMatch_TriggeredOnUpload(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedAutoMatchConfig(t, h, true)
		registerFailureDiagnostics(t, h, seed.ContextID)

		trigger := newCapturingMatchTrigger()
		ctxProvider := &stubContextProvider{enabled: true}

		uc := wireIngestionWithTrigger(t, h, trigger, ctxProvider)

		csv := buildCSV("AUTO-001", "100.00", "USD", "2026-01-15", "auto match test")
		_, err := uc.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"auto_match.csv",
			int64(len(csv)),
			"csv",
			strings.NewReader(csv),
		)
		require.NoError(t, err)

		// triggerAutoMatchIfEnabled calls TriggerMatchForContext synchronously
		// (the MatchTriggerAdapter spawns a goroutine, but our capturing impl
		// records immediately).
		require.True(t, trigger.wasCalled(), "MatchTrigger should have been called after successful ingestion")

		call, ok := trigger.lastCall()
		require.True(t, ok)
		require.Equal(t, seed.ContextID, call.ContextID, "trigger should receive the correct contextID")
		require.Equal(t, h.Seed.TenantID, call.TenantID, "trigger should receive the correct tenantID")
	})
}

// TestAutoMatch_NotTriggeredWhenDisabled verifies that when AutoMatchOnUpload
// is disabled (via ContextProvider returning false), ingestion does NOT trigger matching.
func TestAutoMatch_NotTriggeredWhenDisabled(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		// Create context with auto-match OFF.
		seed := seedAutoMatchConfig(t, h, false)
		registerFailureDiagnostics(t, h, seed.ContextID)

		trigger := newCapturingMatchTrigger()
		ctxProvider := &stubContextProvider{enabled: false}

		uc := wireIngestionWithTrigger(t, h, trigger, ctxProvider)

		csv := buildCSV("NOAUTO-001", "200.00", "USD", "2026-02-01", "no auto match")
		_, err := uc.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"no_auto_match.csv",
			int64(len(csv)),
			"csv",
			strings.NewReader(csv),
		)
		require.NoError(t, err)

		require.False(t, trigger.wasCalled(), "MatchTrigger should NOT be called when auto-match is disabled")
	})
}

// TestAutoMatch_TriggeredAfterEachUpload verifies that the trigger fires on every
// successful upload — once for ledger, once for bank. Each upload independently
// fires the trigger because the ingestion UseCase doesn't track cross-source state.
func TestAutoMatch_TriggeredAfterEachUpload(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedAutoMatchConfig(t, h, true)
		registerFailureDiagnostics(t, h, seed.ContextID)

		trigger := newCapturingMatchTrigger()
		ctxProvider := &stubContextProvider{enabled: true}

		uc := wireIngestionWithTrigger(t, h, trigger, ctxProvider)

		// Upload to ledger source.
		ledgerCSV := buildCSV("MULTI-001", "300.00", "USD", "2026-03-01", "ledger upload")
		_, err := uc.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"multi_ledger.csv",
			int64(len(ledgerCSV)),
			"csv",
			strings.NewReader(ledgerCSV),
		)
		require.NoError(t, err)
		require.Equal(t, 1, trigger.totalCalls(), "trigger should fire once after ledger upload")

		// Upload to bank source.
		bankCSV := buildCSV("multi-001", "300.00", "USD", "2026-03-01", "bank upload")
		_, err = uc.StartIngestion(
			ctx,
			seed.ContextID,
			seed.NonLedgerSourceID,
			"multi_bank.csv",
			int64(len(bankCSV)),
			"csv",
			strings.NewReader(bankCSV),
		)
		require.NoError(t, err)
		require.Equal(t, 2, trigger.totalCalls(), "trigger should fire again after bank upload")

		// Both calls should reference the same contextID.
		for i, call := range trigger.allCalls() {
			require.Equal(t, seed.ContextID, call.ContextID, "call %d should have correct contextID", i)
			require.Equal(t, h.Seed.TenantID, call.TenantID, "call %d should have correct tenantID", i)
		}
	})
}

// TestAutoMatch_NotTriggeredOnFailedIngestion verifies that when ingestion
// fails (e.g., malformed file), the MatchTrigger is NOT called. Auto-match
// only fires after successful ingestion completion.
func TestAutoMatch_NotTriggeredOnFailedIngestion(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctxBase := e4t9Ctx(t, h)
		ctx, cancel := context.WithTimeout(ctxBase, 90*time.Second)
		defer cancel()

		seed := seedAutoMatchConfig(t, h, true)
		registerFailureDiagnostics(t, h, seed.ContextID)

		trigger := newCapturingMatchTrigger()
		ctxProvider := &stubContextProvider{enabled: true}

		uc := wireIngestionWithTrigger(t, h, trigger, ctxProvider)

		// Upload a CSV with valid headers but completely unparseable data rows.
		// The CSV parser will produce rows but the field mapping will fail to
		// extract required fields, causing all rows to be parse errors.
		// With zero successfully inserted transactions, the ingestion should
		// still complete (as a "completed with errors" job), but since the file
		// is technically parseable, auto-match may still fire.
		//
		// To guarantee ingestion failure, we use a format that doesn't exist.
		_, err := uc.StartIngestion(
			ctx,
			seed.ContextID,
			seed.LedgerSourceID,
			"broken.xyz",
			10,
			"xyz_unsupported_format",
			strings.NewReader("this is not valid data"),
		)
		require.Error(t, err, "ingestion should fail with unsupported format")

		require.False(t, trigger.wasCalled(),
			"MatchTrigger should NOT be called when ingestion fails")
	})
}
