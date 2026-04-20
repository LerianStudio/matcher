//go:build chaos

// Chaos tests for the T-001 trusted-stream intake seam. They exercise the
// same pipeline as the integration test suite in
// internal/ingestion/services/command/trusted_stream_integration_test.go
// but through the Toxiproxy-fronted chaos harness so the PostgreSQL seam
// can be poisoned mid-call.
//
// File placement: the Gate 7 brief proposed placing this file under
// internal/ingestion/services/command/. The matcher convention keeps every
// chaos test in tests/chaos/ so they all share the 4-container harness via
// InitSharedChaos in tests/chaos/main_test.go. Splitting into a sibling
// package would duplicate the harness per test binary or force internal/*
// code to import tests/*, which the codebase does not do. Convention wins;
// the deviation is surfaced in the Gate 7 handoff.

package chaos

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboxpg "github.com/LerianStudio/lib-commons/v5/commons/outbox/postgres"

	"github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTransactionRepo "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --------------------------------------------------------------------------
// CHAOS-T001-A: Postgres reset_peer during trusted-stream intake
// --------------------------------------------------------------------------

// TestIntegration_Chaos_TrustedStream_PostgresResetPeer covers Gate 7
// Scenario 1: when PostgreSQL drops the TCP connection mid-ingest the
// trusted-stream pipeline must return a wrapped error, write nothing
// partial, and recover cleanly on a subsequent call after the toxic is
// removed. This guards the T-001 intake seam's transactional invariant —
// IngestionJob + transactions + outbox event are all-or-nothing.
//
// Phases (per Gate 7 chaos standard):
//  1. Normal — baseline intake succeeds.
//  2. Inject — reset_peer toxic on PostgreSQL proxy.
//  3. Verify — intake fails cleanly (wrapped error, no panic, no partial writes).
//  4. Restore — remove toxics.
//  5. Recovery — fresh intake succeeds; phantom rows from phase 3 absent.
func TestIntegration_Chaos_TrustedStream_PostgresResetPeer(t *testing.T) {
	requireChaosEnabled(t)

	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	useCase := buildTrustedStreamUseCase(t, h)

	// --- Phase 1: Normal ------------------------------------------------
	baselineCSV := "id,amount,currency,date\n" +
		"chaos-baseline-1,10.00,USD,2024-03-01\n"

	baselineOut, err := useCase.IngestFromTrustedStream(
		context.Background(),
		ingestionCommand.IngestFromTrustedStreamInput{
			ContextID:      h.Seed.ContextID,
			SourceID:       h.Seed.SourceID,
			Format:         "csv",
			Content:        strings.NewReader(baselineCSV),
			SourceMetadata: map[string]string{"filename": "chaos-baseline.csv"},
		},
	)
	require.NoError(t, err, "baseline trusted-stream intake must succeed before injection")
	require.NotNil(t, baselineOut)
	require.Equal(t, 1, baselineOut.TransactionCount, "baseline row must insert")

	// --- Phase 2: Inject ------------------------------------------------
	// timeout=0 means every byte exchange triggers an immediate TCP reset.
	h.InjectPGResetPeer(t, 0)

	// --- Phase 3: Verify -------------------------------------------------
	// The chaos CSV uses a distinctive prefix so Phase 5 can confirm no row
	// landed despite the use case running through most of the pipeline.
	chaosCSV := "id,amount,currency,date\n" +
		"chaos-phantom-1,12.50,USD,2024-03-01\n" +
		"chaos-phantom-2,20.00,USD,2024-03-02\n"

	chaosCtx, chaosCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer chaosCancel()

	chaosOut, chaosErr := useCase.IngestFromTrustedStream(
		chaosCtx,
		ingestionCommand.IngestFromTrustedStreamInput{
			ContextID:      h.Seed.ContextID,
			SourceID:       h.Seed.SourceID,
			Format:         "csv",
			Content:        strings.NewReader(chaosCSV),
			SourceMetadata: map[string]string{"filename": "chaos-phantom.csv"},
		},
	)
	require.Error(t, chaosErr, "intake through poisoned PG proxy must fail")
	require.Nil(t, chaosOut, "no outcome must be returned on failure")
	// Guard: error must come from the PG seam, not from caller-side
	// validation or the nil-adapter path. Those are different defects.
	assert.NotErrorIs(t, chaosErr, ingestionCommand.ErrIngestFromTrustedStreamContentRequired,
		"error must be a downstream failure, not caller-side validation")
	assert.NotErrorIs(t, chaosErr, ingestionCommand.ErrIngestFromTrustedStreamSourceRequired,
		"error must be a downstream failure, not caller-side validation")
	assert.NotErrorIs(t, chaosErr, sharedPorts.ErrNilFetcherBridgeIntake,
		"error must be a downstream failure, not a nil-adapter sentinel")

	// --- Phase 4: Restore ------------------------------------------------
	// RemoveAllToxics plus the t.Cleanup hook registered by InjectPGResetPeer
	// ensure the proxy is clean before Phase 5 (defence in depth against
	// ordering of cleanup calls).
	h.RemoveAllToxics(t)

	// --- Phase 5: Recovery -----------------------------------------------
	recoveryCSV := "id,amount,currency,date\n" +
		"chaos-recovery-1,30.00,USD,2024-03-05\n"

	// The PG connection pool may need a beat to retire the reset-peer
	// connection; Eventually tolerates the pool replacement window.
	require.Eventually(t, func() bool {
		recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recoveryCancel()

		out, rErr := useCase.IngestFromTrustedStream(
			recoveryCtx,
			ingestionCommand.IngestFromTrustedStreamInput{
				ContextID:      h.Seed.ContextID,
				SourceID:       h.Seed.SourceID,
				Format:         "csv",
				Content:        strings.NewReader(recoveryCSV),
				SourceMetadata: map[string]string{"filename": "chaos-recovery.csv"},
			},
		)
		return rErr == nil && out != nil && out.TransactionCount == 1
	}, 15*time.Second, 500*time.Millisecond,
		"trusted-stream intake must recover after PG proxy is restored")

	// Direct DB bypasses Toxiproxy, confirming the chaos-phase rows were
	// rolled back (T-001 AC-F2 transactional integrity under failure).
	directDB := h.DirectDB(t)

	var phantomCount int
	queryErr := directDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM ingestion_transactions
		WHERE external_id LIKE 'chaos-phantom-%'
	`).Scan(&phantomCount)
	require.NoError(t, queryErr, "direct DB phantom-transaction check must work")
	assert.Equal(t, 0, phantomCount,
		"no phantom transactions must be committed under PG reset (found %d)", phantomCount)

	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// CHAOS-T001-C (SKIPPED, HARNESS GAP): Redis unavailable during dedup
// --------------------------------------------------------------------------

// TestIntegration_Chaos_TrustedStream_RedisUnavailable documents a harness
// gap in code rather than in a handoff. The chaos harness supports Redis
// fault injection (InjectRedisTimeout, DisableRedisProxy), but the
// trusted-stream pipeline under T-001 receives its DedupeService from
// UseCaseDeps and the integration-style wiring here plugs in an in-memory
// fake (trustedStreamFakeChaosDedupe). The fake never touches Redis, so
// toxic injection cannot reach T-001.
//
// Closing this gap is out of scope for Gate 7 (write-only, no new
// infrastructure). A follow-up would need to either expose a Redis-backed
// DedupeService constructor the chaos package can wire against the proxied
// Redis, or boot bootstrap.Service against the proxied Redis the way the
// lifecycle chaos tests do for RabbitMQ.
func TestIntegration_Chaos_TrustedStream_RedisUnavailable(t *testing.T) {
	t.Skip("harness gap: trusted-stream pipeline uses in-memory dedup in test wiring; proxied Redis is unreachable without constructing a Redis-backed DedupeService here")
}

// --------------------------------------------------------------------------
// Test wiring
// --------------------------------------------------------------------------

// buildTrustedStreamUseCase wires a real ingestion UseCase against the
// chaos harness's proxied PostgreSQL. Dedup stays in-memory so PG is the
// sole failure seam under test — the chaos standard's principle of
// isolating one fault surface per scenario.
func buildTrustedStreamUseCase(t *testing.T, h *ChaosHarness) *ingestionCommand.UseCase {
	t.Helper()

	provider := h.Provider()
	jobRepo := ingestionJobRepo.NewRepository(provider)
	txRepo := ingestionTransactionRepo.NewRepository(provider)

	// Inline the canonical outbox repository directly. Build-tag isolation
	// (chaos vs integration) prevents importing tests/integration's
	// NewTestOutboxRepository helper, so we replicate the minimal wiring
	// here. WithAllowEmptyTenant mirrors the integration helper because the
	// chaos harness also runs in the public schema.
	schemaResolver, err := outboxpg.NewSchemaResolver(h.Connection, outboxpg.WithAllowEmptyTenant())
	require.NoError(t, err, "create outbox schema resolver for chaos harness")
	outboxRepo, err := outboxpg.NewRepository(h.Connection, schemaResolver, schemaResolver)
	require.NoError(t, err, "create outbox repository for chaos harness")

	fieldMap := &shared.FieldMap{Mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	registry := parsers.NewParserRegistry()
	registry.Register(parsers.NewCSVParser())

	uc, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         jobRepo,
		TransactionRepo: txRepo,
		Dedupe:          &trustedStreamFakeChaosDedupe{},
		Publisher:       &trustedStreamChaosPublisher{},
		OutboxRepo:      outboxRepo,
		Parsers:         registry,
		FieldMapRepo:    &trustedStreamChaosFieldMapStub{fieldMap: fieldMap},
		SourceRepo:      newTrustedStreamChaosSourceStub(h.Seed.ContextID, h.Seed.SourceID),
		DedupeTTL:       0,
	})
	require.NoError(t, err, "build trusted-stream ingestion UseCase")

	return uc
}

// requireChaosEnabled layers the Gate 7 dual-gate pattern over the
// project's existing //go:build chaos tag. The build tag already hides
// this file from default test runs; adding the `-short` skip lets any
// "fast" CI path opt out uniformly.
func requireChaosEnabled(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("chaos: skipped because -short was set")
	}
}

// --------------------------------------------------------------------------
// Minimal stubs (mirror the integration-test helpers without re-exporting)
// --------------------------------------------------------------------------

// trustedStreamChaosFieldMapStub returns a fixed field map regardless of
// source id. The chaos harness seeds a single source, so no dispatch is
// required.
type trustedStreamChaosFieldMapStub struct {
	fieldMap *shared.FieldMap
}

func (f *trustedStreamChaosFieldMapStub) FindBySourceID(
	_ context.Context,
	_ uuid.UUID,
) (*shared.FieldMap, error) {
	return f.fieldMap, nil
}

// trustedStreamChaosSourceStub satisfies ingestion ports.SourceRepository
// for the single context/source pair seeded by the chaos harness.
type trustedStreamChaosSourceStub struct {
	contextID uuid.UUID
	sourceID  uuid.UUID
}

func newTrustedStreamChaosSourceStub(contextID, sourceID uuid.UUID) *trustedStreamChaosSourceStub {
	return &trustedStreamChaosSourceStub{contextID: contextID, sourceID: sourceID}
}

func (s *trustedStreamChaosSourceStub) FindByID(
	_ context.Context,
	contextID, id uuid.UUID,
) (*shared.ReconciliationSource, error) {
	if s.contextID != contextID || s.sourceID != id {
		return nil, errChaosSourceNotFound
	}
	return &shared.ReconciliationSource{ID: s.sourceID, ContextID: s.contextID}, nil
}

// trustedStreamFakeChaosDedupe is a minimal in-memory DedupeService that
// exercises MarkSeenWithRetry correctly and rejects repeated hashes. The
// chaos tests never pre-seed duplicates, so the retry branch stays cold.
type trustedStreamFakeChaosDedupe struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (d *trustedStreamFakeChaosDedupe) CalculateHash(
	sourceID uuid.UUID, externalID string,
) string {
	return sourceID.String() + ":" + externalID
}

func (d *trustedStreamFakeChaosDedupe) IsDuplicate(
	_ context.Context, _ uuid.UUID, hash string,
) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[hash], nil
}

func (d *trustedStreamFakeChaosDedupe) MarkSeen(
	_ context.Context, _ uuid.UUID, hash string, _ time.Duration,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	d.seen[hash] = true
	return nil
}

func (d *trustedStreamFakeChaosDedupe) MarkSeenWithRetry(
	_ context.Context, _ uuid.UUID, hash string, _ time.Duration, _ int,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	if d.seen[hash] {
		return ingestionPorts.ErrDuplicateTransaction
	}
	d.seen[hash] = true
	return nil
}

func (d *trustedStreamFakeChaosDedupe) Clear(
	_ context.Context, _ uuid.UUID, hash string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, hash)
	return nil
}

func (d *trustedStreamFakeChaosDedupe) ClearBatch(
	_ context.Context, _ uuid.UUID, hashes []string,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, h := range hashes {
		delete(d.seen, h)
	}
	return nil
}

// trustedStreamChaosPublisher is a no-op IngestionEventPublisher. The
// outbox is the authoritative path for integration-level assertions; the
// publisher just needs to exist so the UseCase constructor accepts it.
type trustedStreamChaosPublisher struct{}

func (*trustedStreamChaosPublisher) PublishIngestionCompleted(
	_ context.Context, _ *shared.IngestionCompletedEvent,
) error {
	return nil
}

func (*trustedStreamChaosPublisher) PublishIngestionFailed(
	_ context.Context, _ *shared.IngestionFailedEvent,
) error {
	return nil
}

// Compile-time interface satisfaction keeps contract drift loud if the
// ingestion ports evolve.
var (
	_ ingestionPorts.DedupeService        = (*trustedStreamFakeChaosDedupe)(nil)
	_ ingestionPorts.FieldMapRepository   = (*trustedStreamChaosFieldMapStub)(nil)
	_ ingestionPorts.SourceRepository     = (*trustedStreamChaosSourceStub)(nil)
	_ sharedPorts.IngestionEventPublisher = (*trustedStreamChaosPublisher)(nil)
)

// errChaosSourceNotFound is returned when the stub does not recognise a
// context/source pair. Defined as a sentinel so the pipeline's error
// wrapping is visible in failure output.
var errChaosSourceNotFound = errors.New("chaos stub: source not found")
