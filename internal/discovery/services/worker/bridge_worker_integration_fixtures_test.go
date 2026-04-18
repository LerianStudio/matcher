// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Scenario-local fake stubs, seed fixtures, and worker-wiring helpers for
// the bridge worker integration scenarios. Kept separate from the MinIO/
// crypto infrastructure in bridge_worker_integration_helpers_test.go so
// each file stays under the 500-line Ring cap.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	fetcherAdapter "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	extractionRepoPkg "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	ingestionParsers "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepoPkg "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepoPkg "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	cross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	custodyAdapter "github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	outboxPostgres "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedFee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// bridgeTestNopMatchTrigger is a no-op MatchTrigger used by the ingestion
// pipeline. The bridge tests care about ingestion side-effects (job row +
// transaction rows + extraction link) not auto-match fan-out.
type bridgeTestNopMatchTrigger struct{}

func (*bridgeTestNopMatchTrigger) TriggerMatchForContext(_ context.Context, _ uuid.UUID, _ uuid.UUID) {
}

// bridgeTestFakeDedupe is a minimal in-memory DedupeService. Mirrors the
// shape used in trusted_stream_integration_helpers_test.go. The bridge
// tests never pre-seed duplicates so MarkSeenWithRetry's
// duplicate-retry branch stays cold.
type bridgeTestFakeDedupe struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (d *bridgeTestFakeDedupe) CalculateHash(sourceID uuid.UUID, externalID string) string {
	return sourceID.String() + ":" + externalID
}

func (d *bridgeTestFakeDedupe) IsDuplicate(_ context.Context, _ uuid.UUID, hash string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[hash], nil
}

func (d *bridgeTestFakeDedupe) MarkSeen(_ context.Context, _ uuid.UUID, hash string, _ time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	d.seen[hash] = true
	return nil
}

func (d *bridgeTestFakeDedupe) MarkSeenWithRetry(_ context.Context, _ uuid.UUID, hash string, _ time.Duration, _ int) error {
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

func (d *bridgeTestFakeDedupe) Clear(_ context.Context, _ uuid.UUID, hash string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, hash)
	return nil
}

func (d *bridgeTestFakeDedupe) ClearBatch(_ context.Context, _ uuid.UUID, hashes []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, h := range hashes {
		delete(d.seen, h)
	}
	return nil
}

// bridgeTestFieldMapStub returns a field map that matches the flattened
// Fetcher JSON we ship in the scenarios below.
type bridgeTestFieldMapStub struct {
	mapping map[string]any
}

func (f *bridgeTestFieldMapStub) FindBySourceID(_ context.Context, _ uuid.UUID) (*sharedDomain.FieldMap, error) {
	return &sharedDomain.FieldMap{Mapping: f.mapping}, nil
}

// bridgeTestSourceStub satisfies ingestion ports.SourceRepository. The
// scenarios seed exactly one (context, source) pair via the harness +
// direct SQL, so a single-shot stub is sufficient.
type bridgeTestSourceStub struct {
	contextID uuid.UUID
	sourceID  uuid.UUID
}

func (s *bridgeTestSourceStub) FindByID(
	_ context.Context,
	contextID, id uuid.UUID,
) (*sharedDomain.ReconciliationSource, error) {
	if s.contextID != contextID || s.sourceID != id {
		return nil, fmt.Errorf("source not found")
	}
	return &sharedDomain.ReconciliationSource{ID: s.sourceID, ContextID: s.contextID}, nil
}

// bridgeTestIngestionPublisher is a no-op publisher; the assertions read
// the outbox table directly because that is the durable contract with
// downstream consumers.
type bridgeTestIngestionPublisher struct{}

func (*bridgeTestIngestionPublisher) PublishIngestionCompleted(_ context.Context, _ *sharedDomain.IngestionCompletedEvent) error {
	return nil
}
func (*bridgeTestIngestionPublisher) PublishIngestionFailed(_ context.Context, _ *sharedDomain.IngestionFailedEvent) error {
	return nil
}

// Compile-time interface satisfaction checks keep signature drift loud.
var (
	_ sharedPorts.TenantLister            = (*bridgeTestTenantLister)(nil)
	_ sharedPorts.MatchTrigger            = (*bridgeTestNopMatchTrigger)(nil)
	_ ingestionPorts.DedupeService        = (*bridgeTestFakeDedupe)(nil)
	_ ingestionPorts.FieldMapRepository   = (*bridgeTestFieldMapStub)(nil)
	_ ingestionPorts.SourceRepository     = (*bridgeTestSourceStub)(nil)
	_ sharedPorts.IngestionEventPublisher = (*bridgeTestIngestionPublisher)(nil)
)

// bridgeSeed records the IDs of every parent row the bridge pipeline
// needs so individual scenarios can assert on persisted state.
type bridgeSeed struct {
	connectionID uuid.UUID
	sourceID     uuid.UUID
	contextID    uuid.UUID
	extractionID uuid.UUID
	resultPath   string
}

// seedBridgeFixtures inserts the parent rows the bridge pipeline needs
// and returns the IDs for downstream assertions. The seed runs against
// the harness's default tenant (public schema) because the shared
// integration harness uses DefaultTenantID; auth.ApplyTenantSchema
// short-circuits for that tenant and leaves search_path at public.
func seedBridgeFixtures(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
) bridgeSeed {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries, "harness must expose at least one primary DB")
	primary := primaries[0]
	require.NotNil(t, primary)

	// 1) Seed fetcher_connections. fetcher_conn_id is UNIQUE so we
	// fingerprint it with a UUID suffix per-test to avoid cross-test
	// collisions under the shared harness.
	connID := uuid.New()
	fetcherConnHandle := "bridge-conn-" + uuid.NewString()[:8]

	_, err = primary.ExecContext(ctx, `
		INSERT INTO fetcher_connections (
			id, fetcher_conn_id, config_name, database_type, host, port,
			database_name, product_name, status
		)
		VALUES ($1, $2, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')
	`, connID, fetcherConnHandle)
	require.NoError(t, err, "seed fetcher_connections")

	// 2) Seed reconciliation_sources with type=FETCHER and a config
	// referencing the fetcher_connections row above. The bridge source
	// resolver keys off `config->>'connection_id'` so the JSON payload
	// must carry the connection's UUID.
	srcEntity, err := configEntities.NewReconciliationSource(
		ctx,
		h.Seed.ContextID,
		configEntities.CreateReconciliationSourceInput{
			Name: "Integration Fetcher Source",
			Type: configVO.SourceTypeFetcher,
			Side: sharedFee.MatchingSideRight,
			Config: map[string]any{
				"connection_id": connID.String(),
			},
		},
	)
	require.NoError(t, err)

	// Persist the source via direct SQL rather than the configuration
	// source repository so the test is decoupled from repository wiring
	// churn. The columns written here match the live schema and the
	// additive `side` column from migration 000017.
	configJSON, mErr := json.Marshal(srcEntity.Config)
	require.NoError(t, mErr)
	srcEntity.ID = uuid.New()
	_, err = primary.ExecContext(ctx, `
		INSERT INTO reconciliation_sources (id, context_id, name, type, config, side, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
	`, srcEntity.ID, srcEntity.ContextID, srcEntity.Name, string(srcEntity.Type), configJSON, string(srcEntity.Side))
	require.NoError(t, err, "seed reconciliation_sources (FETCHER)")

	// 3) Seed extraction_requests in state COMPLETE with NULL
	// ingestion_job_id. result_path must be an absolute path per the
	// extraction_requests CHECK constraint — the URL passed to the
	// httptest Fetcher is the httptest baseURL concatenated with this
	// path.
	extractionID := uuid.New()
	resultPath := "/v1/extractions/" + extractionID.String() + "/artifact"

	_, err = primary.ExecContext(ctx, `
		INSERT INTO extraction_requests (
			id, connection_id, status, fetcher_job_id, result_path,
			tables, created_at, updated_at
		)
		VALUES ($1, $2, 'COMPLETE', $3, $4, '{}'::jsonb, NOW(), NOW())
	`, extractionID, connID, "fetcher-job-"+extractionID.String()[:8], resultPath)
	require.NoError(t, err, "seed extraction_requests (COMPLETE, unlinked)")

	return bridgeSeed{
		connectionID: connID,
		sourceID:     srcEntity.ID,
		contextID:    h.Seed.ContextID,
		extractionID: extractionID,
		resultPath:   resultPath,
	}
}

// buildBridgeWorker wires the full production bridge pipeline against the
// real Postgres/Redis/MinIO/httptest stack. Returning the worker plus the
// MinIO raw client lets tests assert directly on custody object presence.
//
// seedSourceID is looked up post-seed because the helper that seeds the
// FETCHER source needs to run before the ingestion UseCase can be
// constructed (the source stub must know the persisted id).
func buildBridgeWorker(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	mh *bridgeMinIOHarness,
	fetcherServer *httptest.Server,
	masterKey []byte,
	tenants []string,
	seed bridgeSeed,
) (*BridgeWorker, *s3.Client) {
	t.Helper()

	redisUniversal := bridgeTestRedisClient(t, h.RedisAddr)
	provider := bridgeTestInfraProvider(h, redisUniversal)

	// --- Discovery plumbing -------------------------------------------------
	extractRepo := extractionRepoPkg.NewRepository(provider)

	gateway, err := fetcherAdapter.NewArtifactRetrievalClient(&http.Client{Timeout: 10 * time.Second})
	require.NoError(t, err)

	verifier, err := fetcherAdapter.NewArtifactVerifier(masterKey)
	require.NoError(t, err)

	storage := mh.storageClient(ctx, t)
	custody, err := custodyAdapter.NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	verifiedOrchestr, err := discoveryCommand.NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, custody)
	require.NoError(t, err)

	// --- Ingestion plumbing -------------------------------------------------
	jobRepo := ingestionJobRepoPkg.NewRepository(provider)
	txRepo := ingestionTxRepoPkg.NewRepository(provider)
	outboxRepo := outboxPostgres.NewRepository(provider)

	fieldMap := &bridgeTestFieldMapStub{mapping: map[string]any{
		"external_id": "id",
		"amount":      "amount",
		"currency":    "currency",
		"date":        "date",
	}}

	srcStub := &bridgeTestSourceStub{contextID: seed.contextID, sourceID: seed.sourceID}

	registry := ingestionParsers.NewParserRegistry()
	registry.Register(ingestionParsers.NewJSONParser())

	ingestionUC, err := ingestionCommand.NewUseCase(ingestionCommand.UseCaseDeps{
		JobRepo:         jobRepo,
		TransactionRepo: txRepo,
		Dedupe:          &bridgeTestFakeDedupe{},
		Publisher:       &bridgeTestIngestionPublisher{},
		OutboxRepo:      outboxRepo,
		Parsers:         registry,
		FieldMapRepo:    fieldMap,
		SourceRepo:      srcStub,
		DedupeTTL:       time.Minute,
	})
	require.NoError(t, err)

	intakeAdapter, err := cross.NewFetcherBridgeIntakeAdapter(ingestionUC)
	require.NoError(t, err)

	linkWriter, err := cross.NewExtractionLifecycleLinkWriterAdapter(extractRepo)
	require.NoError(t, err)

	sourceResolver, err := cross.NewBridgeSourceResolverAdapter(provider)
	require.NoError(t, err)

	baseURL := fetcherServer.URL
	bridgeOrch, err := discoveryCommand.NewBridgeExtractionOrchestrator(
		extractRepo,
		verifiedOrchestr,
		custody,
		intakeAdapter,
		linkWriter,
		sourceResolver,
		discoveryCommand.BridgeOrchestratorConfig{
			FetcherBaseURLGetter: func() string { return baseURL },
			MaxExtractionBytes:   0, // defaults to package default
			Flatten:              fetcherAdapter.FlattenFetcherJSON,
		},
	)
	require.NoError(t, err)

	tenantLister := &bridgeTestTenantLister{tenants: tenants}

	worker, err := NewBridgeWorker(
		bridgeOrch,
		extractRepo,
		tenantLister,
		provider,
		BridgeWorkerConfig{Interval: 30 * time.Second, BatchSize: 50},
		&libLog.NopLogger{},
	)
	require.NoError(t, err)
	// Keep the tracer non-nil so span.Start calls don't panic outside of a
	// bootstrap context.
	worker.tracer = otel.Tracer("bridge_worker_integration_test")

	return worker, mh.rawS3Client(ctx)
}

// newBridgeFetcherImpersonator returns an httptest.Server that serves the
// given ciphertext with the Fetcher-contract HMAC + IV headers. Each test
// instantiates its own server so tampered payloads stay scoped to a
// single scenario.
func newBridgeFetcherImpersonator(ciphertext []byte, hmacHex, ivHex string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(bridgeTestHeaderHMAC, hmacHex)
		w.Header().Set(bridgeTestHeaderIV, ivHex)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(ciphertext)
	}))
}

// fetcherFlatPayload encodes a synthetic Fetcher extraction JSON that
// flattens into a single transaction row. Keeping the payload tiny makes
// scenario assertions cheap while still exercising the full
// FlattenFetcherJSON → JSON parser ingestion path.
func fetcherFlatPayload(t *testing.T, transactionID string) []byte {
	t.Helper()

	payload := map[string]map[string][]map[string]any{
		"ds-a": {
			"accounts": {
				{
					"id":       transactionID,
					"amount":   "100.00",
					"currency": "USD",
					"date":     "2026-01-15T00:00:00Z",
				},
			},
		},
	}

	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	return encoded
}

// bridgeExtractionStatus returns the extraction_requests row's
// ingestion_job_id and status columns for the given id. Used by
// assertions that need to see raw DB state (e.g. the ingestion_job_id
// column after a bridge cycle). Reaches into the DB directly to stay
// decoupled from the repository's domain entity decoding.
func bridgeExtractionStatus(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	id uuid.UUID,
) (ingestionJobID uuid.NullUUID, status string, updatedAt time.Time) {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)

	err = resolver.QueryRowContext(ctx,
		`SELECT ingestion_job_id, status, updated_at FROM extraction_requests WHERE id = $1`,
		id,
	).Scan(&ingestionJobID, &status, &updatedAt)
	require.NoError(t, err, "read extraction_requests row")
	return ingestionJobID, status, updatedAt
}

// bridgeCustodyObjectExists queries MinIO directly to check whether a
// custody object is present at the expected key. Normalises the various
// smithy/s3 not-found shapes into a simple boolean so assertions stay
// clean.
func bridgeCustodyObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}

	var notFound *types.NotFound
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &notFound) || errors.As(err, &noSuchKey) {
		return false, nil
	}

	msg := err.Error()
	if strings.Contains(msg, "404") || strings.Contains(msg, "NotFound") || strings.Contains(msg, "not found") {
		return false, nil
	}

	return false, err
}
