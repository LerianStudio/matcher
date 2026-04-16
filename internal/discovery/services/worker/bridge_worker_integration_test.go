// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// End-to-end integration tests for the fetcher→matcher bridge worker (T-003).
// These scenarios wire the full production pipeline and drive it against:
//
//  1. A real Postgres (testcontainers via the shared integration harness) that
//     hosts the extraction_requests, fetcher_connections, reconciliation_sources,
//     ingestion_jobs, and transactions tables exactly as production would.
//  2. A real Redis (shared harness) used by the bridge worker's distributed
//     lock plus the ingestion pipeline's dedup service.
//  3. A real MinIO (testcontainers) that plays the role of Matcher's custody
//     bucket end-to-end — this is where the verified plaintext lands, and
//     from which the bridge reads it back for ingestion.
//  4. An httptest server that impersonates Fetcher's artifact endpoint,
//     serving real AES-256-GCM ciphertext with the contract-locked HMAC + IV
//     headers so the production verifier runs unmodified.
//
// The test file lives inside package `worker` (not `worker_test`) so it can
// drive the worker's unexported pollCycle helper — the same pattern the unit
// tests use. Build-tag isolation keeps the unit build clean.
//
// No testcontainers for Fetcher: Fetcher is a remote HTTP service and its
// artifact contract is fully captured by the httptest impersonator.
// Containerising Fetcher would add no signal and a lot of flakiness.
package worker

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	redisgo "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/otel"
	"golang.org/x/crypto/hkdf"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	"github.com/LerianStudio/matcher/internal/auth"
	configEntities "github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	configVO "github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	fetcherAdapter "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	extractionRepoPkg "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryVO "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	ingestionParsers "github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	ingestionJobRepoPkg "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/job"
	ingestionTxRepoPkg "github.com/LerianStudio/matcher/internal/ingestion/adapters/postgres/transaction"
	ingestionPorts "github.com/LerianStudio/matcher/internal/ingestion/ports"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	cross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	custodyAdapter "github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	outboxPostgres "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedFee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
	infraTestutil "github.com/LerianStudio/matcher/internal/shared/infrastructure/testutil"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// HKDF info strings and header names must match the production verifier
// exactly. We hard-code them locally (rather than reaching into unexported
// production constants) so a rename on either side surfaces here as a test
// failure instead of a silent rewire.
const (
	bridgeTestHKDFContextHMAC = "fetcher-external-hmac-v1"
	bridgeTestHKDFContextAES  = "fetcher-external-aes-v1"
	bridgeTestHeaderHMAC      = "X-Fetcher-Artifact-Hmac"
	bridgeTestHeaderIV        = "X-Fetcher-Artifact-Iv"

	bridgeMinIOImage      = "minio/minio:RELEASE.2024-10-13T13-34-11Z"
	bridgeMinIORootUser   = "matcheradmin"
	bridgeMinIORootPass   = "matcher-super-secret"
	bridgeMinIOBucket     = "matcher-bridge-integration"
	bridgeMinIOAPIPort    = "9000/tcp"
	bridgeMinIOConsolePrt = "9001/tcp"
)

// sharedBridgeMinIO holds the package-level MinIO container. Starting
// MinIO via testcontainers costs ~5-10s; sharing it across scenarios
// (per-test unique tenant + bucket-prefix ensures isolation) keeps the
// suite responsive.
var (
	sharedBridgeMinIOOnce sync.Once
	sharedBridgeMinIO     *bridgeMinIOHarness
	sharedBridgeMinIOErr  error
)

type bridgeMinIOHarness struct {
	container testcontainers.Container
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
}

func getBridgeMinIOHarness(tb testing.TB) *bridgeMinIOHarness {
	tb.Helper()

	sharedBridgeMinIOOnce.Do(func() {
		sharedBridgeMinIO, sharedBridgeMinIOErr = startBridgeMinIO()
		if sharedBridgeMinIOErr != nil {
			return
		}

		tb.Cleanup(func() {
			if sharedBridgeMinIO == nil || sharedBridgeMinIO.container == nil {
				return
			}

			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_ = sharedBridgeMinIO.container.Terminate(cleanupCtx)
		})
	})

	if sharedBridgeMinIOErr != nil {
		tb.Fatalf("failed to start MinIO testcontainer for bridge worker: %v", sharedBridgeMinIOErr)
	}

	return sharedBridgeMinIO
}

func startBridgeMinIO() (*bridgeMinIOHarness, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        bridgeMinIOImage,
		ExposedPorts: []string{bridgeMinIOAPIPort, bridgeMinIOConsolePrt},
		Env: map[string]string{
			"MINIO_ROOT_USER":     bridgeMinIORootUser,
			"MINIO_ROOT_PASSWORD": bridgeMinIORootPass,
		},
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(bridgeMinIOAPIPort),
			// Pair ForListeningPort + ForLog("API: http://") — MinIO announces
			// API readiness in its log once the S3 endpoint accepts traffic.
			// The paired wait guards against the port-before-ready race we
			// documented in CLAUDE.md memory.
			wait.ForLog("API: http://").WithStartupTimeout(60*time.Second),
		).WithStartupTimeout(90 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start minio container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get minio host: %w", err)
	}

	port, err := container.MappedPort(ctx, bridgeMinIOAPIPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get minio api port: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	harness := &bridgeMinIOHarness{
		container: container,
		endpoint:  endpoint,
		accessKey: bridgeMinIORootUser,
		secretKey: bridgeMinIORootPass,
		bucket:    bridgeMinIOBucket,
	}

	if err := harness.ensureBucket(ctx); err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return harness, nil
}

func (h *bridgeMinIOHarness) ensureBucket(ctx context.Context) error {
	client := h.rawS3Client(ctx)

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(h.bucket),
	})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", h.bucket, err)
	}

	return nil
}

// rawS3Client returns a low-level s3.Client bound directly to the MinIO
// endpoint. Used by assertions that need to HEAD/GET objects without
// going through the production storage client.
func (h *bridgeMinIOHarness) rawS3Client(ctx context.Context) *s3.Client {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(h.accessKey, h.secretKey, ""),
		),
	)
	if err != nil {
		// LoadDefaultConfig failures are bootstrap-level; a test run that
		// reaches this path already has larger problems.
		panic(fmt.Sprintf("load aws config for raw bridge client: %v", err))
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(h.endpoint)
		o.UsePathStyle = true
	})
}

// storageClient builds the same production S3Client the custody store
// wraps in bootstrap. Exercising the production client here proves the
// full wiring path works end-to-end.
func (h *bridgeMinIOHarness) storageClient(ctx context.Context, tb testing.TB) sharedPorts.ObjectStorageClient {
	tb.Helper()

	client, err := reportingStorage.NewS3Client(ctx, reportingStorage.S3Config{
		Endpoint:        h.endpoint,
		Region:          "us-east-1",
		Bucket:          h.bucket,
		AccessKeyID:     h.accessKey,
		SecretAccessKey: h.secretKey,
		UsePathStyle:    true,
		AllowInsecure:   true,
	})
	require.NoError(tb, err, "build production S3 client against MinIO")

	return client
}

// deriveBridgeKey mirrors the production verifier's HKDF derivation so
// tests can synthesise valid Fetcher-shaped ciphertexts without
// duplicating the production implementation. Drift between this helper
// and the production verifier surfaces as an integrity failure in the
// tests, not a silent mismatch.
func deriveBridgeKey(tb testing.TB, master []byte, info string) []byte {
	tb.Helper()

	r := hkdf.New(sha256.New, master, nil, []byte(info))

	key := make([]byte, 32)
	_, err := io.ReadFull(r, key)
	require.NoError(tb, err, "derive key via HKDF")

	return key
}

// encryptBridgeArtifact produces a Fetcher-shaped payload: AES-256-GCM
// ciphertext with a random IV and an HMAC-SHA256 over the ciphertext
// using the external HMAC key. The three returned values become the
// httptest response body + HMAC/IV headers.
func encryptBridgeArtifact(tb testing.TB, master, plaintext []byte) ([]byte, string, string) {
	tb.Helper()

	aesKey := deriveBridgeKey(tb, master, bridgeTestHKDFContextAES)
	hmacKey := deriveBridgeKey(tb, master, bridgeTestHKDFContextHMAC)

	block, err := aes.NewCipher(aesKey)
	require.NoError(tb, err)

	gcm, err := cipher.NewGCM(block)
	require.NoError(tb, err)

	iv := make([]byte, gcm.NonceSize())
	_, err = rand.Read(iv)
	require.NoError(tb, err)

	ciphertext := gcm.Seal(nil, iv, plaintext, nil)

	mac := hmac.New(sha256.New, hmacKey)
	_, err = mac.Write(ciphertext)
	require.NoError(tb, err)

	return ciphertext, hex.EncodeToString(iv), hex.EncodeToString(mac.Sum(nil))
}

// randomBridgeMasterKey returns a fresh APP_ENC_KEY-sized master key.
// Using a fresh key per test keeps scenarios isolated even when they
// share MinIO state.
func randomBridgeMasterKey(tb testing.TB) []byte {
	tb.Helper()

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(tb, err)

	return key
}

// bridgeTestRedisClient opens a real Redis connection against the
// harness's container and returns a *redis.Client. Matches the pattern
// established in tests/integration/matching/helpers_test.go.
func bridgeTestRedisClient(t *testing.T, redisAddr string) *redisgo.Client {
	t.Helper()

	parsed, err := url.Parse(strings.TrimSpace(redisAddr))
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Host)

	client := redisgo.NewClient(&redisgo.Options{Addr: parsed.Host})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("redis client close: %v (expected during teardown)", err)
		}
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(pingCtx).Err(), "redis ping must succeed")

	// Flush the bridge worker's lock key so a prior test's leftover cannot
	// block the first poll cycle in the current test. Namespaced cleanup is
	// preferable to FLUSHDB because the shared harness shares Redis with
	// other integration suites.
	_ = client.Del(pingCtx, "matcher:fetcher_bridge:cycle").Err()

	return client
}

// bridgeTestInfraProvider wraps the harness's postgres client and a real
// *redis.UniversalClient into an InfrastructureProvider. We use the
// mock-provider helper (which is really a real-wiring helper despite the
// name) because it implements the full InfrastructureProvider contract
// against concrete clients, which is exactly what the bridge worker's
// distributed lock + postgres-backed repositories need.
func bridgeTestInfraProvider(
	h *integration.TestHarness,
	redisClient redisgo.UniversalClient,
) sharedPorts.InfrastructureProvider {
	return infraTestutil.NewSingleTenantInfrastructureProvider(
		h.Connection,
		infraTestutil.NewRedisClientWithMock(redisClient),
	)
}

// bridgeTestInfraProviderNoRedis exposes only Postgres — sufficient for
// DB-only assertions that do not touch the Redis path. Keeps the
// assertion helpers decoupled from the full worker wiring.
func bridgeTestInfraProviderNoRedis(h *integration.TestHarness) sharedPorts.InfrastructureProvider {
	return infraTestutil.NewSingleTenantInfrastructureProvider(h.Connection, nil)
}

// bridgeTestTenantLister returns a TenantLister that emits the canned list
// of tenant IDs supplied at construction. Used so the bridge worker can be
// driven without invoking the production pg_namespace enumerator (which
// would only see our seeded data in the public schema via the default
// tenant, which is exactly what we hand-supply here).
type bridgeTestTenantLister struct {
	tenants []string
}

func (l *bridgeTestTenantLister) ListTenants(_ context.Context) ([]string, error) {
	return l.tenants, nil
}

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

// -----------------------------------------------------------------------------
// IS-1: End-to-end bridge worker against real Postgres + Redis + httptest
// Fetcher + MinIO. The happy path drives the full retrieval → verify →
// custody → ingest → link pipeline and verifies every checkpoint
// (custody presence, ingestion job row, link persistence, custody
// delete-after-ingest, and replay idempotency).
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_HappyPath_EndToEnd_PersistsCustodyAndLinks(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-it-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, rawS3 := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		beforeTick, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.False(t, beforeTick.Valid, "pre-tick extraction must be unlinked")

		// --- Tick 1: full pipeline runs ---
		worker.pollCycle(tenantCtx)

		// Assertion 1: the extraction now carries an ingestion_job_id, and
		// its status is still COMPLETE (linking does not change status).
		after1JobID, after1Status, after1Updated := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, after1JobID.Valid, "extraction must be linked after tick 1")
		assert.Equal(t, string(discoveryVO.ExtractionStatusComplete), after1Status,
			"extraction status unchanged by bridge linkage")
		assert.True(t, after1Updated.After(time.Now().Add(-60*time.Second)),
			"UpdatedAt must advance when linking")

		// Assertion 2: the ingestion_jobs row exists and reports the flat
		// JSON row count we supplied (1).
		provider := bridgeTestInfraProviderNoRedis(h)
		jobRepo := ingestionJobRepoPkg.NewRepository(provider)
		job, err := jobRepo.FindByID(tenantCtx, after1JobID.UUID)
		require.NoError(t, err, "ingestion job must be persisted")
		require.NotNil(t, job)
		assert.Equal(t, 1, job.Metadata.TotalRows,
			"one row should have flattened from the seed payload")

		// Assertion 3: the custody object under the tenant prefix has been
		// DELETED after successful ingestion (D2: delete-after-ingest).
		expectedKey := auth.DefaultTenantID + "/fetcher-artifacts/" + seed.extractionID.String() + ".json"
		exists, headErr := bridgeCustodyObjectExists(ctx, rawS3, minIO.bucket, expectedKey)
		require.NoError(t, headErr)
		assert.False(t, exists, "custody object must be gone after delete-after-ingest")

		// --- Tick 2: idempotent replay ---
		// The second tick must not produce a second ingestion job. The
		// extraction is already linked so FindEligibleForBridge yields
		// zero rows.
		worker.pollCycle(tenantCtx)

		after2JobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		assert.Equal(t, after1JobID.UUID, after2JobID.UUID,
			"link must not change on replay")

		// Count ingestion_jobs owned by this test to prove no duplicate
		// bridge run spawned a second ingestion.
		resolver, rErr := h.Connection.Resolver(tenantCtx)
		require.NoError(t, rErr)
		var jobCount int
		err = resolver.QueryRowContext(tenantCtx,
			`SELECT COUNT(*) FROM ingestion_jobs WHERE source_id = $1`,
			seed.sourceID).Scan(&jobCount)
		require.NoError(t, err)
		assert.Equal(t, 1, jobCount,
			"replay must not create a duplicate ingestion job")
	})
}

// -----------------------------------------------------------------------------
// IS-2: Default-tenant inclusion (AC-T2). The shared integration harness
// seeds everything in the public/default schema. This scenario verifies
// that when the bridge worker's TenantLister reports DefaultTenantID, the
// eligible extraction in public still gets processed end-to-end.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_DefaultTenant_IsProcessed(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-default-tenant-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, _ := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		// Sanity check: the tenant id we are driving IS the default tenant.
		// If the harness ever starts seeding against a non-default tenant
		// this assertion fails loudly rather than silently testing the
		// wrong code path.
		require.Equal(t, auth.DefaultTenantID, h.Seed.TenantID.String(),
			"shared harness must seed against DefaultTenantID; otherwise the default-tenant path is not what this test exercises")

		worker.pollCycle(tenantCtx)

		linkedJobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, linkedJobID.Valid,
			"extraction seeded in the default (public) tenant must be bridged end-to-end")
	})
}

// -----------------------------------------------------------------------------
// IS-3: Distributed lock prevents concurrent double-processing. Two
// goroutines drive pollCycle simultaneously; the Redis SetNX guard in
// BridgeWorker.acquireLock must admit only one of them into the critical
// section, and the atomic LinkIfUnlinked write guarantees that even if
// both goroutines happened to race past the lock they still produce at
// most one linked extraction.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeWorker_DistributedLock_PreventsConcurrentBridge(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		minIO := getBridgeMinIOHarness(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		tenantCtx := h.Ctx()

		seed := seedBridgeFixtures(t, tenantCtx, h)

		masterKey := randomBridgeMasterKey(t)
		plaintext := fetcherFlatPayload(t, "bridge-lock-tx-001")
		ciphertext, ivHex, hmacHex := encryptBridgeArtifact(t, masterKey, plaintext)

		server := newBridgeFetcherImpersonator(ciphertext, hmacHex, ivHex)
		t.Cleanup(server.Close)

		worker, _ := buildBridgeWorker(
			t, ctx, h, minIO, server, masterKey,
			[]string{auth.DefaultTenantID},
			seed,
		)

		// Run two pollCycle invocations concurrently. Whichever goroutine
		// wins the lock does the full work; the other acquires the lock and
		// finds the extraction already linked, or is rejected by SetNX
		// outright. Either outcome is acceptable — the invariant is that
		// exactly one ingestion job exists at the end.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			worker.pollCycle(tenantCtx)
		}()
		go func() {
			defer wg.Done()
			worker.pollCycle(tenantCtx)
		}()
		wg.Wait()

		// Invariant: exactly one ingestion_jobs row for our seeded source.
		resolver, rErr := h.Connection.Resolver(tenantCtx)
		require.NoError(t, rErr)
		var jobCount int
		err := resolver.QueryRowContext(tenantCtx,
			`SELECT COUNT(*) FROM ingestion_jobs WHERE source_id = $1`,
			seed.sourceID).Scan(&jobCount)
		require.NoError(t, err)
		assert.Equal(t, 1, jobCount,
			"concurrent pollCycle calls must produce exactly one ingestion job")

		linkedJobID, _, _ := bridgeExtractionStatus(t, tenantCtx, h, seed.extractionID)
		require.True(t, linkedJobID.Valid,
			"extraction must be linked after at least one pollCycle completed")
	})
}
