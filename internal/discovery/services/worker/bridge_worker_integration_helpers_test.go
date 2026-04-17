// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Shared MinIO/Redis/crypto fixtures for the fetcher→matcher bridge worker
// integration scenarios. The scenarios + test-specific fixtures live in
// sibling files; keeping infrastructure setup in its own file keeps each
// file under the 500-line Ring cap.
package worker

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
	redisgo "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/hkdf"

	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
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
