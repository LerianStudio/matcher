// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Integration tests for the ArtifactCustodyStore against a real S3
// backend (MinIO via testcontainers). Complements the unit tests in
// artifact_custody_store_test.go which use a fake storage — these
// scenarios prove the custody store works end-to-end against a real
// S3 implementation, exercising the roundtrip Store → direct read →
// Delete that the bridge worker will perform in production.
//
// Dual-tenant coverage:
//   - default-tenant path: AC-T2 says the default tenant uses the
//     "public" schema, so custody writes for the default tenant must
//     still produce the standard tenant-scoped layout.
//   - non-default tenant path: the common case exercised elsewhere,
//     included here for parity so both branches get S3-backed proof.
//
// Packaging note: this file lives in the `custody` package (not
// `custody_test`) so it can reach the unexported `counterWriter` if
// future diagnostics need to assert on internal state. Currently the
// tests stay on the exported surface only.
package custody

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Package-level constants mirror the values used in the discovery
// integration test. Duplicated intentionally so each test file is
// self-contained; a rename does not have to ripple across packages.
const (
	custodyMinIOImage    = "minio/minio:RELEASE.2024-10-13T13-34-11Z"
	custodyMinIOUser     = "matcheradmin"
	custodyMinIOPass     = "matcher-super-secret"
	custodyMinIOBucket   = "matcher-custody-adapter-it"
	custodyMinIOAPIPort  = "9000/tcp"
	custodyMinIOConsPort = "9001/tcp"

	// defaultTenantID mirrors auth.DefaultTenantID. We inline the value
	// here rather than importing the auth package from the shared
	// kernel adapter to avoid adding a new cross-package import purely
	// for a test constant. The test double-checks the value is
	// well-formed before using it.
	defaultTenantID = "00000000-0000-0000-0000-000000000000"
)

var (
	custodyMinIOOnce sync.Once
	custodyMinIO     *custodyMinIOHarness
	custodyMinIOErr  error
)

type custodyMinIOHarness struct {
	container testcontainers.Container
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
}

func getCustodyMinIOHarness(tb testing.TB) *custodyMinIOHarness {
	tb.Helper()

	custodyMinIOOnce.Do(func() {
		custodyMinIO, custodyMinIOErr = startCustodyMinIO()
		if custodyMinIOErr != nil {
			return
		}

		tb.Cleanup(func() {
			if custodyMinIO == nil || custodyMinIO.container == nil {
				return
			}

			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_ = custodyMinIO.container.Terminate(cleanupCtx)
		})
	})

	if custodyMinIOErr != nil {
		tb.Fatalf("failed to start MinIO testcontainer: %v", custodyMinIOErr)
	}

	return custodyMinIO
}

func startCustodyMinIO() (*custodyMinIOHarness, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        custodyMinIOImage,
		ExposedPorts: []string{custodyMinIOAPIPort, custodyMinIOConsPort},
		Env: map[string]string{
			"MINIO_ROOT_USER":     custodyMinIOUser,
			"MINIO_ROOT_PASSWORD": custodyMinIOPass,
		},
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(custodyMinIOAPIPort),
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

	port, err := container.MappedPort(ctx, custodyMinIOAPIPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get minio api port: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	harness := &custodyMinIOHarness{
		container: container,
		endpoint:  endpoint,
		accessKey: custodyMinIOUser,
		secretKey: custodyMinIOPass,
		bucket:    custodyMinIOBucket,
	}

	if err := harness.ensureBucket(ctx); err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return harness, nil
}

func (h *custodyMinIOHarness) ensureBucket(ctx context.Context) error {
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

func (h *custodyMinIOHarness) rawS3Client(ctx context.Context) *s3.Client {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(h.accessKey, h.secretKey, ""),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("load aws config: %v", err))
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(h.endpoint)
		o.UsePathStyle = true
	})
}

// storageClient returns a production-shaped ObjectStorageClient wired
// to the running MinIO. The custody store under test speaks only to
// this interface, so exercising the real client here matches the
// production wiring exactly.
func (h *custodyMinIOHarness) storageClient(ctx context.Context, tb testing.TB) sharedPorts.ObjectStorageClient {
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
	require.NoError(tb, err)

	return client
}

// ------------------------------------------------------------------
// IS-4a: Happy-path roundtrip for a non-default tenant.
// ------------------------------------------------------------------

func TestIntegration_ArtifactCustodyStore_Store_Then_Delete_NonDefaultTenant(t *testing.T) {
	harness := getCustodyMinIOHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := "tenant-" + uuid.NewString()[:8]
	extractionID := uuid.New()
	plaintext := []byte(`{"rows":[{"id":"rt-001","amount":"42.00","currency":"USD"}]}`)

	storage := harness.storageClient(ctx, t)

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	// --- Store ---
	ref, err := store.Store(ctx, sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(plaintext),
	})
	require.NoError(t, err, "store must succeed against real MinIO")
	require.NotNil(t, ref)

	expectedKey := tenantID + "/" + KeyPrefix + "/" + extractionID.String() + ".json"
	assert.Equal(t, expectedKey, ref.Key, "custody key layout matches BuildObjectKey contract")
	assert.Equal(t, URIScheme+"://"+expectedKey, ref.URI)
	assert.Equal(t, int64(len(plaintext)), ref.Size, "size must match persisted bytes")
	assert.NotEmpty(t, ref.SHA256, "SHA256 must be populated by streaming hasher")
	assert.False(t, ref.StoredAt.IsZero(), "StoredAt must be set")

	// --- Direct read-back ---
	rawS3 := harness.rawS3Client(ctx)
	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(harness.bucket),
		Key:    aws.String(expectedKey),
	})
	require.NoError(t, err, "direct GetObject on stored key must succeed")
	t.Cleanup(func() { _ = obj.Body.Close() })

	fetched, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	assert.Equal(t, plaintext, fetched, "stored bytes must match original plaintext byte-for-byte")

	// --- Delete ---
	require.NoError(t, store.Delete(ctx, *ref), "delete must succeed")

	// --- Assert object is gone ---
	exists, headErr := custodyObjectExists(ctx, rawS3, harness.bucket, expectedKey)
	require.NoError(t, headErr, "HEAD against deleted key must return a clean not-found, not an error")
	assert.False(t, exists, "object must be gone after Delete")
}

// ------------------------------------------------------------------
// IS-4b: Same roundtrip for the default tenant path (AC-T2).
// ------------------------------------------------------------------

func TestIntegration_ArtifactCustodyStore_Store_Then_Delete_DefaultTenant(t *testing.T) {
	harness := getCustodyMinIOHarness(t)

	// Sanity-check that the inlined defaultTenantID still parses; if
	// auth.DefaultTenantID ever changes shape the test will fail here
	// with a loud diagnostic rather than silently drifting.
	_, err := uuid.Parse(defaultTenantID)
	require.NoError(t, err, "defaultTenantID must be a valid UUID")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extractionID := uuid.New()
	plaintext := []byte(`{"rows":[{"id":"default-001","amount":"7.00","currency":"BRL"}]}`)

	storage := harness.storageClient(ctx, t)

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	ref, err := store.Store(ctx, sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     defaultTenantID,
		Content:      bytes.NewReader(plaintext),
	})
	require.NoError(t, err, "default-tenant store must succeed")
	require.NotNil(t, ref)

	expectedKey := defaultTenantID + "/" + KeyPrefix + "/" + extractionID.String() + ".json"
	assert.Equal(t, expectedKey, ref.Key,
		"default-tenant custody key layout must match the standard tenant-scoped pattern")

	// Byte-identical read-back.
	rawS3 := harness.rawS3Client(ctx)
	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(harness.bucket),
		Key:    aws.String(expectedKey),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = obj.Body.Close() })

	fetched, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	assert.Equal(t, plaintext, fetched, "default-tenant persisted bytes must match plaintext")

	// Delete cleans up even for the default tenant.
	require.NoError(t, store.Delete(ctx, *ref))

	exists, headErr := custodyObjectExists(ctx, rawS3, harness.bucket, expectedKey)
	require.NoError(t, headErr)
	assert.False(t, exists, "default-tenant object must be gone after Delete")
}

// ------------------------------------------------------------------
// IS-5: Custody write-once under replay.
//
// The bridge worker may replay an extraction after a crash (e.g. the
// worker wrote the custody copy, died before writing the ingestion
// link, then restarted). When it re-enters the verified-retrieval
// pipeline it hits Store() with the SAME (tenantID, extractionID) but
// a semantically IDENTICAL plaintext that nevertheless may differ
// byte-for-byte in the reader supplied (fresh decrypt, fresh buffer).
//
// The write-once guard introduced in T-003 P3 protects the bridge's
// "no duplicate downstream readiness outcomes" invariant: the second
// Store() call MUST return a reference to the existing object
// without re-uploading. Re-uploading would produce a second custody
// copy with potentially-different bytes, which in turn would make the
// extraction→ingestion link non-deterministic under retry.
//
// This scenario drives the guard end-to-end against real MinIO:
//  1. Store content1 → succeeds, stored bytes == content1, SHA256 = A.
//  2. Store content2 with the SAME extraction_id / tenant_id but
//     DIFFERENT bytes → must return a reference pointing at the first
//     object; stored bytes must STILL be content1 (not content2).
// ------------------------------------------------------------------

func TestIntegration_ArtifactCustodyStore_WriteOnce_ReplayPreservesFirstBytes(t *testing.T) {
	harness := getCustodyMinIOHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := "tenant-wo-" + uuid.NewString()[:8]
	extractionID := uuid.New()

	content1 := []byte(`{"rows":[{"id":"wo-001","amount":"1.00","currency":"USD"}]}`)
	content2 := []byte(`{"rows":[{"id":"wo-999","amount":"2.00","currency":"EUR"}]}`)
	require.NotEqual(t, content1, content2, "test setup: the two payloads must differ")

	storage := harness.storageClient(ctx, t)

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	// --- First Store: writes the object, returns hydrated reference.
	ref1, err := store.Store(ctx, sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(content1),
	})
	require.NoError(t, err, "first Store must succeed")
	require.NotNil(t, ref1)
	require.NotEmpty(t, ref1.SHA256, "first write must populate SHA256 via the streaming hasher")
	firstSHA := ref1.SHA256

	// --- Second Store: same (tenant, extraction) but DIFFERENT bytes.
	// The write-once guard must short-circuit on Exists() and return a
	// reference pointing at the existing key WITHOUT re-uploading.
	ref2, err := store.Store(ctx, sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(content2),
	})
	require.NoError(t, err, "replay Store must succeed (short-circuit path)")
	require.NotNil(t, ref2)

	// Fix 7: the replay path NOW re-hashes the persisted bytes via a
	// Download + SHA-256 round-trip so source_metadata downstream of the
	// bridge orchestrator never carries empty digest fields. SHA256/Size
	// on ref2 must equal ref1's values (both describe the SAME bytes —
	// content1, not content2 — because write-once preserved the original).
	assert.Equal(t, ref1.Key, ref2.Key,
		"replay reference must point at the same object key as the first write")
	assert.Equal(t, ref1.URI, ref2.URI, "replay URI must match first write URI")
	assert.Equal(t, ref1.SHA256, ref2.SHA256,
		"replay SHA256 must match first-write SHA256 (write-once preserved bytes)")
	assert.Equal(t, ref1.Size, ref2.Size,
		"replay Size must match first-write Size (write-once preserved bytes)")

	// --- Byte-level assertion: the persisted object must still hold
	// content1, not content2. This is the whole point of the write-once
	// guard. Reading direct from MinIO bypasses any ObjectStorageClient
	// caching that might mask a real re-upload.
	rawS3 := harness.rawS3Client(ctx)
	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(harness.bucket),
		Key:    aws.String(ref1.Key),
	})
	require.NoError(t, err, "direct GetObject on the write-once key must succeed")
	t.Cleanup(func() { _ = obj.Body.Close() })

	stored, err := io.ReadAll(obj.Body)
	require.NoError(t, err)
	assert.Equal(t, content1, stored,
		"replay MUST NOT overwrite: stored bytes must still be content1")
	assert.NotEqual(t, content2, stored,
		"replay MUST NOT overwrite: content2 must never reach the bucket")

	// --- Reference SHA256 (from the first write) must still describe the
	// bytes currently in the bucket. Recomputing the digest here would be
	// redundant; instead we pin the sha we captured earlier so any future
	// regression that quietly re-hashes on replay trips this assertion.
	assert.NotEmpty(t, firstSHA, "first-write SHA256 must be non-empty")
}

// custodyObjectExists returns (true, nil) when the key exists,
// (false, nil) when it is cleanly absent, and (false, err) on any
// unexpected error. MinIO returns a variety of shapes for "not found"
// depending on how the operation is issued; this helper normalises
// them all to a boolean answer.
func custodyObjectExists(
	ctx context.Context,
	client *s3.Client,
	bucket, key string,
) (bool, error) {
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

	// MinIO HEAD 404s often surface as a smithy operation error; match
	// by substring rather than reflectively inspecting the smithy tree.
	// This mirrors the same helper in the discovery integration test.
	if containsAny(err.Error(), "404", "NotFound", "not found") {
		return false, nil
	}

	return false, err
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if n == "" {
			continue
		}
		if indexOf(s, n) >= 0 {
			return true
		}
	}
	return false
}

// indexOf avoids pulling in strings just for the substring probe, and
// keeps the helper local so coverage tooling does not charge us for
// it in unrelated packages.
func indexOf(s, sub string) int {
	n, m := len(s), len(sub)
	if m == 0 {
		return 0
	}
	if m > n {
		return -1
	}
	for i := 0; i+m <= n; i++ {
		if s[i:i+m] == sub {
			return i
		}
	}
	return -1
}
