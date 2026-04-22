// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// End-to-end integration tests for T-002's verified artifact retrieval
// pipeline. These scenarios wire the production
// ArtifactRetrievalClient + ArtifactVerifier + ArtifactCustodyStore +
// VerifiedArtifactRetrievalOrchestrator and drive them against:
//
//  1. A real S3-compatible object store (MinIO via testcontainers) used
//     as custody — this proves tenant-scoped paths land where we
//     document and that plaintext roundtrips byte-identically.
//  2. An httptest server that impersonates Fetcher's artifact endpoint
//     and serves real AES-256-GCM ciphertext with the contract-locked
//     HMAC + IV headers — this proves the verifier accepts valid
//     Fetcher payloads and rejects tampered ones.
//
// No testcontainers for Fetcher: Fetcher is a remote HTTP service and
// its artifact contract is fully captured by the httptest impersonator.
// Containerising Fetcher would add no signal and a lot of flakiness.
package command_test

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
	"net/http"
	"net/http/httptest"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/hkdf"

	fetcherAdapter "github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	reportingStorage "github.com/LerianStudio/matcher/internal/reporting/adapters/storage"
	custodyAdapter "github.com/LerianStudio/matcher/internal/shared/adapters/custody"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// HKDF info strings must match the production verifier exactly; the
// whole point of these tests is to verify the contract so we hard-code
// the strings here rather than reach into unexported production
// constants.
const (
	testHKDFContextHMAC = "fetcher-external-hmac-v1"
	testHKDFContextAES  = "fetcher-external-aes-v1"

	// Fetcher artifact header names — match production constants in
	// fetcher/artifact_retrieval.go. Duplicated here intentionally so a
	// rename of the production constants shows up as a test failure
	// rather than a silent rewire.
	testHeaderArtifactHMAC = "X-Fetcher-Artifact-Hmac"
	testHeaderArtifactIV   = "X-Fetcher-Artifact-Iv"

	// minioImage and credentials used by the MinIO testcontainer. MinIO
	// is S3-compatible and has a single public image, which makes it a
	// cleaner fit for testcontainers than SeaweedFS (which requires a
	// config file mount we would have to synthesise per test run).
	minioImage      = "minio/minio:RELEASE.2024-10-13T13-34-11Z"
	minioRootUser   = "matcheradmin"
	minioRootPass   = "matcher-super-secret"
	minioBucket     = "matcher-custody-integration"
	minioAPIPort    = "9000/tcp"
	minioConsolePrt = "9001/tcp"
)

// sharedMinIO holds the package-level MinIO container. testcontainers
// spin-up is expensive (~5-10s) so we share across scenarios in the
// same package. Each test still creates its own bucket-prefix-free
// client and cleans up via unique tenant IDs.
var (
	sharedMinIOOnce sync.Once
	sharedMinIO     *minioHarness
	sharedMinIOErr  error
)

// minioHarness carries the handle needed to build an S3Client against a
// running MinIO container.
type minioHarness struct {
	container testcontainers.Container
	endpoint  string
	accessKey string
	secretKey string
	bucket    string
}

func getMinIOHarness(tb testing.TB) *minioHarness {
	tb.Helper()

	sharedMinIOOnce.Do(func() {
		sharedMinIO, sharedMinIOErr = startMinIOContainer()
		if sharedMinIOErr != nil {
			return
		}

		tb.Cleanup(func() {
			if sharedMinIO == nil || sharedMinIO.container == nil {
				return
			}

			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			_ = sharedMinIO.container.Terminate(cleanupCtx)
		})
	})

	if sharedMinIOErr != nil {
		tb.Fatalf("failed to start MinIO testcontainer: %v", sharedMinIOErr)
	}

	return sharedMinIO
}

func startMinIOContainer() (*minioHarness, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        minioImage,
		ExposedPorts: []string{minioAPIPort, minioConsolePrt},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioRootUser,
			"MINIO_ROOT_PASSWORD": minioRootPass,
		},
		// MinIO in "server" mode requires a data path; /data is the
		// canonical value used by the upstream Docker examples.
		Cmd: []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(minioAPIPort),
			// MinIO logs "API: http://..." once the S3 endpoint is
			// ready to accept traffic. Pairing the log wait with a
			// listening-port wait guards against the port-before-ready
			// race we saw on Postgres (see CLAUDE.md memory entry).
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

	port, err := container.MappedPort(ctx, minioAPIPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("get minio api port: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	harness := &minioHarness{
		container: container,
		endpoint:  endpoint,
		accessKey: minioRootUser,
		secretKey: minioRootPass,
		bucket:    minioBucket,
	}

	if err := harness.ensureBucket(ctx); err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return harness, nil
}

// ensureBucket creates the working bucket if it does not already exist.
// A MinIO container starts with no buckets; the production code is not
// responsible for bucket creation, so we do it here in the harness.
func (h *minioHarness) ensureBucket(ctx context.Context) error {
	client := h.rawS3Client(ctx)

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(h.bucket),
	})
	if err != nil {
		// If the bucket already exists (shared across tests), that is
		// fine — BucketAlreadyOwnedByYou maps to that case in MinIO.
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", h.bucket, err)
	}

	return nil
}

// rawS3Client returns a low-level s3.Client used by the harness for
// bucket creation and direct HEAD/GetObject/ListObjectsV2 assertions.
// The production code under test wires its own S3Client via the
// reportingStorage.NewS3Client constructor.
func (h *minioHarness) rawS3Client(ctx context.Context) *s3.Client {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(h.accessKey, h.secretKey, ""),
		),
	)
	if err != nil {
		// Can happen only on really exotic env failures; harness callers
		// are expected to stop the whole test run in that case.
		panic(fmt.Sprintf("load aws config for raw client: %v", err))
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(h.endpoint)
		o.UsePathStyle = true
	})
}

// storageClient builds the production ObjectStorageClient the custody
// store depends on. Keeping this in a helper means every test gets the
// same construction path exercised.
func (h *minioHarness) storageClient(ctx context.Context, tb testing.TB) *reportingStorage.S3Client {
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

// deriveKey mirrors the production verifier's HKDF derivation so tests
// can build valid Fetcher-style ciphertexts. Keeping the derivation
// logic inline (instead of calling the unexported production helper)
// proves that any drift between the test fixture and production fails
// verification loudly.
func deriveKey(tb testing.TB, master []byte, info string) []byte {
	tb.Helper()

	r := hkdf.New(sha256.New, master, nil, []byte(info))

	key := make([]byte, 32)
	_, err := io.ReadFull(r, key)
	require.NoError(tb, err, "derive key via HKDF")

	return key
}

// encryptArtifact produces the same shape of payload Fetcher would:
//   - AES-256-GCM ciphertext with a random 12-byte IV
//   - HMAC-SHA256 over the ciphertext using the HKDF-derived HMAC key
//
// Returns (ciphertext, ivHex, hmacHex) ready to be served by the
// httptest Fetcher impersonator.
func encryptArtifact(tb testing.TB, master, plaintext []byte) ([]byte, string, string) {
	tb.Helper()

	aesKey := deriveKey(tb, master, testHKDFContextAES)
	hmacKey := deriveKey(tb, master, testHKDFContextHMAC)

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

// fetcherImpersonator returns an httptest.Server that behaves like
// Fetcher's artifact endpoint. Behaviour is configurable per test
// via the handler closure — e.g. serve valid ciphertext, tampered
// HMAC, or 500 errors for transient-failure scenarios.
func fetcherImpersonator(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(handler)
}

// randomMasterKey returns a fresh APP_ENC_KEY-sized (32 byte) master
// key. Using a fresh key per test makes sure scenarios stay isolated
// even when re-run across MinIO state.
func randomMasterKey(tb testing.TB) []byte {
	tb.Helper()

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(tb, err)

	return key
}

// buildVerifiedPipeline wires the production orchestrator the same way
// bootstrap does. Returning the individual components too lets tests
// assert on custody state directly.
func buildVerifiedPipeline(
	tb testing.TB,
	ctx context.Context,
	masterKey []byte,
	harness *minioHarness,
) (
	*discoveryCommand.VerifiedArtifactRetrievalOrchestrator,
	sharedPorts.ArtifactCustodyStore,
	objectstorage.Backend,
) {
	tb.Helper()

	gateway, err := fetcherAdapter.NewArtifactRetrievalClient(&http.Client{Timeout: 10 * time.Second})
	require.NoError(tb, err)

	verifier, err := fetcherAdapter.NewArtifactVerifier(masterKey)
	require.NoError(tb, err)

	storage := harness.storageClient(ctx, tb)
	custody, err := custodyAdapter.NewArtifactCustodyStore(storage)
	require.NoError(tb, err)

	orch, err := discoveryCommand.NewVerifiedArtifactRetrievalOrchestrator(gateway, verifier, custody)
	require.NoError(tb, err)

	return orch, custody, storage
}

// headObjectExists queries the MinIO backend directly (not through the
// production abstraction) to verify whether an object is present. Going
// direct prevents the existence check from being fooled by any caching
// layer in the production client.
func headObjectExists(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
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

	// MinIO returns 404 wrapped in smithy operation errors that don't
	// unwrap cleanly to types.NotFound. Treat any error whose string
	// includes "404" or "NotFound" as "absent" — we care about
	// presence vs absence, not the specific error taxonomy.
	msg := err.Error()
	if strings.Contains(msg, "404") || strings.Contains(msg, "NotFound") || strings.Contains(msg, "not found") {
		return false, nil
	}

	return false, err
}

// listTenantObjects returns every key under a tenant prefix. Used to
// assert "no custody write happened" by proving the tenant prefix is
// empty after a failed retrieval.
func listTenantObjects(ctx context.Context, client *s3.Client, bucket, tenantID string) ([]string, error) {
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(tenantID + "/"),
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}

	return keys, nil
}

// ------------------------------------------------------------------
// IS-1: End-to-end verified retrieval against real MinIO + httptest.
// ------------------------------------------------------------------

func TestIntegration_VerifiedArtifactRetrieval_HappyPath_PersistsPlaintext(t *testing.T) {
	harness := getMinIOHarness(t)

	masterKey := randomMasterKey(t)
	plaintext := []byte(`{"transactions":[{"id":"tx-001","amount":"100.00","currency":"USD"}]}`)

	ciphertext, ivHex, hmacHex := encryptArtifact(t, masterKey, plaintext)

	server := fetcherImpersonator(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(testHeaderArtifactHMAC, hmacHex)
		w.Header().Set(testHeaderArtifactIV, ivHex)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(ciphertext)
	})
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	orch, _, _ := buildVerifiedPipeline(t, ctx, masterKey, harness)

	tenantID := "it-happy-" + uuid.NewString()[:8]
	extractionID := uuid.New()

	out, err := orch.RetrieveAndCustodyVerifiedArtifact(ctx, discoveryCommand.VerifiedArtifactRetrievalInput{
		Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
			ExtractionID: extractionID,
			TenantID:     tenantID,
			URL:          server.URL + "/v1/extractions/" + extractionID.String() + "/artifact",
		},
	})
	require.NoError(t, err, "happy path must not return an error")
	require.NotNil(t, out, "orchestrator must return an output")
	require.NotNil(t, out.Custody, "custody reference must be present")

	// Assertion 1: key layout is {tenantID}/fetcher-artifacts/{extractionID}.json
	expectedKey := tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json"
	assert.Equal(t, expectedKey, out.Custody.Key, "custody key layout matches documented contract")
	assert.True(t,
		strings.HasPrefix(out.Custody.URI, "custody://"),
		"custody URI must use the custody:// scheme (got %q)", out.Custody.URI,
	)

	// Assertion 2: the stored object's bytes equal the original plaintext.
	rawS3 := harness.rawS3Client(ctx)
	obj, err := rawS3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(harness.bucket),
		Key:    aws.String(expectedKey),
	})
	require.NoError(t, err, "custody object must exist in MinIO")
	t.Cleanup(func() { _ = obj.Body.Close() })

	stored, err := io.ReadAll(obj.Body)
	require.NoError(t, err, "read stored custody object body")
	assert.Equal(t, plaintext, stored, "custody must hold verified plaintext byte-for-byte")

	// Assertion 3: SHA-256 reported on the reference matches the sha256
	// of what is actually in storage.
	gotSHA := sha256.Sum256(stored)
	assert.Equal(t, hex.EncodeToString(gotSHA[:]), out.Custody.SHA256,
		"reference SHA256 must match the persisted plaintext")

	// Assertion 4: size on the reference matches the persisted size.
	assert.Equal(t, int64(len(plaintext)), out.Custody.Size,
		"reference Size must match the persisted plaintext length")
}

// ------------------------------------------------------------------
// IS-2: Integrity failure path — tampered HMAC must not custody.
// ------------------------------------------------------------------

func TestIntegration_VerifiedArtifactRetrieval_TamperedHMAC_ReturnsTerminalAndDoesNotCustody(t *testing.T) {
	harness := getMinIOHarness(t)

	masterKey := randomMasterKey(t)
	plaintext := []byte(`{"transactions":[{"id":"tx-tamper-001","amount":"1.00","currency":"USD"}]}`)

	ciphertext, ivHex, _ := encryptArtifact(t, masterKey, plaintext)

	// Tamper the HMAC: flip the last hex nibble. The result is still a
	// valid hex string (so hex decoding succeeds) but cannot possibly
	// match the legitimate digest — this drives the verifier through
	// the HMAC-mismatch path, not a malformed-input path.
	_, _, realHMACHex := encryptArtifact(t, masterKey, plaintext)
	tamperedHMACHex := flipLastNibble(realHMACHex)
	require.NotEqual(t, realHMACHex, tamperedHMACHex, "nibble flip must change the digest")

	server := fetcherImpersonator(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(testHeaderArtifactHMAC, tamperedHMACHex)
		w.Header().Set(testHeaderArtifactIV, ivHex)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(ciphertext)
	})
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	orch, _, _ := buildVerifiedPipeline(t, ctx, masterKey, harness)

	tenantID := "it-tamper-" + uuid.NewString()[:8]
	extractionID := uuid.New()

	out, err := orch.RetrieveAndCustodyVerifiedArtifact(ctx, discoveryCommand.VerifiedArtifactRetrievalInput{
		Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
			ExtractionID: extractionID,
			TenantID:     tenantID,
			URL:          server.URL + "/v1/extractions/" + extractionID.String() + "/artifact",
		},
	})
	require.Error(t, err, "tampered HMAC must surface a terminal verification error")
	require.Nil(t, out, "no output must be returned on verification failure")
	require.ErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed,
		"error must be the terminal integrity sentinel, not a transient retrieval sentinel")
	require.NotErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed,
		"retrieval succeeded — verification is what failed")

	// Assert nothing was written to custody. Listing under the tenant
	// prefix is authoritative because our tenant IDs are unique per
	// test run.
	rawS3 := harness.rawS3Client(ctx)
	keys, err := listTenantObjects(ctx, rawS3, harness.bucket, tenantID)
	require.NoError(t, err, "list tenant prefix in MinIO")
	assert.Empty(t, keys, "custody prefix must be empty — verification failed before persistence")
}

// ------------------------------------------------------------------
// IS-3: Retrieval failure is transient, not terminal.
// ------------------------------------------------------------------

func TestIntegration_VerifiedArtifactRetrieval_ServerError_ReturnsTransientAndDoesNotCustody(t *testing.T) {
	harness := getMinIOHarness(t)

	masterKey := randomMasterKey(t)

	server := fetcherImpersonator(func(w http.ResponseWriter, _ *http.Request) {
		// 500 on every call simulates Fetcher being unavailable. The
		// retrieval classifier collapses 5xx to ErrArtifactRetrievalFailed
		// so the bridge worker can drive retry with backoff (T-005).
		w.WriteHeader(http.StatusInternalServerError)
	})
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	orch, _, _ := buildVerifiedPipeline(t, ctx, masterKey, harness)

	tenantID := "it-500-" + uuid.NewString()[:8]
	extractionID := uuid.New()

	out, err := orch.RetrieveAndCustodyVerifiedArtifact(ctx, discoveryCommand.VerifiedArtifactRetrievalInput{
		Descriptor: sharedPorts.ArtifactRetrievalDescriptor{
			ExtractionID: extractionID,
			TenantID:     tenantID,
			URL:          server.URL + "/v1/extractions/" + extractionID.String() + "/artifact",
		},
	})
	require.Error(t, err, "5xx upstream must surface an error")
	require.Nil(t, out, "no output must be returned when retrieval fails")
	require.ErrorIs(t, err, sharedPorts.ErrArtifactRetrievalFailed,
		"error must be the transient retrieval sentinel so callers retry")
	require.NotErrorIs(t, err, sharedPorts.ErrIntegrityVerificationFailed,
		"retrieval never reached the verifier — must not surface as terminal")

	// Nothing to custody on retrieval failure.
	rawS3 := harness.rawS3Client(ctx)
	keys, err := listTenantObjects(ctx, rawS3, harness.bucket, tenantID)
	require.NoError(t, err, "list tenant prefix in MinIO")
	assert.Empty(t, keys, "custody prefix must be empty — retrieval failed before verification")
}

// flipLastNibble returns a copy of hex string s with its final nibble
// toggled. Used to tamper HMACs in IS-2 without producing a malformed
// hex input (which would take a different code path in the verifier).
func flipLastNibble(s string) string {
	if s == "" {
		return s
	}

	last := s[len(s)-1]

	switch {
	case last >= '0' && last <= '8':
		last++
	case last == '9':
		last = 'a'
	case last >= 'a' && last <= 'e':
		last++
	case last == 'f':
		last = '0'
	}

	return s[:len(s)-1] + string(last)
}
