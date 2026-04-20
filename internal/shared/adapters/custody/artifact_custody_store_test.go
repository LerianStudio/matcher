// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package custody

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// fakeObjectStorage is a manual mock of ObjectStorageClient. The custody
// store exercises UploadIfAbsent, Download, and Delete; the remaining
// methods return zero values so tests stay focused on the paths under
// test. We tick the interface as small as possible because go.uber.org/mock
// is overkill for a ~7-method port per CLAUDE.md guidance.
//
// uploadIfAbsentErr controls the UploadIfAbsent branch:
//   - nil: happy-path success (first write).
//   - sharedPorts.ErrObjectAlreadyExists wrapped: replay path (object
//     already exists); custody Store falls through to recoverDigest.
//   - any other error: transient failure; Store wraps with
//     ErrCustodyStoreFailed.
type fakeObjectStorage struct {
	uploadKey           string
	uploadContentType   string
	uploadBody          []byte
	uploadErr           error
	uploadIfAbsentErr   error
	deleteKey           string
	deleteErr           error
	downloadKey         string
	downloadBody        []byte
	downloadReader      io.ReadCloser // if non-nil, takes precedence over downloadBody
	downloadErr         error
	existsResult        bool
	existsErr           error
	uploadCalls         int
	uploadIfAbsentCalls int
	deleteCalls         int
	downloadCalls       int
	existsCalls         int
}

func (f *fakeObjectStorage) Upload(
	_ context.Context,
	key string,
	reader io.Reader,
	contentType string,
) (string, error) {
	f.uploadCalls++
	f.uploadKey = key
	f.uploadContentType = contentType

	if reader != nil {
		buf, _ := io.ReadAll(reader)
		f.uploadBody = buf
	}

	if f.uploadErr != nil {
		return "", f.uploadErr
	}

	return key, nil
}

// UploadIfAbsent models the conditional PUT path used by ArtifactCustodyStore
// post-C7. When uploadIfAbsentErr is set, the fake returns it verbatim so
// tests can exercise both the happy path and the ErrObjectAlreadyExists
// replay branch. On success we ALSO drain the reader (and populate
// uploadBody / uploadContentType) so assertions about what would have been
// written stay accurate, mirroring the real S3 PutObject semantics.
func (f *fakeObjectStorage) UploadIfAbsent(
	_ context.Context,
	key string,
	reader io.Reader,
	contentType string,
) (string, error) {
	f.uploadIfAbsentCalls++
	f.uploadKey = key
	f.uploadContentType = contentType

	// The custody store streams plaintext through a TeeReader; we still
	// need to read it even on the ErrObjectAlreadyExists branch so the
	// fake mirrors real S3 behaviour (the body is consumed during request
	// signing before the server responds 412). A test that wants to assert
	// "upload body was not persisted" should use uploadIfAbsentCalls and
	// downloadCalls, not uploadBody.
	if reader != nil {
		buf, _ := io.ReadAll(reader)
		f.uploadBody = buf
	}

	if f.uploadIfAbsentErr != nil {
		return "", f.uploadIfAbsentErr
	}

	return key, nil
}

func (f *fakeObjectStorage) UploadWithOptions(
	ctx context.Context,
	key string,
	reader io.Reader,
	contentType string,
	_ ...storageopt.UploadOption,
) (string, error) {
	return f.Upload(ctx, key, reader, contentType)
}

func (f *fakeObjectStorage) Download(_ context.Context, key string) (io.ReadCloser, error) {
	f.downloadCalls++
	f.downloadKey = key

	if f.downloadErr != nil {
		return nil, f.downloadErr
	}

	if f.downloadReader != nil {
		return f.downloadReader, nil
	}

	return io.NopCloser(bytes.NewReader(f.downloadBody)), nil
}

func (f *fakeObjectStorage) Delete(_ context.Context, key string) error {
	f.deleteCalls++
	f.deleteKey = key

	return f.deleteErr
}

func (f *fakeObjectStorage) GeneratePresignedURL(
	_ context.Context,
	_ string,
	_ time.Duration,
) (string, error) {
	return "", nil
}

func (f *fakeObjectStorage) Exists(_ context.Context, _ string) (bool, error) {
	f.existsCalls++

	return f.existsResult, f.existsErr
}

func TestNewArtifactCustodyStore_NilStorage(t *testing.T) {
	t.Parallel()

	s, err := NewArtifactCustodyStore(nil)
	require.Nil(t, s)
	require.ErrorIs(t, err, ErrNilObjectStorage)
}

func TestNewArtifactCustodyStore_AcceptsStorage(t *testing.T) {
	t.Parallel()

	s, err := NewArtifactCustodyStore(&fakeObjectStorage{})
	require.NoError(t, err)
	require.NotNil(t, s)
}

func TestStore_HappyPath_WritesTenantScopedKey(t *testing.T) {
	t.Parallel()

	storage := &fakeObjectStorage{}
	frozen := time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC)

	store, err := NewArtifactCustodyStore(
		storage,
		WithNowFunc(func() time.Time { return frozen }),
	)
	require.NoError(t, err)

	extractionID := uuid.New()
	tenantID := "tenant-abc"
	plaintext := []byte(`{"rows":[{"id":1,"amount":"100.00"}]}`)

	ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(plaintext),
	})
	require.NoError(t, err)
	require.NotNil(t, ref)

	expectedKey := tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json"
	assert.Equal(t, expectedKey, storage.uploadKey, "tenant-scoped key layout")
	assert.Equal(t, "application/json", storage.uploadContentType)
	assert.Equal(t, plaintext, storage.uploadBody, "plaintext forwarded to upload")
	assert.Equal(t, 1, storage.uploadIfAbsentCalls, "conditional upload must run exactly once on happy path")
	assert.Equal(t, 0, storage.uploadCalls, "non-conditional Upload must NOT be used")
	assert.Equal(t, 0, storage.existsCalls, "Exists must NOT be probed — condition is enforced server-side")

	assert.Equal(t, expectedKey, ref.Key)
	assert.Equal(t, URIScheme+"://"+expectedKey, ref.URI)
	assert.Equal(t, int64(len(plaintext)), ref.Size)

	expectedSHA := sha256.Sum256(plaintext)
	assert.Equal(t, hex.EncodeToString(expectedSHA[:]), ref.SHA256)
	assert.Equal(t, frozen, ref.StoredAt)
}

func TestStore_InputValidation(t *testing.T) {
	t.Parallel()

	store, err := NewArtifactCustodyStore(&fakeObjectStorage{})
	require.NoError(t, err)

	t.Run("missing extraction id", func(t *testing.T) {
		t.Parallel()

		_, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
			TenantID: "tenant",
			Content:  strings.NewReader("x"),
		})
		require.ErrorIs(t, err, sharedPorts.ErrArtifactExtractionIDRequired)
	})

	t.Run("missing tenant id", func(t *testing.T) {
		t.Parallel()

		_, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
			ExtractionID: uuid.New(),
			Content:      strings.NewReader("x"),
		})
		require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
	})

	t.Run("nil content", func(t *testing.T) {
		t.Parallel()

		_, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
			ExtractionID: uuid.New(),
			TenantID:     "tenant",
		})
		require.ErrorIs(t, err, sharedPorts.ErrArtifactCiphertextRequired)
	})
}

func TestStore_UploadError_WrapsSentinel(t *testing.T) {
	t.Parallel()

	backendErr := errors.New("s3 putobject refused")
	// Drive the error through UploadIfAbsent — the conditional-PUT path is
	// how custody Store now reaches storage. Setting only the legacy
	// uploadErr field would never fire because Store no longer calls
	// Upload directly.
	storage := &fakeObjectStorage{uploadIfAbsentErr: backendErr}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	_, err = store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: uuid.New(),
		TenantID:     "tenant",
		Content:      strings.NewReader("plaintext"),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	// Backend error stays in the chain so operators can diagnose it.
	require.ErrorIs(t, err, backendErr)
	// The conditional-upload error must NOT be misread as a replay.
	require.NotErrorIs(t, err, sharedPorts.ErrObjectAlreadyExists)
}

func TestStore_TenantWithSlash_IsRejected(t *testing.T) {
	t.Parallel()

	store, err := NewArtifactCustodyStore(&fakeObjectStorage{})
	require.NoError(t, err)

	_, err = store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: uuid.New(),
		TenantID:     "malicious/../../escape",
		Content:      strings.NewReader("x"),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
}

func TestDelete_HappyPath_CallsStorage(t *testing.T) {
	t.Parallel()

	storage := &fakeObjectStorage{}
	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	ref := sharedPorts.ArtifactCustodyReference{
		Key: "tenant-abc/fetcher-artifacts/" + uuid.New().String() + ".json",
		URI: URIScheme + "://tenant-abc/fetcher-artifacts/x.json",
	}

	require.NoError(t, store.Delete(context.Background(), ref))
	assert.Equal(t, 1, storage.deleteCalls)
	assert.Equal(t, ref.Key, storage.deleteKey)
}

func TestDelete_MissingKey_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	storage := &fakeObjectStorage{}
	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	err = store.Delete(context.Background(), sharedPorts.ArtifactCustodyReference{})
	require.ErrorIs(t, err, ErrCustodyRefRequired)
	assert.Equal(t, 0, storage.deleteCalls)
}

func TestDelete_StorageError_WrapsSentinel(t *testing.T) {
	t.Parallel()

	backendErr := errors.New("s3 delete refused")
	storage := &fakeObjectStorage{deleteErr: backendErr}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	err = store.Delete(context.Background(), sharedPorts.ArtifactCustodyReference{
		Key: "tenant/fetcher-artifacts/extraction.json",
	})
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	require.ErrorIs(t, err, backendErr)
}

func TestBuildObjectKey_Layout(t *testing.T) {
	t.Parallel()

	extractionID := uuid.New()

	t.Run("valid tenant", func(t *testing.T) {
		t.Parallel()

		key, err := BuildObjectKey("tenant-42", extractionID)
		require.NoError(t, err)
		assert.Equal(t, "tenant-42/fetcher-artifacts/"+extractionID.String()+".json", key)
	})

	t.Run("empty tenant rejected", func(t *testing.T) {
		t.Parallel()

		_, err := BuildObjectKey("   ", extractionID)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
	})

	t.Run("tenant with slash rejected", func(t *testing.T) {
		t.Parallel()

		_, err := BuildObjectKey("bad/tenant", extractionID)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
	})

	// Regression: fuzz-discovered. A tenant id with an embedded NUL byte
	// was accepted pre-fix, which produced an object key that masquerades
	// as a normal tenant key in any downstream code that truncates on NUL
	// (C-string FS APIs, some S3 clients).
	t.Run("tenant with NUL byte rejected", func(t *testing.T) {
		t.Parallel()

		_, err := BuildObjectKey("tenant\x00evil", extractionID)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactTenantIDRequired)
	})

	// Regression: control bytes below 0x20 are all rejected — not just
	// NUL — to foreclose smuggling CR/LF/TAB into object-storage keys.
	t.Run("tenant with other control byte rejected", func(t *testing.T) {
		t.Parallel()

		for _, b := range []byte{0x01, 0x09 /*TAB*/, 0x0A /*LF*/, 0x0D /*CR*/, 0x1F, 0x7F /*DEL*/} {
			tenant := "tenant" + string(b) + "suffix"
			_, err := BuildObjectKey(tenant, extractionID)
			require.ErrorIsf(
				t, err, sharedPorts.ErrArtifactTenantIDRequired,
				"control byte 0x%02x should be rejected", b,
			)
		}
	})

	t.Run("nil extraction rejected", func(t *testing.T) {
		t.Parallel()

		_, err := BuildObjectKey("tenant", uuid.Nil)
		require.ErrorIs(t, err, sharedPorts.ErrArtifactExtractionIDRequired)
	})
}

// TestBuildObjectKey_DualTenantMode asserts the custody key layout is
// preserved for BOTH default-tenant mode (where Matcher operates against
// the `public` schema and the tenant id is the literal tenant slug) AND
// non-default multi-tenant mode (where each tenant lives under a UUID
// schema). AC-T2 requires tenant scoping to function identically in both
// operating modes — the custody adapter must never special-case the
// default tenant because cross-tenant collisions would allow silent data
// bleed. The custody store sees only the tenant id string: whichever form
// the bootstrap layer hands it, the key layout stays
// "{tenantID}/fetcher-artifacts/{extractionID}.json".
//
// We toggle MULTI_TENANT_ENABLED via t.Setenv for both branches so the
// test documents the intent even though custody itself does not read the
// variable directly — the invariant is "output is shape-identical
// regardless of mode".
func TestBuildObjectKey_DualTenantMode(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	extractionID := uuid.New()

	t.Run("default-tenant mode (public schema, slug as id)", func(t *testing.T) {
		t.Setenv("MULTI_TENANT_ENABLED", "false")

		// In default-tenant mode the id is a slug (the Matcher default is
		// `default`), not a UUID. The custody adapter must accept it
		// without special-casing.
		key, err := BuildObjectKey("default", extractionID)
		require.NoError(t, err)
		assert.Equal(t,
			"default/fetcher-artifacts/"+extractionID.String()+".json",
			key,
			"default tenant key layout matches multi-tenant layout",
		)
	})

	t.Run("non-default multi-tenant mode (UUID tenant id)", func(t *testing.T) {
		t.Setenv("MULTI_TENANT_ENABLED", "true")

		tenantUUID := uuid.New().String()

		key, err := BuildObjectKey(tenantUUID, extractionID)
		require.NoError(t, err)
		assert.Equal(t,
			tenantUUID+"/fetcher-artifacts/"+extractionID.String()+".json",
			key,
			"multi-tenant key layout matches default-tenant layout",
		)
	})

	t.Run("both modes produce comparable keys under same layout", func(t *testing.T) {
		t.Setenv("MULTI_TENANT_ENABLED", "true")

		// The invariant under test: different tenant IDs produce distinct
		// top-level prefixes so multi-tenant isolation holds. Default
		// tenant (`default`) and any multi-tenant UUID must never collide.
		defaultKey, err := BuildObjectKey("default", extractionID)
		require.NoError(t, err)

		tenantUUID := uuid.New().String()
		mtKey, err := BuildObjectKey(tenantUUID, extractionID)
		require.NoError(t, err)

		assert.NotEqual(t,
			defaultKey,
			mtKey,
			"default tenant prefix must not collide with multi-tenant UUID prefix",
		)
		assert.True(t,
			strings.HasPrefix(defaultKey, "default/fetcher-artifacts/"),
			"default tenant rooted under `default/`",
		)
		assert.True(t,
			strings.HasPrefix(mtKey, tenantUUID+"/fetcher-artifacts/"),
			"multi-tenant rooted under its UUID",
		)
	})
}

// TestStore_DualTenantMode_KeyLayoutPreserved exercises the full Store
// call in both tenant modes (not just BuildObjectKey) so we prove the
// write path itself respects AC-T2 end-to-end. Without this the invariant
// only covers the pure helper.
func TestStore_DualTenantMode_KeyLayoutPreserved(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	extractionID := uuid.New()
	plaintext := []byte(`{"rows":[]}`)

	t.Run("default-tenant mode", func(t *testing.T) {
		t.Setenv("MULTI_TENANT_ENABLED", "false")

		storage := &fakeObjectStorage{}
		store, err := NewArtifactCustodyStore(storage)
		require.NoError(t, err)

		ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
			ExtractionID: extractionID,
			TenantID:     "default",
			Content:      bytes.NewReader(plaintext),
		})
		require.NoError(t, err)
		require.NotNil(t, ref)

		expected := "default/fetcher-artifacts/" + extractionID.String() + ".json"
		assert.Equal(t, expected, storage.uploadKey)
		assert.Equal(t, expected, ref.Key)
		assert.Equal(t, URIScheme+"://"+expected, ref.URI)
	})

	t.Run("non-default multi-tenant mode", func(t *testing.T) {
		t.Setenv("MULTI_TENANT_ENABLED", "true")

		tenantUUID := uuid.New().String()
		storage := &fakeObjectStorage{}
		store, err := NewArtifactCustodyStore(storage)
		require.NoError(t, err)

		ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
			ExtractionID: extractionID,
			TenantID:     tenantUUID,
			Content:      bytes.NewReader(plaintext),
		})
		require.NoError(t, err)
		require.NotNil(t, ref)

		expected := tenantUUID + "/fetcher-artifacts/" + extractionID.String() + ".json"
		assert.Equal(t, expected, storage.uploadKey)
		assert.Equal(t, expected, ref.Key)
		assert.Equal(t, URIScheme+"://"+expected, ref.URI)
	})
}

func TestStore_NilReceiver_Sentinel(t *testing.T) {
	t.Parallel()

	var store *ArtifactCustodyStore

	_, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: uuid.New(),
		TenantID:     "tenant",
		Content:      strings.NewReader("x"),
	})
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
}

func TestDelete_NilReceiver_Sentinel(t *testing.T) {
	t.Parallel()

	var store *ArtifactCustodyStore

	err := store.Delete(context.Background(), sharedPorts.ArtifactCustodyReference{
		Key: "tenant/fetcher-artifacts/x.json",
	})
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
}

// TestOpen_HappyPath_ReturnsReader verifies Open streams the stored plaintext
// back to the caller (AC-F2: bridge worker consumes custody output without
// re-downloading from Fetcher). Covers bridge_extraction_commands.ingestAndLink's
// dependency on a functional custody.Open.
func TestOpen_HappyPath_ReturnsReader(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"datasources":[]}`)
	storage := &fakeObjectStorage{downloadBody: payload}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	ref := sharedPorts.ArtifactCustodyReference{
		Key: "tenant-a/fetcher-artifacts/" + uuid.New().String() + ".json",
	}

	reader, err := store.Open(context.Background(), ref)
	require.NoError(t, err)
	require.NotNil(t, reader)

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, payload, got)
	assert.Equal(t, 1, storage.downloadCalls)
	assert.Equal(t, ref.Key, storage.downloadKey)
}

// TestOpen_NilReceiver_ReturnsSentinel asserts the defensive nil-receiver
// guard returns a wrapped sentinel rather than panicking. This path is
// symmetric with Store/Delete nil-receiver guards.
func TestOpen_NilReceiver_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	var store *ArtifactCustodyStore

	reader, err := store.Open(context.Background(), sharedPorts.ArtifactCustodyReference{
		Key: "tenant/fetcher-artifacts/x.json",
	})
	require.Nil(t, reader)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
}

// TestOpen_NilStorage_ReturnsSentinel covers the second branch of the guard:
// the receiver is non-nil but storage is nil (defense-in-depth against a
// constructor regression).
func TestOpen_NilStorage_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	store := &ArtifactCustodyStore{}

	reader, err := store.Open(context.Background(), sharedPorts.ArtifactCustodyReference{
		Key: "tenant/fetcher-artifacts/x.json",
	})
	require.Nil(t, reader)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
}

// TestOpen_MissingKey_ReturnsSentinel verifies the key-required guard rejects
// blank and whitespace-only refs before touching storage. This prevents the
// adapter from issuing an S3 Download("") which would 404 with a less
// actionable error.
func TestOpen_MissingKey_ReturnsSentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
	}{
		{name: "empty key", key: ""},
		{name: "whitespace only", key: "   "},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			storage := &fakeObjectStorage{}
			store, err := NewArtifactCustodyStore(storage)
			require.NoError(t, err)

			reader, err := store.Open(context.Background(), sharedPorts.ArtifactCustodyReference{
				Key: tt.key,
			})
			require.Nil(t, reader)
			require.ErrorIs(t, err, ErrCustodyRefRequired)
			assert.Equal(t, 0, storage.downloadCalls, "storage should not be invoked for blank key")
		})
	}
}

// TestOpen_DownloadError_WrapsSentinel verifies a storage-level failure is
// wrapped with ErrCustodyStoreFailed so worker logging can classify the
// failure as custody-transient rather than a logic bug.
func TestOpen_DownloadError_WrapsSentinel(t *testing.T) {
	t.Parallel()

	storage := &fakeObjectStorage{downloadErr: errors.New("s3 timeout")}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	reader, err := store.Open(context.Background(), sharedPorts.ArtifactCustodyReference{
		Key: "tenant/fetcher-artifacts/x.json",
	})
	require.Nil(t, reader)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	require.ErrorContains(t, err, "download")
	require.ErrorContains(t, err, "s3 timeout")
}

// TestStore_ReplayPath_RehashesPersistedBytes exercises the
// ErrObjectAlreadyExists branch of conditional upload: when the custody
// key already exists, UploadIfAbsent returns ErrObjectAlreadyExists; Store
// performs one extra Download() to recompute SHA-256 and Size from the
// persisted bytes so source_metadata downstream of the bridge orchestrator
// never carries empty digest fields.
//
// Pre-C7 behavior used Exists + Upload, which was TOCTOU-vulnerable. The
// test now drives the replay signal through UploadIfAbsent returning a
// wrapped ErrObjectAlreadyExists (the shape real S3 adapters produce on
// 412 Precondition Failed responses) and still asserts the audit contract:
// the returned SHA/Size describe the PERSISTED bytes, not the caller's
// input bytes.
func TestStore_ReplayPath_RehashesPersistedBytes(t *testing.T) {
	t.Parallel()

	persisted := []byte(`{"rows":[{"id":"r1","amount":"1.00"}]}`)
	expectedSHA := sha256.Sum256(persisted)

	// Wrap the sentinel the same way a real adapter would (fmt.Errorf with
	// %w), so errors.Is keeps working through the wrapper. This matches
	// internal/reporting/adapters/storage/s3_client.go:UploadIfAbsent.
	collisionErr := fmt.Errorf("%w: s3 412 precondition failed", sharedPorts.ErrObjectAlreadyExists)

	storage := &fakeObjectStorage{
		uploadIfAbsentErr: collisionErr,
		downloadBody:      persisted,
	}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	extractionID := uuid.New()
	tenantID := "tenant-replay"

	// Hand Store DIFFERENT bytes from what's persisted to prove the upload
	// path was NOT taken on the replay branch (write-once preserves the
	// original) AND the SHA/Size in the returned reference describes the
	// PERSISTED bytes, not the input bytes.
	differentInput := []byte(`{"rows":[{"id":"r2","amount":"99.99"}]}`)

	ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(differentInput),
	})
	require.NoError(t, err)
	require.NotNil(t, ref)

	expectedKey := tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json"

	assert.Equal(t, 0, storage.existsCalls, "Exists must NOT be probed — server-side condition replaces it")
	assert.Equal(t, 1, storage.uploadIfAbsentCalls, "UploadIfAbsent runs exactly once (condition rejects the write)")
	assert.Equal(t, 0, storage.uploadCalls, "non-conditional Upload MUST NOT run on replay (write-once)")
	assert.Equal(t, 1, storage.downloadCalls, "Download MUST run for replay digest recovery")
	assert.Equal(t, expectedKey, storage.downloadKey)

	assert.Equal(t, expectedKey, ref.Key)
	assert.Equal(t, URIScheme+"://"+expectedKey, ref.URI)
	assert.Equal(t, int64(len(persisted)), ref.Size,
		"replay Size must describe persisted bytes, not input bytes")
	assert.Equal(t, hex.EncodeToString(expectedSHA[:]), ref.SHA256,
		"replay SHA256 must describe persisted bytes, not input bytes")
}

// TestStore_ReplayPath_DownloadFailureWrapsSentinel exercises the error
// branch in recoverDigest: if Download fails on replay, Store wraps the
// error with ErrCustodyStoreFailed instead of silently returning an empty
// digest. Treating the recovery failure as a hard error is correct because
// the audit contract depends on the digest; an empty SHA would cascade
// silently into source_metadata.
func TestStore_ReplayPath_DownloadFailureWrapsSentinel(t *testing.T) {
	t.Parallel()

	collisionErr := fmt.Errorf("%w: s3 412 precondition failed", sharedPorts.ErrObjectAlreadyExists)

	storage := &fakeObjectStorage{
		uploadIfAbsentErr: collisionErr,
		downloadErr:       errors.New("s3 transient"),
	}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: uuid.New(),
		TenantID:     "tenant-replay",
		Content:      bytes.NewReader([]byte(`{"x":1}`)),
	})
	require.Nil(t, ref)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	require.ErrorContains(t, err, "replay recovery")
	require.ErrorContains(t, err, "s3 transient")
}

// zeroReader is an io.ReadCloser that produces zero bytes up to a fixed
// total, then returns io.EOF. Used to simulate a storage backend handing
// back more bytes than the ingest cap would have allowed — exercising the
// custody store's replay-path LimitReader defence without allocating the
// full payload in memory.
type zeroReader struct {
	remaining int64
}

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}

	n := int64(len(p))
	if n > z.remaining {
		n = z.remaining
	}
	// p is already zero-valued from the caller's Make; no need to clear.
	z.remaining -= n

	return int(n), nil
}

func (z *zeroReader) Close() error { return nil }

// TestStore_ReplayPath_OversizeDownloadIsCapped exercises the
// defence-in-depth cap on recoverDigest: if a storage backend misreports
// Exists (or a future caller writes bytes directly), the replay path must
// not materialise more than sharedPorts.MaxArtifactBytes. Today the ingest
// verifier rejects oversize ciphertext before custody writes, so this is a
// backstop rather than a live hazard — but a silent removal of the cap
// would reopen a DoS window, so we lock the behaviour with a test.
func TestStore_ReplayPath_OversizeDownloadIsCapped(t *testing.T) {
	t.Parallel()

	// Produce one byte beyond the cap so io.LimitReader hands recoverDigest
	// exactly MaxArtifactBytes+1 bytes, which then trips the counter check.
	oversize := &zeroReader{remaining: int64(sharedPorts.MaxArtifactBytes) + 1}

	collisionErr := fmt.Errorf("%w: s3 412 precondition failed", sharedPorts.ErrObjectAlreadyExists)

	storage := &fakeObjectStorage{
		uploadIfAbsentErr: collisionErr,
		downloadReader:    oversize,
	}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: uuid.New(),
		TenantID:     "tenant-oversize",
		Content:      bytes.NewReader([]byte(`{"x":1}`)),
	})
	require.Nil(t, ref)
	require.ErrorIs(t, err, sharedPorts.ErrCustodyStoreFailed)
	require.ErrorContains(t, err, "replay recovery")
	require.ErrorContains(t, err, "byte cap")
}

// TestStore_ConditionalUpload_FirstCallPersists is the C7 happy-path
// shape test: the first Store call for a (tenant, extraction) pair runs
// exactly one UploadIfAbsent against the storage layer and returns a
// fully-populated reference. No Exists probe, no Download — a single
// round-trip. This is the invariant the C7 fix is meant to preserve:
// closing the TOCTOU window at the storage layer must not add cost on
// the hot path.
func TestStore_ConditionalUpload_FirstCallPersists(t *testing.T) {
	t.Parallel()

	plaintext := []byte(`{"rows":[{"id":"ok-001","amount":"10.00"}]}`)
	storage := &fakeObjectStorage{}

	store, err := NewArtifactCustodyStore(storage)
	require.NoError(t, err)

	extractionID := uuid.New()
	tenantID := "tenant-first"

	ref, err := store.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(plaintext),
	})
	require.NoError(t, err)
	require.NotNil(t, ref)

	expectedKey := tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json"
	assert.Equal(t, expectedKey, ref.Key)
	assert.Equal(t, URIScheme+"://"+expectedKey, ref.URI)
	assert.Equal(t, int64(len(plaintext)), ref.Size)

	expectedSHA := sha256.Sum256(plaintext)
	assert.Equal(t, hex.EncodeToString(expectedSHA[:]), ref.SHA256)

	// Exactly one storage round-trip on the happy path. The C7 fix must
	// NOT regress into Exists + Upload, so we pin both counts here.
	assert.Equal(t, 1, storage.uploadIfAbsentCalls,
		"happy path must call UploadIfAbsent exactly once")
	assert.Equal(t, 0, storage.existsCalls,
		"happy path must NOT probe Exists — the condition is server-side")
	assert.Equal(t, 0, storage.downloadCalls,
		"happy path must NOT Download — digest comes from the streaming TeeReader")
	assert.Equal(t, 0, storage.uploadCalls,
		"happy path must NOT use the non-conditional Upload")
}

// TestStore_ConditionalUpload_ConcurrentWinner_ReplaysSuccessfully
// models the race the C7 fix closes. The scenario sequenced:
//
//  1. Two writers, A and B, call Store for the same (tenant, extraction).
//  2. Writer A's UploadIfAbsent lands first and writes the bytes.
//  3. Writer B's UploadIfAbsent arrives after A's PUT has completed; the
//     server responds 412 Precondition Failed. The adapter returns
//     ErrObjectAlreadyExists wrapped.
//  4. Writer B's Store MUST treat this as a successful replay — it
//     short-circuits into recoverDigest, reads the persisted bytes
//     (Writer A's content), and returns a reference populated from THOSE
//     bytes, not from its own input.
//
// The test simulates both arms sequentially on a shared fake backend so
// it stays deterministic. The fake mirrors the server-side condition:
// the first UploadIfAbsent succeeds and "persists" its bytes; the second
// is configured to return ErrObjectAlreadyExists and expose the
// already-persisted bytes through Download.
func TestStore_ConditionalUpload_ConcurrentWinner_ReplaysSuccessfully(t *testing.T) {
	t.Parallel()

	writerAPayload := []byte(`{"winner":true,"rows":[{"id":"A"}]}`)
	writerBPayload := []byte(`{"winner":false,"rows":[{"id":"B"}]}`)
	require.NotEqual(t, writerAPayload, writerBPayload,
		"test setup: the two payloads must differ so we can prove which one won")

	extractionID := uuid.New()
	tenantID := "tenant-race"
	expectedKey := tenantID + "/fetcher-artifacts/" + extractionID.String() + ".json"

	// --- Writer A: happy path. Single fake, no collision, no download.
	storageA := &fakeObjectStorage{}
	storeA, err := NewArtifactCustodyStore(storageA)
	require.NoError(t, err)

	refA, err := storeA.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(writerAPayload),
	})
	require.NoError(t, err, "writer A's conditional upload must succeed")
	require.NotNil(t, refA)
	assert.Equal(t, expectedKey, refA.Key)
	assert.Equal(t, 1, storageA.uploadIfAbsentCalls,
		"writer A reaches UploadIfAbsent exactly once")

	winnerSHA := refA.SHA256
	winnerSize := refA.Size

	// --- Writer B: racer. Its fake is configured so UploadIfAbsent returns
	// ErrObjectAlreadyExists (server-side 412 Precondition Failed) and
	// Download hands back writer A's bytes — the scenario writer B would
	// face in production after A won the race.
	collisionErr := fmt.Errorf("%w: s3 412 precondition failed", sharedPorts.ErrObjectAlreadyExists)

	storageB := &fakeObjectStorage{
		uploadIfAbsentErr: collisionErr,
		// Download returns what A persisted — B must read it verbatim for
		// the audit digest to describe the winning bytes, not B's input.
		downloadBody: writerAPayload,
	}

	storeB, err := NewArtifactCustodyStore(storageB)
	require.NoError(t, err)

	refB, err := storeB.Store(context.Background(), sharedPorts.ArtifactCustodyWriteInput{
		ExtractionID: extractionID,
		TenantID:     tenantID,
		Content:      bytes.NewReader(writerBPayload),
	})
	require.NoError(t, err,
		"writer B's collision MUST resolve as a successful replay, not an error")
	require.NotNil(t, refB)

	// The replay path MUST point at the same key and carry winner A's
	// digest/size, not B's input bytes. This is the whole point of the
	// write-once guarantee: concurrent writers converge on a single
	// persisted object.
	assert.Equal(t, expectedKey, refB.Key,
		"replay reference must point at the persisted key")
	assert.Equal(t, URIScheme+"://"+expectedKey, refB.URI)
	assert.Equal(t, winnerSize, refB.Size,
		"replay Size must match winner A's bytes, not B's input")
	assert.Equal(t, winnerSHA, refB.SHA256,
		"replay SHA must match winner A's bytes, not B's input")

	// Storage access pattern on the replay arm: one conditional upload
	// (rejected server-side), one download for digest recovery, no
	// non-conditional Upload, no Exists probe.
	assert.Equal(t, 1, storageB.uploadIfAbsentCalls,
		"writer B calls UploadIfAbsent exactly once (rejected)")
	assert.Equal(t, 1, storageB.downloadCalls,
		"writer B downloads the persisted bytes to recover the audit digest")
	assert.Equal(t, 0, storageB.existsCalls,
		"writer B MUST NOT probe Exists — the TOCTOU fix removed that call")
	assert.Equal(t, 0, storageB.uploadCalls,
		"writer B MUST NOT fall back to non-conditional Upload")
}
