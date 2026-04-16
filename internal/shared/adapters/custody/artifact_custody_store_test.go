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
// store only exercises Upload and Delete; the remaining methods return
// zero values so tests stay focused on the two under test. We tick the
// interface as small as possible because go.uber.org/mock is overkill for
// a 5-method port per CLAUDE.md guidance.
type fakeObjectStorage struct {
	uploadKey         string
	uploadContentType string
	uploadBody        []byte
	uploadErr         error
	deleteKey         string
	deleteErr         error
	uploadCalls       int
	deleteCalls       int
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

func (f *fakeObjectStorage) UploadWithOptions(
	ctx context.Context,
	key string,
	reader io.Reader,
	contentType string,
	_ ...storageopt.UploadOption,
) (string, error) {
	return f.Upload(ctx, key, reader, contentType)
}

func (f *fakeObjectStorage) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
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
	return false, nil
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
	storage := &fakeObjectStorage{uploadErr: backendErr}

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
