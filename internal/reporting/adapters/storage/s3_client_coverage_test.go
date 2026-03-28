//go:build unit

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/tenant-manager/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestS3Config returns the common S3Config used across tests.
func newTestS3Config() S3Config {
	return S3Config{
		Endpoint:     "http://localhost:8333",
		Region:       "us-east-1",
		Bucket:       "test-bucket",
		UsePathStyle: true,
	}
}

// --- GeneratePresignedURL Tests ---

func TestGeneratePresignedURLCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	url, err := client.GeneratePresignedURL(context.Background(), "", 1*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, url)
}

func TestGeneratePresignedURLCov_WithValidKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()
	cfg.AccessKeyID = "test"
	cfg.SecretAccessKey = "test"

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	// PresignGetObject doesn't actually contact the server, it builds a URL.
	url, err := client.GeneratePresignedURL(context.Background(), "exports/test-file.csv", 1*time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, url)
	assert.Contains(t, url, "test-file.csv")
}

func TestGeneratePresignedURLCov_ShortExpiry(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()
	cfg.AccessKeyID = "test"
	cfg.SecretAccessKey = "test"

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	url, err := client.GeneratePresignedURL(context.Background(), "key/file.json", 5*time.Minute)
	require.NoError(t, err)
	assert.NotEmpty(t, url)
}

// --- Upload Empty Key ---

func TestUploadCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	key, err := client.Upload(context.Background(), "", nil, "text/csv")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, key)
}

// --- UploadWithOptions Empty Key ---

func TestUploadWithOptionsCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	key, err := client.UploadWithOptions(context.Background(), "", nil, "text/csv")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, key)
}

// --- Download Empty Key ---

func TestDownloadCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	reader, err := client.Download(context.Background(), "")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Nil(t, reader)
}

// --- Delete Empty Key ---

func TestDeleteCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	err = client.Delete(context.Background(), "")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
}

// --- Exists Empty Key ---

func TestExistsCov_EmptyKey(t *testing.T) {
	t.Parallel()

	cfg := newTestS3Config()

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	exists, err := client.Exists(context.Background(), "")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.False(t, exists)
}

// --- DefaultSeaweedConfig Tests ---

func TestDefaultSeaweedConfigCov(t *testing.T) {
	t.Parallel()

	cfg := DefaultSeaweedConfig("my-bucket")
	assert.Equal(t, "http://localhost:8333", cfg.Endpoint)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "my-bucket", cfg.Bucket)
	assert.True(t, cfg.UsePathStyle)
	assert.True(t, cfg.DisableSSL)
}

// --- NewS3Client with credentials ---

func TestNewS3ClientCov_WithCredentials(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint:        "http://localhost:9000",
		Region:          "us-west-2",
		Bucket:          "test-bucket",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		UsePathStyle:    true,
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

// --- NewS3Client without endpoint ---

func TestNewS3ClientCov_WithoutEndpoint(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Region: "us-east-1",
		Bucket: "test-bucket",
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

// --- NewS3Client without region ---

func TestNewS3ClientCov_WithoutRegion(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint:     "http://localhost:8333",
		Bucket:       "test-bucket",
		UsePathStyle: true,
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

// ============================================================================
// Multi-Tenant S3 Key Prefix Tests
// ============================================================================

// TestGetTenantPrefixedKey_AddsTenantPrefix verifies that getTenantPrefixedKey
// adds the tenant ID prefix when tenant context is present.
func TestGetTenantPrefixedKey_AddsTenantPrefix(t *testing.T) {
	t.Parallel()

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	key := "exports/report.csv"

	// Use canonical lib-commons v4 context setter for tenant ID
	ctx := core.ContextWithTenantID(context.Background(), tenantID)

	prefixedKey, err := getTenantPrefixedKey(ctx, key)
	require.NoError(t, err)

	// Key should be {tenantID}/{key}
	assert.Equal(t, tenantID+"/"+key, prefixedKey)
}

// TestGetTenantPrefixedKey_EmptyTenantNoPrefix verifies that getTenantPrefixedKey
// returns the key unchanged when an empty tenant ID is set in context.
// core.GetTenantIDFromContext returns the empty string, so no prefix is added.
func TestGetTenantPrefixedKey_EmptyTenantNoPrefix(t *testing.T) {
	t.Parallel()

	key := "exports/report.csv"

	// Set empty tenant ID -- core.GetTenantIDFromContext returns empty,
	// so tms3.GetS3KeyStorageContext returns key unchanged.
	ctx := core.ContextWithTenantID(context.Background(), "")

	prefixedKey, err := getTenantPrefixedKey(ctx, key)
	require.NoError(t, err)

	// Key should have no tenant prefix
	assert.Equal(t, key, prefixedKey)
}

// TestGetTenantPrefixedKey_StripsLeadingSlashes verifies that leading slashes
// are stripped from the key before prefixing.
func TestGetTenantPrefixedKey_StripsLeadingSlashes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         string
		tenantID    string
		expectedKey string
	}{
		{
			name:        "key_with_leading_slash_and_tenant",
			key:         "/exports/report.csv",
			tenantID:    "550e8400-e29b-41d4-a716-446655440000",
			expectedKey: "550e8400-e29b-41d4-a716-446655440000/exports/report.csv",
		},
		{
			name:        "key_with_multiple_leading_slashes_and_tenant",
			key:         "///exports/report.csv",
			tenantID:    "550e8400-e29b-41d4-a716-446655440000",
			expectedKey: "550e8400-e29b-41d4-a716-446655440000/exports/report.csv",
		},
		{
			name:        "key_without_leading_slash_and_tenant",
			key:         "exports/report.csv",
			tenantID:    "550e8400-e29b-41d4-a716-446655440000",
			expectedKey: "550e8400-e29b-41d4-a716-446655440000/exports/report.csv",
		},
		{
			name:        "key_with_leading_slash_empty_tenant_no_prefix",
			key:         "/exports/report.csv",
			tenantID:    "",
			expectedKey: "exports/report.csv", // Empty tenant: no prefix, just strip leading slashes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use canonical lib-commons v4 context setter for tenant ID
			ctx := core.ContextWithTenantID(context.Background(), tt.tenantID)

			result, err := getTenantPrefixedKey(ctx, tt.key)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedKey, result)
		})
	}
}

// TestGetTenantPrefixedKey_DifferentTenants_ProduceDifferentPaths verifies that
// different tenants produce different S3 paths for the same key.
func TestGetTenantPrefixedKey_DifferentTenants_ProduceDifferentPaths(t *testing.T) {
	t.Parallel()

	key := "exports/daily-report.csv"
	tenant1 := "550e8400-e29b-41d4-a716-446655440001"
	tenant2 := "550e8400-e29b-41d4-a716-446655440002"

	// Use canonical lib-commons v4 context setter for tenant ID
	ctx1 := core.ContextWithTenantID(context.Background(), tenant1)
	ctx2 := core.ContextWithTenantID(context.Background(), tenant2)

	path1, err := getTenantPrefixedKey(ctx1, key)
	require.NoError(t, err)

	path2, err := getTenantPrefixedKey(ctx2, key)
	require.NoError(t, err)

	// Paths should be different for different tenants
	assert.NotEqual(t, path1, path2)

	// Both should contain their respective tenant IDs
	assert.Contains(t, path1, tenant1)
	assert.Contains(t, path2, tenant2)

	// Both should contain the original key
	assert.Contains(t, path1, key)
	assert.Contains(t, path2, key)
}

// TestGetTenantPrefixedKey_NoTenantInContext verifies that when no tenant ID is set
// in context, the key is returned unchanged (no prefix added).
func TestGetTenantPrefixedKey_NoTenantInContext(t *testing.T) {
	t.Parallel()

	key := "exports/report.csv"

	// Background context has no tenant -- core.GetTenantIDFromContext returns empty
	ctx := context.Background()

	prefixedKey, err := getTenantPrefixedKey(ctx, key)
	require.NoError(t, err)

	// Key should be unchanged when no tenant is in context
	assert.Equal(t, key, prefixedKey)
}

// TestGetTenantPrefixedKey_ExplicitTenantPrefix verifies that when a tenant ID
// is explicitly set in context, the key gets prefixed.
func TestGetTenantPrefixedKey_ExplicitTenantPrefix(t *testing.T) {
	t.Parallel()

	key := "exports/report.csv"
	tenantID := "550e8400-e29b-41d4-a716-446655440000"

	ctx := core.ContextWithTenantID(context.Background(), tenantID)

	prefixedKey, err := getTenantPrefixedKey(ctx, key)
	require.NoError(t, err)

	// Key should be prefixed with the tenant ID
	assert.Equal(t, tenantID+"/"+key, prefixedKey)
	assert.Contains(t, prefixedKey, key)
	assert.NotEqual(t, key, prefixedKey)
}
