//go:build unit

package storage

import (
	"context"
	"testing"
	"time"

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
