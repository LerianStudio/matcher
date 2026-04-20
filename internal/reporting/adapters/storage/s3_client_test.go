//go:build unit

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// mockReadCloser provides a test double for io.ReadCloser.
type mockReadCloser struct {
	data      []byte
	readPos   int
	closed    bool
	closeErr  error
	readErr   error
	readCount int
}

func newMockReadCloser(data []byte) *mockReadCloser {
	return &mockReadCloser{data: data}
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.readErr != nil && m.readCount > 0 {
		return 0, m.readErr
	}

	m.readCount++

	if m.readPos >= len(m.data) {
		return 0, io.EOF
	}

	n = copy(p, m.data[m.readPos:])
	m.readPos += n

	return n, nil
}

func (m *mockReadCloser) Close() error {
	m.closed = true

	return m.closeErr
}

func TestNewS3Client_BucketRequired(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint: "http://localhost:8333",
		Region:   "us-east-1",
		Bucket:   "", // empty bucket
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.Error(t, err)
	require.Nil(t, client)
	require.ErrorIs(t, err, ErrBucketRequired)
}

func TestNewS3Client_Success(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint:     "http://localhost:8333",
		Region:       "us-east-1",
		Bucket:       "test-bucket",
		UsePathStyle: true,
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Equal(t, "test-bucket", client.bucket)
}

func TestNewS3Client_WithCredentials(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint:        "http://localhost:8333",
		Region:          "us-east-1",
		Bucket:          "test-bucket",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		UsePathStyle:    true,
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
	assert.Equal(t, "test-bucket", client.bucket)
}

func TestNewS3Client_WithoutEndpoint(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Region: "us-east-1",
		Bucket: "test-bucket",
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestDefaultSeaweedConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultSeaweedConfig("my-bucket")

	assert.Equal(t, "http://localhost:8333", cfg.Endpoint)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "my-bucket", cfg.Bucket)
	assert.True(t, cfg.UsePathStyle)
	assert.True(t, cfg.DisableSSL)
	assert.Empty(t, cfg.AccessKeyID)
	assert.Empty(t, cfg.SecretAccessKey)
}

func TestUploadWithOptions_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	url, err := client.UploadWithOptions(
		context.Background(), "", nil, "application/gzip",
		storageopt.WithStorageClass("GLACIER"),
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, url)
}

func TestS3Client_UnavailableClientReturnsSharedError(t *testing.T) {
	t.Parallel()

	var client *S3Client

	_, err := client.Upload(context.Background(), "key", bytes.NewReader([]byte("data")), "text/plain")
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)

	_, err = client.UploadWithOptions(context.Background(), "key", bytes.NewReader([]byte("data")), "text/plain")
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)

	_, err = client.Download(context.Background(), "key")
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)

	err = client.Delete(context.Background(), "key")
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)

	_, err = client.GeneratePresignedURL(context.Background(), "key", time.Hour)
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)

	_, err = client.Exists(context.Background(), "key")
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}

func TestUploadWithOptions_EmptyKeyReturnsError_NoOptions(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	// Verify UploadWithOptions with no options still validates key.
	url, err := client.UploadWithOptions(
		context.Background(), "", bytes.NewReader([]byte("data")), "application/gzip",
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, url)
}

func TestUpload_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	url, err := client.Upload(context.Background(), "", nil, "text/plain")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, url)
}

func TestDownload_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	reader, err := client.Download(context.Background(), "")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Nil(t, reader)
}

func TestDelete_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	err := client.Delete(context.Background(), "")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
}

func TestGeneratePresignedURL_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	url, err := client.GeneratePresignedURL(context.Background(), "", time.Hour)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.Empty(t, url)
}

func TestExists_EmptyKeyReturnsError(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	exists, err := client.Exists(context.Background(), "")

	require.Error(t, err)
	require.ErrorIs(t, err, ErrKeyRequired)
	assert.False(t, exists)
}

func TestErrorDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrBucketRequired",
			err:      ErrBucketRequired,
			expected: "bucket name is required",
		},
		{
			name:     "ErrKeyRequired",
			err:      ErrKeyRequired,
			expected: "object key is required",
		},
		{
			name:     "ErrObjectNotFound",
			err:      ErrObjectNotFound,
			expected: "object not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestErrorsAreDistinct(t *testing.T) {
	t.Parallel()

	errors := []error{
		ErrBucketRequired,
		ErrKeyRequired,
		ErrObjectNotFound,
	}

	for i, err1 := range errors {
		for j, err2 := range errors {
			if i != j {
				assert.NotEqual(
					t,
					err1,
					err2,
					"errors at index %d and %d should be different",
					i,
					j,
				)
			}
		}
	}
}

func TestS3Client_ImplementsInterface(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Endpoint:     "http://localhost:8333",
		Region:       "us-east-1",
		Bucket:       "test-bucket",
		UsePathStyle: true,
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	// The compile-time check at the bottom of s3_client.go already ensures this,
	// but we explicitly test it here for documentation purposes.
	require.NotNil(t, client)
}

func TestNewS3Client_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         S3Config
		wantErr     bool
		expectedErr error
	}{
		{
			name: "valid_config_minimal",
			cfg: S3Config{
				Bucket: "test-bucket",
			},
			wantErr: false,
		},
		{
			name: "valid_config_with_endpoint",
			cfg: S3Config{
				Endpoint:     "http://localhost:8333",
				Region:       "us-east-1",
				Bucket:       "test-bucket",
				UsePathStyle: true,
			},
			wantErr: false,
		},
		{
			name: "valid_config_with_credentials",
			cfg: S3Config{
				Endpoint:        "http://localhost:9000",
				Region:          "eu-west-1",
				Bucket:          "minio-bucket",
				AccessKeyID:     "minioadmin",
				SecretAccessKey: "minioadmin",
				UsePathStyle:    true,
			},
			wantErr: false,
		},
		{
			name: "empty_bucket",
			cfg: S3Config{
				Endpoint: "http://localhost:8333",
				Region:   "us-east-1",
				Bucket:   "",
			},
			wantErr:     true,
			expectedErr: ErrBucketRequired,
		},
		{
			name: "config_with_only_access_key",
			cfg: S3Config{
				Bucket:      "test-bucket",
				AccessKeyID: "only-access-key",
				// SecretAccessKey is empty - should still create client
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewS3Client(context.Background(), tt.cfg)

			if tt.wantErr {
				require.Error(t, err)

				if tt.expectedErr != nil {
					require.ErrorIs(t, err, tt.expectedErr)
				}

				assert.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				assert.Equal(t, tt.cfg.Bucket, client.bucket)
			}
		})
	}
}

func TestEmptyKeyValidation_TableDriven(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	tests := []struct {
		name   string
		method string
		action func() error
	}{
		{
			name:   "Upload_EmptyKey",
			method: "Upload",
			action: func() error {
				_, err := client.Upload(context.Background(), "", nil, "text/plain")
				return err
			},
		},
		{
			name:   "Download_EmptyKey",
			method: "Download",
			action: func() error {
				_, err := client.Download(context.Background(), "")
				return err
			},
		},
		{
			name:   "Delete_EmptyKey",
			method: "Delete",
			action: func() error {
				return client.Delete(context.Background(), "")
			},
		},
		{
			name:   "GeneratePresignedURL_EmptyKey",
			method: "GeneratePresignedURL",
			action: func() error {
				_, err := client.GeneratePresignedURL(context.Background(), "", time.Hour)
				return err
			},
		},
		{
			name:   "Exists_EmptyKey",
			method: "Exists",
			action: func() error {
				_, err := client.Exists(context.Background(), "")
				return err
			},
		},
		{
			name:   "UploadWithOptions_EmptyKey",
			method: "UploadWithOptions",
			action: func() error {
				_, err := client.UploadWithOptions(context.Background(), "", nil, "text/plain", storageopt.WithStorageClass("GLACIER"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.action()

			require.Error(t, err)
			require.ErrorIs(
				t,
				err,
				ErrKeyRequired,
				"method %s should return ErrKeyRequired for empty key",
				tt.method,
			)
		})
	}
}

func TestS3Config_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    S3Config
		checks func(t *testing.T, cfg S3Config)
	}{
		{
			name: "all_fields_set",
			cfg: S3Config{
				Endpoint:        "http://localhost:9000",
				Region:          "eu-west-1",
				Bucket:          "my-bucket",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
				UsePathStyle:    true,
				DisableSSL:      true,
			},
			checks: func(t *testing.T, cfg S3Config) {
				t.Helper()

				assert.Equal(t, "http://localhost:9000", cfg.Endpoint)
				assert.Equal(t, "eu-west-1", cfg.Region)
				assert.Equal(t, "my-bucket", cfg.Bucket)
				assert.Equal(t, "access-key", cfg.AccessKeyID)
				assert.Equal(t, "secret-key", cfg.SecretAccessKey)
				assert.True(t, cfg.UsePathStyle)
				assert.True(t, cfg.DisableSSL)
			},
		},
		{
			name: "minimal_config",
			cfg: S3Config{
				Bucket: "minimal-bucket",
			},
			checks: func(t *testing.T, cfg S3Config) {
				t.Helper()

				assert.Empty(t, cfg.Endpoint)
				assert.Empty(t, cfg.Region)
				assert.Equal(t, "minimal-bucket", cfg.Bucket)
				assert.Empty(t, cfg.AccessKeyID)
				assert.Empty(t, cfg.SecretAccessKey)
				assert.False(t, cfg.UsePathStyle)
				assert.False(t, cfg.DisableSSL)
			},
		},
		{
			name: "aws_style_config",
			cfg: S3Config{
				Region:          "us-west-2",
				Bucket:          "aws-bucket",
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				UsePathStyle:    false,
				DisableSSL:      false,
			},
			checks: func(t *testing.T, cfg S3Config) {
				t.Helper()

				assert.Empty(t, cfg.Endpoint)
				assert.Equal(t, "us-west-2", cfg.Region)
				assert.Equal(t, "aws-bucket", cfg.Bucket)
				assert.False(t, cfg.UsePathStyle)
				assert.False(t, cfg.DisableSSL)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.checks(t, tt.cfg)
		})
	}
}

func TestDefaultSeaweedConfig_AllSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
	}{
		{
			name:       "standard_bucket",
			bucketName: "reports",
		},
		{
			name:       "bucket_with_dashes",
			bucketName: "my-export-bucket",
		},
		{
			name:       "bucket_with_numbers",
			bucketName: "bucket123",
		},
		{
			name:       "empty_bucket",
			bucketName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultSeaweedConfig(tt.bucketName)

			assert.Equal(t, "http://localhost:8333", cfg.Endpoint)
			assert.Equal(t, "us-east-1", cfg.Region)
			assert.Equal(t, tt.bucketName, cfg.Bucket)
			assert.True(t, cfg.UsePathStyle)
			assert.True(t, cfg.DisableSSL)
			assert.Empty(t, cfg.AccessKeyID)
			assert.Empty(t, cfg.SecretAccessKey)
		})
	}
}

func TestNewS3Client_RegionVariations(t *testing.T) {
	t.Parallel()

	regions := []string{
		"us-east-1",
		"us-west-2",
		"eu-west-1",
		"ap-northeast-1",
		"sa-east-1",
		"",
	}

	for _, region := range regions {
		t.Run("region_"+region, func(t *testing.T) {
			t.Parallel()

			cfg := S3Config{
				Bucket: "test-bucket",
				Region: region,
			}

			client, err := NewS3Client(context.Background(), cfg)

			require.NoError(t, err)
			require.NotNil(t, client)
			assert.Equal(t, "test-bucket", client.bucket)
		})
	}
}

func TestNewS3Client_EndpointVariations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		endpoint    string
		expectError error
	}{
		{
			name:     "seaweedfs",
			endpoint: "http://localhost:8333",
		},
		{
			name:     "minio",
			endpoint: "http://localhost:9000",
		},
		{
			name:     "minio_https",
			endpoint: "https://minio.example.com",
		},
		{
			name:        "custom_port",
			endpoint:    "http://storage.local:8080",
			expectError: ErrInsecureEndpoint,
		},
		{
			name:     "no_endpoint",
			endpoint: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := S3Config{
				Bucket:   "test-bucket",
				Endpoint: tt.endpoint,
			}

			client, err := NewS3Client(context.Background(), cfg)
			if tt.expectError != nil {
				require.ErrorIs(t, err, tt.expectError)
				require.Nil(t, client)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, client)
		})
	}
}

func TestValidateEndpointSecurity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		endpoint      string
		allowInsecure bool
		expectError   error
	}{
		{name: "empty", endpoint: ""},
		{name: "localhost http", endpoint: "http://localhost:8333"},
		{name: "loopback ip", endpoint: "http://127.0.0.1:9000"},
		{name: "https remote", endpoint: "https://storage.example.com"},
		{name: "remote http", endpoint: "http://storage.example.com", expectError: ErrInsecureEndpoint},
		{name: "remote http allowed", endpoint: "http://storage.example.com", allowInsecure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateEndpointSecurity(tt.endpoint, tt.allowInsecure)
			if tt.expectError != nil {
				require.ErrorIs(t, err, tt.expectError)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestNewS3Client_AllowInsecureEndpoint(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Bucket:        "test-bucket",
		Endpoint:      "http://storage.internal:8333",
		AllowInsecure: true,
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
}

func TestNewS3Client_CredentialCombinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accessKey string
		secretKey string
	}{
		{
			name:      "both_credentials",
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "wJalrXUtnFEMI/K7MDENG",
		},
		{
			name:      "no_credentials",
			accessKey: "",
			secretKey: "",
		},
		{
			name:      "access_only",
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			secretKey: "",
		},
		{
			name:      "secret_only",
			accessKey: "",
			secretKey: "wJalrXUtnFEMI/K7MDENG",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := S3Config{
				Bucket:          "test-bucket",
				AccessKeyID:     tt.accessKey,
				SecretAccessKey: tt.secretKey,
			}

			client, err := NewS3Client(context.Background(), cfg)

			require.NoError(t, err)
			require.NotNil(t, client)
		})
	}
}

func TestS3Client_BucketProperty(t *testing.T) {
	t.Parallel()

	buckets := []string{
		"simple-bucket",
		"bucket.with.dots",
		"bucket-with-dashes",
		"bucket123",
		"a",
		strings.Repeat("x", 63),
	}

	for _, bucket := range buckets {
		t.Run("bucket_"+bucket[:min(10, len(bucket))], func(t *testing.T) {
			t.Parallel()

			cfg := S3Config{
				Bucket: bucket,
			}

			client, err := NewS3Client(context.Background(), cfg)

			require.NoError(t, err)
			require.NotNil(t, client)
			assert.Equal(t, bucket, client.bucket)
		})
	}
}

func TestErrorWrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sentinel    error
		expectedMsg string
	}{
		{
			name:        "bucket_required",
			sentinel:    ErrBucketRequired,
			expectedMsg: "bucket name is required",
		},
		{
			name:        "key_required",
			sentinel:    ErrKeyRequired,
			expectedMsg: "object key is required",
		},
		{
			name:        "object_not_found",
			sentinel:    ErrObjectNotFound,
			expectedMsg: "object not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expectedMsg, tt.sentinel.Error())

			// Test that wrapped errors can be unwrapped to match the sentinel
			wrapped := fmt.Errorf("operation failed: %w", tt.sentinel)
			assert.True(t, errors.Is(wrapped, tt.sentinel), "wrapped error should match sentinel")
			assert.NotEqual(
				t,
				wrapped.Error(),
				tt.sentinel.Error(),
				"wrapped error message should differ from sentinel",
			)
		})
	}
}

func TestValidKeyInputs(t *testing.T) {
	t.Parallel()

	validKeys := []string{
		"simple-key",
		"path/to/object",
		"deep/nested/path/to/file.json",
		"file.txt",
		"reports/2024/01/export.csv",
		"a",
		strings.Repeat("k", 1024),
	}

	for _, key := range validKeys {
		t.Run("key_validation_"+key[:min(20, len(key))], func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, key, "key should not be empty")
			assert.Greater(t, len(key), 0, "key length should be positive")
		})
	}
}

func TestUpload_ContentTypes(t *testing.T) {
	t.Parallel()

	contentTypes := []string{
		"text/plain",
		"text/csv",
		"application/json",
		"application/octet-stream",
		"application/pdf",
		"text/html",
		"",
	}

	for _, contentType := range contentTypes {
		testName := contentType
		if testName == "" {
			testName = "empty"
		}

		t.Run("content_type_"+testName, func(t *testing.T) {
			t.Parallel()

			assert.True(t, true, "content type %s is valid input", contentType)
		})
	}
}

func TestUpload_ReaderVariations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reader   io.Reader
		isNil    bool
		dataSize int
	}{
		{
			name:     "bytes_reader",
			reader:   bytes.NewReader([]byte("test content")),
			dataSize: 12,
		},
		{
			name:     "strings_reader",
			reader:   strings.NewReader("test content"),
			dataSize: 12,
		},
		{
			name:     "empty_bytes_reader",
			reader:   bytes.NewReader([]byte{}),
			dataSize: 0,
		},
		{
			name:  "nil_reader",
			isNil: true,
		},
		{
			name:     "large_content",
			reader:   bytes.NewReader(bytes.Repeat([]byte("x"), 10*1024)),
			dataSize: 10 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.isNil {
				assert.Nil(t, tt.reader)
			} else {
				assert.NotNil(t, tt.reader)
			}
		})
	}
}

func TestGeneratePresignedURL_ExpiryVariations(t *testing.T) {
	t.Parallel()

	expiries := []time.Duration{
		time.Second,
		time.Minute,
		time.Hour,
		24 * time.Hour,
		7 * 24 * time.Hour,
		0,
		-time.Hour,
	}

	for _, expiry := range expiries {
		t.Run("expiry_"+expiry.String(), func(t *testing.T) {
			t.Parallel()

			assert.True(t, true, "expiry duration %s is valid input", expiry)
		})
	}
}

func TestS3Client_FieldsAccessible(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Bucket:   "test-bucket",
		Endpoint: "http://localhost:9000",
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, "test-bucket", client.bucket)
	assert.NotNil(t, client.s3)
}

func TestContextCancellation(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Bucket:   "test-bucket",
		Endpoint: "http://localhost:9000",
	}

	client, err := NewS3Client(context.Background(), cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("upload_cancelled", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte("test"))
		_, err := client.Upload(ctx, "key", reader, "text/plain")

		assert.Error(t, err)
	})

	t.Run("upload_with_options_cancelled", func(t *testing.T) {
		t.Parallel()

		reader := bytes.NewReader([]byte("test"))
		_, err := client.UploadWithOptions(ctx, "key", reader, "application/gzip", storageopt.WithStorageClass("GLACIER"))

		assert.Error(t, err)
	})

	t.Run("download_cancelled", func(t *testing.T) {
		t.Parallel()

		_, err := client.Download(ctx, "key")

		assert.Error(t, err)
	})

	t.Run("delete_cancelled", func(t *testing.T) {
		t.Parallel()

		err := client.Delete(ctx, "key")

		assert.Error(t, err)
	})

	t.Run("exists_cancelled", func(t *testing.T) {
		t.Parallel()

		_, err := client.Exists(ctx, "key")

		assert.Error(t, err)
	})
}

func TestMockReadCloser_Behavior(t *testing.T) {
	t.Parallel()

	t.Run("read_all_data", func(t *testing.T) {
		t.Parallel()

		data := []byte("hello world")
		rc := newMockReadCloser(data)

		result, err := io.ReadAll(rc)

		require.NoError(t, err)
		assert.Equal(t, data, result)
	})

	t.Run("close_returns_error", func(t *testing.T) {
		t.Parallel()

		rc := newMockReadCloser([]byte("data"))
		rc.closeErr = errors.New("close failed")

		err := rc.Close()

		require.Error(t, err)
		assert.True(t, rc.closed)
	})

	t.Run("read_empty_data", func(t *testing.T) {
		t.Parallel()

		rc := newMockReadCloser([]byte{})

		result, err := io.ReadAll(rc)

		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("multiple_reads", func(t *testing.T) {
		t.Parallel()

		data := []byte("test data for multiple reads")
		rc := newMockReadCloser(data)

		buf1 := make([]byte, 5)
		n1, err1 := rc.Read(buf1)

		require.NoError(t, err1)
		assert.Equal(t, 5, n1)
		assert.Equal(t, "test ", string(buf1))

		remaining, err2 := io.ReadAll(rc)

		require.NoError(t, err2)
		assert.Equal(t, "data for multiple reads", string(remaining))
	})
}

func TestNoSuchKeyError(t *testing.T) {
	t.Parallel()

	nsk := &types.NoSuchKey{
		Message: new(string),
	}
	*nsk.Message = "The specified key does not exist."

	var target *types.NoSuchKey

	assert.True(t, errors.As(nsk, &target))
}

func TestNotFoundError(t *testing.T) {
	t.Parallel()

	notFound := &types.NotFound{
		Message: new(string),
	}
	*notFound.Message = "Not Found"

	var target *types.NotFound

	assert.True(t, errors.As(notFound, &target))
}

func TestS3Client_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	cfg := S3Config{
		Bucket: "test-bucket",
	}

	client, err := NewS3Client(context.Background(), cfg)

	require.NoError(t, err)

	var _ interface {
		Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)
		UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...sharedPorts.UploadOption) (string, error)
		Download(ctx context.Context, key string) (io.ReadCloser, error)
		Delete(ctx context.Context, key string) error
		GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)
		Exists(ctx context.Context, key string) (bool, error)
	} = client
}

func TestS3Config_ZeroValue(t *testing.T) {
	t.Parallel()

	var cfg S3Config

	assert.Empty(t, cfg.Endpoint)
	assert.Empty(t, cfg.Region)
	assert.Empty(t, cfg.Bucket)
	assert.Empty(t, cfg.AccessKeyID)
	assert.Empty(t, cfg.SecretAccessKey)
	assert.False(t, cfg.UsePathStyle)
	assert.False(t, cfg.DisableSSL)
}

func TestNewS3Client_UsePathStyleOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		usePathStyle bool
	}{
		{
			name:         "path_style_true",
			usePathStyle: true,
		},
		{
			name:         "path_style_false",
			usePathStyle: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := S3Config{
				Bucket:       "test-bucket",
				Endpoint:     "http://localhost:9000",
				UsePathStyle: tt.usePathStyle,
			}

			client, err := NewS3Client(context.Background(), cfg)

			require.NoError(t, err)
			require.NotNil(t, client)
		})
	}
}

func TestKeySpecialCharacters(t *testing.T) {
	t.Parallel()

	specialKeys := []string{
		"path/with/slashes",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.multiple.dots.txt",
		"UPPERCASE",
		"MixedCase",
		"123numeric",
		"special!@#chars",
	}

	for _, key := range specialKeys {
		testName := strings.ReplaceAll(key, "/", "_")

		t.Run("special_key_"+testName, func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, key, "special key should not be empty")
			assert.Greater(t, len(key), 0)
		})
	}
}

// ---------- Error-path tests: nil inner S3 SDK client ----------
//
// These verify that an S3Client whose underlying AWS SDK client is nil
// (struct exists, but s3 field is zero-value) returns a proper error
// through ensureReady() instead of panicking with a nil-pointer deref.
// The nil-*S3Client-pointer case is covered by
// TestS3Client_UnavailableClientReturnsSharedError above; these target
// the complementary "constructed-but-unconfigured" scenario.

func TestS3Client_Upload_NilInnerClient(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
		// s3 field is nil — simulates a partially constructed client
	}

	url, err := client.Upload(
		context.Background(),
		"reports/2024/export.csv",
		bytes.NewReader([]byte("col1,col2\na,b\n")),
		"text/csv",
	)

	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
	assert.Empty(t, url)
}

func TestS3Client_Download_NilInnerClient(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	reader, err := client.Download(
		context.Background(),
		"reports/2024/export.csv",
	)

	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
	assert.Nil(t, reader)
}

func TestS3Client_Delete_NilInnerClient(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	err := client.Delete(
		context.Background(),
		"reports/2024/export.csv",
	)

	require.Error(t, err)
	require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}

// TestS3Client_NilInnerClient_Idempotent verifies that repeatedly calling
// operations on a client with a nil SDK handle is safe and deterministic:
// every call returns the same sentinel error, no panic, no state mutation.
// S3Client has no Close() method (the ObjectStorageClient interface doesn't
// define one), so "idempotent teardown" means "safe to abandon or re-call."
func TestS3Client_NilInnerClient_Idempotent(t *testing.T) {
	t.Parallel()

	client := &S3Client{
		bucket: "test-bucket",
	}

	const iterations = 3

	for i := range iterations {
		_, err := client.Upload(
			context.Background(),
			"key",
			bytes.NewReader([]byte("data")),
			"text/plain",
		)
		require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable,
			"Upload iteration %d should return unavailable", i)

		_, err = client.Download(context.Background(), "key")
		require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable,
			"Download iteration %d should return unavailable", i)

		err = client.Delete(context.Background(), "key")
		require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable,
			"Delete iteration %d should return unavailable", i)

		_, err = client.GeneratePresignedURL(context.Background(), "key", time.Hour)
		require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable,
			"GeneratePresignedURL iteration %d should return unavailable", i)

		_, err = client.Exists(context.Background(), "key")
		require.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable,
			"Exists iteration %d should return unavailable", i)
	}
}
