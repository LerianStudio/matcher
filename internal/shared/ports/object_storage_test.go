//go:build unit

package ports

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestObjectStorageClientInterfaceExists(t *testing.T) {
	t.Parallel()

	var _ ObjectStorageClient = (*mockObjectStorageClient)(nil)
}

type mockObjectStorageClient struct{}

func (m *mockObjectStorageClient) Upload(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) UploadIfAbsent(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) UploadWithOptions(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
	_ ...UploadOption,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockObjectStorageClient) Delete(_ context.Context, _ string) error {
	return nil
}

func (m *mockObjectStorageClient) GeneratePresignedURL(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func TestWithStorageClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		class    string
		expected string
	}{
		{name: "glacier", class: "GLACIER", expected: "GLACIER"},
		{name: "deep_archive", class: "DEEP_ARCHIVE", expected: "DEEP_ARCHIVE"},
		{name: "standard", class: "STANDARD", expected: "STANDARD"},
		{name: "standard_ia", class: "STANDARD_IA", expected: "STANDARD_IA"},
		{name: "empty", class: "", expected: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := &UploadOptions{}
			fn := WithStorageClass(tt.class)
			fn(opts)

			assert.Equal(t, tt.expected, opts.StorageClass)
		})
	}
}

func TestWithServerSideEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sse      string
		expected string
	}{
		{name: "kms", sse: "aws:kms", expected: "aws:kms"},
		{name: "aes256", sse: "AES256", expected: "AES256"},
		{name: "empty", sse: "", expected: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := &UploadOptions{}
			fn := WithServerSideEncryption(tt.sse)
			fn(opts)

			assert.Equal(t, tt.expected, opts.ServerSideEncryption)
		})
	}
}

func TestUploadOptions_MultipleOptions(t *testing.T) {
	t.Parallel()

	opts := &UploadOptions{}

	options := []UploadOption{
		WithStorageClass("GLACIER"),
		WithServerSideEncryption("aws:kms"),
	}

	for _, opt := range options {
		opt(opts)
	}

	assert.Equal(t, "GLACIER", opts.StorageClass)
	assert.Equal(t, "aws:kms", opts.ServerSideEncryption)
}

func TestUploadOptions_LastWriteWins(t *testing.T) {
	t.Parallel()

	opts := &UploadOptions{}

	options := []UploadOption{
		WithStorageClass("STANDARD"),
		WithStorageClass("GLACIER"),
	}

	for _, opt := range options {
		opt(opts)
	}

	assert.Equal(t, "GLACIER", opts.StorageClass)
}

func TestUploadOptions_ZeroValue(t *testing.T) {
	t.Parallel()

	var opts UploadOptions

	assert.Empty(t, opts.StorageClass)
	assert.Empty(t, opts.ServerSideEncryption)
}

func TestUploadOptions_NoOptions(t *testing.T) {
	t.Parallel()

	opts := &UploadOptions{}

	options := []UploadOption{}
	for _, opt := range options {
		opt(opts)
	}

	assert.Empty(t, opts.StorageClass)
	assert.Empty(t, opts.ServerSideEncryption)
}
