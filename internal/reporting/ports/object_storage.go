// Package ports defines interfaces for reporting infrastructure.
package ports

//go:generate mockgen -source=object_storage.go -destination=mocks/object_storage_client_mock.go -package=mocks

import (
	"context"
	"io"
	"time"

	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// UploadOption is a functional option for configuring upload parameters.
// Re-exported from pkg/storageopt to provide a convenient single-import experience.
type UploadOption = storageopt.UploadOption

// UploadOptions holds optional parameters for upload operations.
// Re-exported from pkg/storageopt to provide a convenient single-import experience.
type UploadOptions = storageopt.UploadOptions

// WithStorageClass sets the storage class for the upload (e.g., "GLACIER", "DEEP_ARCHIVE").
var WithStorageClass = storageopt.WithStorageClass

// WithServerSideEncryption sets server-side encryption (e.g., "aws:kms", "AES256").
var WithServerSideEncryption = storageopt.WithServerSideEncryption

// ObjectStorageClient provides object storage operations for export files.
type ObjectStorageClient interface {
	// Upload stores content from a reader at the given key.
	// Returns the final key and any error.
	Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)

	// UploadWithOptions stores content with configurable storage options.
	UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...storageopt.UploadOption) (string, error)

	// Download retrieves content from the given key.
	// The caller must close the returned ReadCloser.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes an object by key.
	Delete(ctx context.Context, key string) error

	// GeneratePresignedURL creates a time-limited download URL.
	GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)

	// Exists checks if an object exists at the given key.
	Exists(ctx context.Context, key string) (bool, error)
}
