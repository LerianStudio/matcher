// Package storageopt defines functional options for object storage operations.
// It exists as a separate package to break import cycles between ports and ports/mocks.
package storageopt

// UploadOption configures optional upload parameters.
type UploadOption func(*UploadOptions)

// UploadOptions holds optional parameters for upload operations.
type UploadOptions struct {
	StorageClass         string
	ServerSideEncryption string
}

// WithStorageClass sets the storage class for the upload (e.g., "GLACIER", "DEEP_ARCHIVE").
func WithStorageClass(class string) UploadOption {
	return func(o *UploadOptions) { o.StorageClass = class }
}

// WithServerSideEncryption sets server-side encryption (e.g., "aws:kms", "AES256").
func WithServerSideEncryption(sse string) UploadOption {
	return func(o *UploadOptions) { o.ServerSideEncryption = sse }
}
