// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"errors"
)

// UploadOption configures optional upload parameters for object storage
// uploads. The concrete hot-reloadable client lives in
// internal/shared/objectstorage — this file keeps only the shared
// value types and sentinel errors that flow across the module boundary.
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

// ErrObjectStorageUnavailable indicates runtime object storage is not currently available.
var ErrObjectStorageUnavailable = errors.New("object storage is unavailable")

// ErrObjectAlreadyExists indicates a conditional upload refused to overwrite
// an existing object at the target key. Callers using UploadIfAbsent treat
// this as the "replay" signal: the key is already held by a prior writer,
// so the caller should short-circuit to its idempotency path (e.g. recover
// the existing object's digest) instead of retrying.
//
// This sentinel closes a TOCTOU window that a separate Exists + Upload
// pair cannot. On S3, the conditional PUT (If-None-Match: *) is enforced
// server-side, so two concurrent writers can never both observe "absent"
// and both succeed — at most one PUT wins, the other gets 412 Precondition
// Failed, which the adapter maps onto this sentinel.
var ErrObjectAlreadyExists = errors.New("object already exists at key")
