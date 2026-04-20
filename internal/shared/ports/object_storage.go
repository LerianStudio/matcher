// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"errors"
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

// ObjectStorageClient provides object storage operations for export files and archives.
// This is the shared kernel interface used by governance (archival) and reporting (export jobs)
// without creating a direct import dependency between those bounded contexts.
type ObjectStorageClient interface {
	// Upload stores content from a reader at the given key.
	// Returns the final key and any error.
	Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)

	// UploadIfAbsent stores content at the given key ONLY if no object
	// currently exists at that key. Returns ErrObjectAlreadyExists (wrapped
	// is acceptable; callers use errors.Is) when the key is already held.
	//
	// Implementations MUST close the check-then-act window at the storage
	// layer — typically by sending an If-None-Match: * conditional PUT (AWS
	// S3 supports this; other backends may not). A separate Exists + Upload
	// pair does NOT satisfy this contract because it is TOCTOU-vulnerable
	// across concurrent writers and across process replicas.
	//
	// Backends that cannot enforce conditional writes MUST document the
	// weaker guarantee in their implementation and SHOULD fall back to an
	// Exists-then-Upload under a per-key mutex so at least intra-process
	// races are serialised. In that mode, inter-process TOCTOU windows
	// still exist — defense-in-depth distributed locks at the caller remain
	// necessary.
	UploadIfAbsent(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)

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
