// Package ports defines interfaces for reporting infrastructure.
// The canonical ObjectStorageClient interface lives in the shared kernel (internal/shared/ports)
// and is re-exported here as a type alias for backward compatibility.
package ports

//go:generate mockgen -destination=mocks/object_storage_client_mock.go -package=mocks github.com/LerianStudio/matcher/internal/shared/ports ObjectStorageClient

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
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
// Re-exported from the shared kernel (internal/shared/ports.ObjectStorageClient).
//
// All bounded contexts that need this interface should use the shared kernel directly:
//
//	import sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
//
// This alias exists for backward compatibility with code that already imports
// this package. No new code should import reporting/ports from outside
// the reporting bounded context.
type ObjectStorageClient = sharedPorts.ObjectStorageClient
