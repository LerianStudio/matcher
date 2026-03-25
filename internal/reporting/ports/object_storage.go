package ports

import (
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// ObjectStorageClient is deprecated; use internal/shared/ports.ObjectStorageClient directly.
type ObjectStorageClient = sharedPorts.ObjectStorageClient

// UploadOption is deprecated; use pkg/storageopt.UploadOption or internal/shared/ports.UploadOption directly.
type UploadOption = storageopt.UploadOption

// UploadOptions is deprecated; use pkg/storageopt.UploadOptions or internal/shared/ports.UploadOptions directly.
type UploadOptions = storageopt.UploadOptions

// WithStorageClass is deprecated; use storageopt.WithStorageClass directly.
var WithStorageClass = storageopt.WithStorageClass

// WithServerSideEncryption is deprecated; use storageopt.WithServerSideEncryption directly.
var WithServerSideEncryption = storageopt.WithServerSideEncryption
