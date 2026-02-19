# Storage Options Package

The `pkg/storageopt` package defines functional options for object storage operations. It exists as a separate package to break import cycles between ports and port mocks.

## Options

| Option | Description |
|--------|-------------|
| `WithStorageClass(class)` | Sets the storage class (e.g., `"GLACIER"`, `"DEEP_ARCHIVE"`) |
| `WithServerSideEncryption(algo)` | Sets server-side encryption algorithm |

## Usage

```go
import "github.com/LerianStudio/matcher/pkg/storageopt"

err := storage.Upload(ctx, key, data,
    storageopt.WithStorageClass("GLACIER"),
    storageopt.WithServerSideEncryption("AES256"),
)
```
