# Test Utilities

The `internal/testutil` package provides shared test helpers used across unit and integration tests.

## Helpers

| File | Description |
|------|-------------|
| `ptr.go` | Generic pointer helper (`Ptr[T](v T) *T`) for creating pointers to literals in tests |
| `time.go` | Time utilities for deterministic test timestamps |

## Usage

```go
import "github.com/LerianStudio/matcher/internal/testutil"

name := testutil.Ptr("test-context")
ts := testutil.FixedTime()
```
