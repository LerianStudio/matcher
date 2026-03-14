//go:build unit

package tenant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/auth"
)

func TestScopedRedisSegments(t *testing.T) {
	t.Parallel()

	t.Run("keeps single tenant key unchanged when default tenant is excluded", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
		assert.Equal(t, "matcher:dashboard:key", ScopedRedisSegments(ctx, false, "matcher", "dashboard", "key"))
	})

	t.Run("prefixes tenant for multi-tenant keys", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")
		assert.Equal(t, "matcher:tenant-a:dashboard:key", ScopedRedisSegments(ctx, false, "matcher", "dashboard", "key"))
	})

	t.Run("preserves legacy default-tenant scoped keys when requested", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(context.Background(), auth.TenantIDKey, auth.DefaultTenantID)
		assert.Equal(t, "matcher:"+auth.DefaultTenantID+":lock:key", ScopedRedisSegments(ctx, true, "matcher", "lock", "key"))
	})

	t.Run("missing tenant keeps passthrough unchanged", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "matcher:dashboard:key", ScopedRedisSegments(context.Background(), false, "matcher", "dashboard", "key"))
	})
}

func TestScopedObjectStorageKey(t *testing.T) {
	t.Parallel()

	t.Run("joins validated segments without normalization", func(t *testing.T) {
		t.Parallel()

		key, err := ScopedObjectStorageKey("exports", "tenant-a", "context-a", "file.csv")
		assert.NoError(t, err)
		assert.Equal(t, "exports/tenant-a/context-a/file.csv", key)
	})

	t.Run("supports nested logical prefixes", func(t *testing.T) {
		t.Parallel()

		key, err := ScopedObjectStorageKey("archives/audit-logs", "tenant-a", "2026", "03", "archive.gz")
		assert.NoError(t, err)
		assert.Equal(t, "archives/audit-logs/tenant-a/2026/03/archive.gz", key)
	})

	t.Run("rejects missing tenant ids", func(t *testing.T) {
		t.Parallel()

		_, err := ScopedObjectStorageKey("exports", "", "context-a", "file.csv")
		assert.ErrorIs(t, err, ErrTenantIDRequired)
	})

	t.Run("rejects path normalizing segments", func(t *testing.T) {
		t.Parallel()

		_, err := ScopedObjectStorageKey("exports", "tenant-a", "..", "file.csv")
		assert.ErrorIs(t, err, ErrInvalidObjectStoragePathSegment)
	})
}
