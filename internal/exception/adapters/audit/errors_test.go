//go:build unit

package audit_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/adapters/audit"
)

var errTestOther = errors.New("some other error")

func TestErrNilAuditLogRepository(t *testing.T) {
	t.Parallel()

	t.Run("error is not nil", func(t *testing.T) {
		t.Parallel()

		require.Error(t, audit.ErrNilAuditLogRepository)
	})

	t.Run("error message is correct", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "audit log repository is required", audit.ErrNilAuditLogRepository.Error())
	})

	t.Run("error can be wrapped and unwrapped", func(t *testing.T) {
		t.Parallel()

		wrapped := fmt.Errorf("initialization failed: %w", audit.ErrNilAuditLogRepository)
		require.ErrorIs(t, wrapped, audit.ErrNilAuditLogRepository)
		require.Contains(t, wrapped.Error(), "audit log repository is required")
	})

	t.Run("error is distinct from other errors", func(t *testing.T) {
		t.Parallel()

		require.NotErrorIs(t, errTestOther, audit.ErrNilAuditLogRepository)
	})
}
