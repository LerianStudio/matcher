//go:build unit

package postgres

import (
	"errors"
	"testing"

	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTestContext = errors.New("context")

func TestRepositoryErrors_NotNil(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrAuditLogRequired", ErrAuditLogRequired},
		{"ErrAuditLogNotFound", ErrAuditLogNotFound},
		{"ErrIDRequired", ErrIDRequired},
		{"ErrLimitMustBePositive", ErrLimitMustBePositive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestRepositoryErrors_Messages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrRepositoryNotInitialized has expected message",
			err:     ErrRepositoryNotInitialized,
			message: "repository not initialized",
		},
		{
			name:    "ErrAuditLogRequired has expected message",
			err:     ErrAuditLogRequired,
			message: "audit log is required",
		},
		{
			name:    "ErrAuditLogNotFound has expected message",
			err:     ErrAuditLogNotFound,
			message: "audit log not found",
		},
		{
			name:    "ErrIDRequired has expected message",
			err:     ErrIDRequired,
			message: "id is required",
		},
		{
			name:    "ErrLimitMustBePositive has expected message",
			err:     ErrLimitMustBePositive,
			message: "limit must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		ErrRepositoryNotInitialized,
		ErrAuditLogRequired,
		ErrAuditLogNotFound,
		ErrIDRequired,
		ErrLimitMustBePositive,
	}

	for i, err1 := range errs {
		for j, err2 := range errs {
			if i != j {
				assert.NotEqual(t, err1, err2, "errors at index %d and %d should be distinct", i, j)
			}
		}
	}
}

func TestErrors_CanBeWrapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrRepositoryNotInitialized", ErrRepositoryNotInitialized},
		{"ErrAuditLogRequired", ErrAuditLogRequired},
		{"ErrAuditLogNotFound", ErrAuditLogNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wrapped := errors.Join(errTestContext, tt.err)
			assert.ErrorIs(t, wrapped, tt.err)
		})
	}
}

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	require.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
