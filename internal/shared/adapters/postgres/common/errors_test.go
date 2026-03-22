//go:build unit

package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{name: "ErrTransactionRequired", err: ErrTransactionRequired, message: "transaction is required"},
		{name: "ErrConnectionRequired", err: ErrConnectionRequired, message: "postgres connection is required"},
		{name: "ErrNoPrimaryDB", err: ErrNoPrimaryDB, message: "no primary database configured for tenant transaction"},
		{name: "ErrNilCallback", err: ErrNilCallback, message: "pgcommon: callback function must not be nil"},
		{name: "ErrInvalidTenantID", err: ErrInvalidTenantID, message: "invalid tenant ID format"},
		{name: "ErrInvalidIdentifier", err: ErrInvalidIdentifier, message: "invalid SQL identifier"},
		{name: "ErrSortCursorCalculatorRequired", err: ErrSortCursorCalculatorRequired, message: "sort cursor calculator is required"},
		{name: "ErrSortCursorBoundaryRecordNil", err: ErrSortCursorBoundaryRecordNil, message: "sort cursor boundary record is nil"},
		{name: "ErrCursorRecordExtractorRequired", err: ErrCursorRecordExtractorRequired, message: "cursor record extractor is required"},
		{name: "ErrCursorEncoderRequired", err: ErrCursorEncoderRequired, message: "cursor encoder is required"},
		{name: "ErrTransactionIDsEmpty", err: ErrTransactionIDsEmpty, message: "transaction IDs must not be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
			require.True(t, errors.Is(tt.err, tt.err))
			require.True(t, errors.Is(errors.Join(errors.New("outer"), tt.err), tt.err))
		})
	}
}
