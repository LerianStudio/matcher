// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dispute

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
)

type mockScanner struct {
	values []any
	err    error
}

func (m *mockScanner) Scan(dest ...any) error {
	if m.err != nil {
		return m.err
	}

	for i, d := range dest {
		if i >= len(m.values) {
			break
		}

		switch ptr := d.(type) {
		case *uuid.UUID:
			switch v := m.values[i].(type) {
			case uuid.UUID:
				*ptr = v
			case string:
				parsed, err := uuid.Parse(v)
				if err != nil {
					return err
				}

				*ptr = parsed
			}
		case *string:
			if v, ok := m.values[i].(string); ok {
				*ptr = v
			}
		case *sql.NullString:
			if v, ok := m.values[i].(sql.NullString); ok {
				*ptr = v
			}
		case *[]byte:
			if v, ok := m.values[i].([]byte); ok {
				*ptr = v
			}
		case *time.Time:
			if v, ok := m.values[i].(time.Time); ok {
				*ptr = v
			}
		}
	}

	return nil
}

func validScannerValues() []any {
	now := time.Now().UTC()

	return []any{
		uuid.New().String(),
		uuid.New().String(),
		"BANK_FEE_ERROR",
		"DRAFT",
		"Test description",
		"user@example.com",
		sql.NullString{},
		sql.NullString{},
		[]byte(`[{"type":"document","url":"https://example.com/doc.pdf"}]`),
		now,
		now,
	}
}

func TestScanDisputeInto_Success(t *testing.T) {
	t.Parallel()

	ms := &mockScanner{values: validScannerValues()}

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, dispute.DisputeCategoryBankFeeError, result.Category)
	assert.Equal(t, dispute.DisputeStateDraft, result.State)
	assert.Equal(t, "Test description", result.Description)
	assert.Equal(t, "user@example.com", result.OpenedBy)
	assert.Nil(t, result.Resolution)
	assert.Nil(t, result.ReopenReason)
	assert.Len(t, result.Evidence, 1)
}

func TestScanDisputeInto_ScanError(t *testing.T) {
	t.Parallel()

	scanErr := errors.New("connection reset")
	ms := &mockScanner{err: scanErr}

	result, err := scanDisputeInto(ms)
	require.ErrorIs(t, err, scanErr)
	require.Nil(t, result)
}

func TestScanDisputeInto_InvalidDisputeID(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[0] = "not-a-uuid"
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestScanDisputeInto_InvalidExceptionID(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[1] = "not-a-uuid"
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.Error(t, err)
	require.Nil(t, result)
}

func TestScanDisputeInto_InvalidCategory(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[2] = "INVALID_CATEGORY"
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, dispute.ErrInvalidDisputeCategory)
}

func TestScanDisputeInto_InvalidState(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[3] = "INVALID_STATE"
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.Error(t, err)
	require.Nil(t, result)
	assert.ErrorIs(t, err, dispute.ErrInvalidDisputeState)
}

func TestScanDisputeInto_MalformedEvidence(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[8] = []byte(`{not valid json`)
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.Error(t, err)
	require.Nil(t, result)
	assert.Contains(t, err.Error(), "unmarshal evidence")
}

func TestScanDisputeInto_NullEvidence(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[8] = []byte("null")
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Evidence)
}

func TestScanDisputeInto_EmptyEvidence(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[8] = []byte{}
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Evidence)
}

func TestScanDisputeInto_WithNullableFields(t *testing.T) {
	t.Parallel()

	values := validScannerValues()
	values[6] = sql.NullString{String: "resolved via review", Valid: true}
	values[7] = sql.NullString{String: "new evidence found", Valid: true}
	ms := &mockScanner{values: values}

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Resolution)
	assert.Equal(t, "resolved via review", *result.Resolution)
	require.NotNil(t, result.ReopenReason)
	assert.Equal(t, "new evidence found", *result.ReopenReason)
}

func TestScanDisputeInto_RepeatedCalls(t *testing.T) {
	t.Parallel()

	ms := &mockScanner{values: validScannerValues()}

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)

	result2, err2 := scanDisputeInto(ms)
	require.NoError(t, err2)
	require.NotNil(t, result2)
}

func TestScannerInterface_AcceptsBothTypes(t *testing.T) {
	t.Parallel()

	var _ scanner = (*sql.Row)(nil)
	var _ scanner = (*sql.Rows)(nil)

	ms := &mockScanner{values: validScannerValues()}
	var _ scanner = ms

	result, err := scanDisputeInto(ms)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestScanDisputeInto_AllCategories(t *testing.T) {
	t.Parallel()

	categories := []string{"BANK_FEE_ERROR", "UNRECOGNIZED_CHARGE", "DUPLICATE_TRANSACTION", "OTHER"}

	for _, cat := range categories {
		t.Run(fmt.Sprintf("category_%s", cat), func(t *testing.T) {
			t.Parallel()

			values := validScannerValues()
			values[2] = cat
			ms := &mockScanner{values: values}

			result, err := scanDisputeInto(ms)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, cat, string(result.Category))
		})
	}
}

func TestScanDisputeInto_AllStates(t *testing.T) {
	t.Parallel()

	states := []string{"DRAFT", "OPEN", "PENDING_EVIDENCE", "WON", "LOST"}

	for _, st := range states {
		t.Run(fmt.Sprintf("state_%s", st), func(t *testing.T) {
			t.Parallel()

			values := validScannerValues()
			values[3] = st
			ms := &mockScanner{values: values}

			result, err := scanDisputeInto(ms)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, st, string(result.State))
		})
	}
}
