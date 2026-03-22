//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
)

func TestValidateUnmatchInput_TableDriven(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000300001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000300002")
	tenantID := mustDefaultTenantUUID(t)

	tests := []struct {
		name    string
		input   UnmatchInput
		wantErr error
	}{
		{
			name: "valid input",
			input: UnmatchInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "User requested unmatch",
			},
			wantErr: nil,
		},
		{
			name: "missing tenant id",
			input: UnmatchInput{
				TenantID:     uuid.Nil,
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "reason",
			},
			wantErr: ErrTenantIDRequired,
		},
		{
			name: "missing context id",
			input: UnmatchInput{
				TenantID:     tenantID,
				ContextID:    uuid.Nil,
				MatchGroupID: matchGroupID,
				Reason:       "reason",
			},
			wantErr: ErrUnmatchContextIDRequired,
		},
		{
			name: "missing match group id",
			input: UnmatchInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: uuid.Nil,
				Reason:       "reason",
			},
			wantErr: ErrUnmatchMatchGroupIDRequired,
		},
		{
			name: "empty reason",
			input: UnmatchInput{
				TenantID:     tenantID,
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "",
			},
			wantErr: ErrUnmatchReasonRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateUnmatchInput(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTenantFromContext_NilTenantID(t *testing.T) {
	t.Parallel()

	err := validateTenantFromContext(context.Background(), uuid.Nil)
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestValidateTenantFromContext_NoTenantInCtx(t *testing.T) {
	t.Parallel()

	// When no tenant key in context, validation passes (lenient)
	err := validateTenantFromContext(context.Background(), uuid.New())
	require.NoError(t, err)
}

func TestValidateTenantFromContext_MatchingTenant(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000300010")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())

	err := validateTenantFromContext(ctx, tenantID)
	require.NoError(t, err)
}

func TestValidateTenantFromContext_MismatchTenant(t *testing.T) {
	t.Parallel()

	ctxTenant := uuid.MustParse("00000000-0000-0000-0000-000000300020")
	inputTenant := uuid.MustParse("00000000-0000-0000-0000-000000300021")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, ctxTenant.String())

	err := validateTenantFromContext(ctx, inputTenant)
	require.ErrorIs(t, err, ErrTenantIDMismatch)
}

func TestValidateTenantFromContext_EmptyStringInCtx(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "   ")

	err := validateTenantFromContext(ctx, uuid.New())
	require.NoError(t, err) // empty string is lenient
}

func TestValidateTenantFromContext_InvalidUUIDInCtx(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")

	err := validateTenantFromContext(ctx, uuid.New())
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestValidateTenantFromContextStrict_NilTenantID(t *testing.T) {
	t.Parallel()

	err := validateTenantFromContextStrict(context.Background(), uuid.Nil)
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestValidateTenantFromContextStrict_InvalidUUID(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "bad-uuid")
	err := validateTenantFromContextStrict(ctx, uuid.New())
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestValidateTenantFromContextStrict_Mismatch(t *testing.T) {
	t.Parallel()

	ctxTenant := uuid.MustParse("00000000-0000-0000-0000-000000300030")
	inputTenant := uuid.MustParse("00000000-0000-0000-0000-000000300031")
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, ctxTenant.String())

	err := validateTenantFromContextStrict(ctx, inputTenant)
	require.ErrorIs(t, err, ErrTenantIDMismatch)
}

func TestUnmatchSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{"ErrUnmatchContextIDRequired", ErrUnmatchContextIDRequired, "context id is required"},
		{"ErrUnmatchMatchGroupIDRequired", ErrUnmatchMatchGroupIDRequired, "match group id is required"},
		{"ErrUnmatchReasonRequired", ErrUnmatchReasonRequired, "reason is required"},
		{"ErrMatchGroupNotFound", ErrMatchGroupNotFound, "match group not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}
