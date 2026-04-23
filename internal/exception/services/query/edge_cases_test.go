//go:build unit

package query

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	govEntities "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

// Test GetException with adapter-specific not found error.
func TestGetException_AdapterNotFoundError(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{findErr: entities.ErrExceptionNotFound}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	result, err := uc.GetException(ctx, uuid.New())

	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
	assert.Nil(t, result)
}

// Test ListExceptions captures filter and cursor correctly.
func TestListExceptions_CapturesFilterAndCursor(t *testing.T) {
	t.Parallel()

	status := value_objects.ExceptionStatusAssigned
	severity := sharedexception.ExceptionSeverityCritical
	assignee := "analyst-1"
	externalSystem := "JIRA"
	dateFrom := time.Now().Add(-24 * time.Hour)
	dateTo := time.Now()

	exceptionRepo := &stubExceptionRepository{
		listResult:     []*entities.Exception{},
		listPagination: libHTTP.CursorPagination{},
	}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	query := ListQuery{
		Filter: repositories.ExceptionFilter{
			Status:         &status,
			Severity:       &severity,
			AssignedTo:     &assignee,
			ExternalSystem: &externalSystem,
			DateFrom:       &dateFrom,
			DateTo:         &dateTo,
		},
		Cursor: repositories.CursorFilter{
			Limit:     25,
			Cursor:    "cursor123",
			SortBy:    "created_at",
			SortOrder: "desc",
		},
	}

	_, _, err = uc.ListExceptions(ctx, query)
	require.NoError(t, err)

	assert.Equal(t, &status, exceptionRepo.lastFilter.Status)
	assert.Equal(t, &severity, exceptionRepo.lastFilter.Severity)
	assert.Equal(t, &assignee, exceptionRepo.lastFilter.AssignedTo)
	assert.Equal(t, &externalSystem, exceptionRepo.lastFilter.ExternalSystem)
	assert.NotNil(t, exceptionRepo.lastFilter.DateFrom)
	assert.NotNil(t, exceptionRepo.lastFilter.DateTo)
	assert.Equal(t, 25, exceptionRepo.lastCursor.Limit)
	assert.Equal(t, "cursor123", exceptionRepo.lastCursor.Cursor)
	assert.Equal(t, "created_at", exceptionRepo.lastCursor.SortBy)
	assert.Equal(t, "desc", exceptionRepo.lastCursor.SortOrder)
}

// Test ListExceptions with pagination response.
func TestListExceptions_WithPagination(t *testing.T) {
	t.Parallel()

	exceptions := []*entities.Exception{
		newTestException(),
		newTestException(),
	}

	paginationResponse := libHTTP.CursorPagination{
		Next: "next_cursor_123",
		Prev: "prev_cursor_456",
	}

	exceptionRepo := &stubExceptionRepository{
		listResult:     exceptions,
		listPagination: paginationResponse,
	}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	result, pagination, err := uc.ListExceptions(ctx, ListQuery{})

	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "next_cursor_123", pagination.Next)
	assert.Equal(t, "prev_cursor_456", pagination.Prev)
}

// Test GetHistory with multiple audit logs.
func TestGetHistory_MultipleEntries(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()
	actor1 := "analyst-1"
	actor2 := "analyst-2"

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "CREATED",
			ActorID:    &actor1,
			Changes:    []byte(`{"status":"OPEN"}`),
			CreatedAt:  time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "ASSIGNED",
			ActorID:    &actor2,
			Changes:    []byte(`{"assignee":"analyst-2"}`),
			CreatedAt:  time.Date(2026, 1, 16, 14, 30, 0, 0, time.UTC),
		},
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "RESOLVED",
			ActorID:    &actor1,
			Changes:    []byte(`{"status":"RESOLVED"}`),
			CreatedAt:  time.Date(2026, 1, 17, 9, 15, 0, 0, time.UTC),
		},
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{
		listResult:     auditLogs,
		listNextCursor: "next_page_cursor",
	}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Equal(t, "next_page_cursor", nextCursor)

	// Verify entries are mapped correctly
	assert.Equal(t, "CREATED", entries[0].Action)
	assert.Equal(t, "2026-01-15T10:00:00Z", entries[0].CreatedAt)
	assert.NotNil(t, entries[0].ActorID)
	assert.Equal(t, "analyst-1", *entries[0].ActorID)

	assert.Equal(t, "ASSIGNED", entries[1].Action)
	assert.Equal(t, "2026-01-16T14:30:00Z", entries[1].CreatedAt)

	assert.Equal(t, "RESOLVED", entries[2].Action)
	assert.Equal(t, "2026-01-17T09:15:00Z", entries[2].CreatedAt)
}

// Test GetHistory changes field mapping.
func TestGetHistory_ChangesFieldMapping(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	expectedChanges := []byte(`{"field1":"value1","field2":"value2","nested":{"key":"val"}}`)

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "UPDATED",
			ActorID:    nil,
			Changes:    expectedChanges,
			CreatedAt:  time.Now().UTC(),
		},
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{listResult: auditLogs}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, _, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, expectedChanges, entries[0].Changes)
}

// Test GetHistory with high limit value.
func TestGetHistory_HighLimitValue(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{
		listResult: []*govEntities.AuditLog{},
	}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	_, _, err = uc.GetHistory(ctx, exceptionID, "", 1000)

	require.NoError(t, err)
	assert.Equal(t, 1000, auditRepo.capturedLimit)
}

// Test UseCase validation order.
func TestNewUseCase_ValidationOrder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		setupFunc   func() (*UseCase, error)
		expectedErr error
	}{
		{
			name: "exception repo checked first",
			setupFunc: func() (*UseCase, error) {
				return NewUseCase(nil, nil, nil, nil)
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "dispute repo checked second",
			setupFunc: func() (*UseCase, error) {
				return NewUseCase(&stubExceptionRepository{}, nil, nil, nil)
			},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "audit repo checked third",
			setupFunc: func() (*UseCase, error) {
				return NewUseCase(&stubExceptionRepository{}, &stubDisputeRepository{}, nil, nil)
			},
			expectedErr: ErrNilAuditRepository,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uc, err := tc.setupFunc()

			require.ErrorIs(t, err, tc.expectedErr)
			assert.Nil(t, uc)
		})
	}
}

// Test HistoryEntry with empty changes.
func TestHistoryEntry_EmptyChanges(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "NO_CHANGE",
			ActorID:    nil,
			Changes:    []byte{},
			CreatedAt:  time.Now().UTC(),
		},
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{listResult: auditLogs}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, _, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].Changes)
}

// Test ListQuery default values.
func TestListQuery_DefaultValues(t *testing.T) {
	t.Parallel()

	query := ListQuery{}

	assert.Nil(t, query.Filter.Status)
	assert.Nil(t, query.Filter.Severity)
	assert.Nil(t, query.Filter.AssignedTo)
	assert.Nil(t, query.Filter.ExternalSystem)
	assert.Nil(t, query.Filter.DateFrom)
	assert.Nil(t, query.Filter.DateTo)
	assert.Equal(t, 0, query.Cursor.Limit)
	assert.Empty(t, query.Cursor.Cursor)
	assert.Empty(t, query.Cursor.SortBy)
	assert.Empty(t, query.Cursor.SortOrder)
}

// Test error type definitions.
func TestQueryErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errors := []error{
		ErrNilExceptionRepository,
		ErrNilAuditRepository,
		ErrNilUseCase,
		entities.ErrExceptionNotFound,
		ErrTenantIDRequired,
	}

	seen := make(map[string]bool)

	for _, err := range errors {
		msg := err.Error()
		if seen[msg] {
			t.Errorf("duplicate error message: %q", msg)
		}

		seen[msg] = true
	}
}

// Test error messages.
func TestQueryErrors_Messages(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err      error
		expected string
	}{
		{ErrNilExceptionRepository, "exception repository is required"},
		{ErrNilAuditRepository, "audit repository is required"},
		{ErrNilUseCase, "exception query use case is required"},
		{entities.ErrExceptionNotFound, "exception not found"},
		{ErrTenantIDRequired, "tenant id is required"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, tc.err.Error())
		})
	}
}
