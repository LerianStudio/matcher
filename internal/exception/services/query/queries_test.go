//go:build unit

package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	govEntities "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for testing.
var (
	errTestFind  = errors.New("test: find failed")
	errTestList  = errors.New("test: list failed")
	errTestAudit = errors.New("test: audit failed")
)

// stubExceptionRepository implements repositories.ExceptionRepository for testing.
type stubExceptionRepository struct {
	findException  *entities.Exception
	findErr        error
	listResult     []*entities.Exception
	listPagination libHTTP.CursorPagination
	listErr        error
	lastFilter     repositories.ExceptionFilter
	lastCursor     repositories.CursorFilter
}

func (repo *stubExceptionRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.Exception, error) {
	if repo.findErr != nil {
		return nil, repo.findErr
	}

	return repo.findException, nil
}

func (repo *stubExceptionRepository) List(
	_ context.Context,
	filter repositories.ExceptionFilter,
	cursor repositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	if repo.listErr != nil {
		return nil, libHTTP.CursorPagination{}, repo.listErr
	}

	repo.lastFilter = filter
	repo.lastCursor = cursor

	return repo.listResult, repo.listPagination, nil
}

func (repo *stubExceptionRepository) Update(
	_ context.Context,
	exception *entities.Exception,
) (*entities.Exception, error) {
	return exception, nil
}

func (repo *stubExceptionRepository) UpdateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	exception *entities.Exception,
) (*entities.Exception, error) {
	return repo.Update(ctx, exception)
}

// stubAuditLogRepository implements governance AuditLogRepository for testing.
type stubAuditLogRepository struct {
	listResult     []*govEntities.AuditLog
	listNextCursor string
	listErr        error
	capturedLimit  int
}

func (repo *stubAuditLogRepository) Create(
	_ context.Context,
	_ *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (repo *stubAuditLogRepository) CreateWithTx(
	ctx context.Context,
	_ sharedPorts.Tx,
	auditLog *govEntities.AuditLog,
) (*govEntities.AuditLog, error) {
	return repo.Create(ctx, auditLog)
}

func (repo *stubAuditLogRepository) GetByID(
	_ context.Context,
	_ uuid.UUID,
) (*govEntities.AuditLog, error) {
	return nil, nil
}

func (repo *stubAuditLogRepository) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *libHTTP.TimestampCursor,
	limit int,
) ([]*govEntities.AuditLog, string, error) {
	if repo.listErr != nil {
		return nil, "", repo.listErr
	}

	repo.capturedLimit = limit

	return repo.listResult, repo.listNextCursor, nil
}

func (repo *stubAuditLogRepository) List(
	_ context.Context,
	_ govEntities.AuditLogFilter,
	_ *libHTTP.TimestampCursor,
	limit int,
) ([]*govEntities.AuditLog, string, error) {
	if repo.listErr != nil {
		return nil, "", repo.listErr
	}

	repo.capturedLimit = limit

	return repo.listResult, repo.listNextCursor, nil
}

// stubTenantExtractor implements TenantExtractor for testing.
type stubTenantExtractor struct {
	tenantID uuid.UUID
}

func (e *stubTenantExtractor) GetTenantID(_ context.Context) uuid.UUID {
	return e.tenantID
}

func tenantExtractor(tenantID uuid.UUID) *stubTenantExtractor {
	return &stubTenantExtractor{tenantID: tenantID}
}

func newTestException() *entities.Exception {
	txnID := uuid.New()

	return &entities.Exception{
		ID:            uuid.New(),
		TransactionID: txnID,
		Severity:      value_objects.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusOpen,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
}

// NewUseCase Tests.
func TestNewUseCase_Success(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{}
	disputeRepo := &stubDisputeRepository{}
	auditRepo := &stubAuditLogRepository{}
	extractor := tenantExtractor(uuid.New())

	uc, err := NewUseCase(exceptionRepo, disputeRepo, auditRepo, extractor)

	require.NoError(t, err)
	require.NotNil(t, uc)
}

func TestNewUseCase_NilExceptionRepository(t *testing.T) {
	t.Parallel()

	auditRepo := &stubAuditLogRepository{}
	disputeRepo := &stubDisputeRepository{}

	uc, err := NewUseCase(nil, disputeRepo, auditRepo, nil)

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilDisputeRepository(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, nil, auditRepo, nil)

	require.ErrorIs(t, err, ErrNilDisputeRepository)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilAuditRepository(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{}
	disputeRepo := &stubDisputeRepository{}

	uc, err := NewUseCase(exceptionRepo, disputeRepo, nil, nil)

	require.ErrorIs(t, err, ErrNilAuditRepository)
	assert.Nil(t, uc)
}

func TestNewUseCase_NilTenantExtractor_Allowed(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{}
	disputeRepo := &stubDisputeRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, disputeRepo, auditRepo, nil)

	require.NoError(t, err)
	require.NotNil(t, uc)
	assert.Nil(t, uc.tenantExtractor)
}

func TestNewUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewUseCase(nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

// GetException Tests.
func TestGetException_Success(t *testing.T) {
	t.Parallel()

	expectedException := newTestException()
	exceptionRepo := &stubExceptionRepository{findException: expectedException}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	result, err := uc.GetException(ctx, expectedException.ID)

	require.NoError(t, err)
	assert.Equal(t, expectedException.ID, result.ID)
	assert.Equal(t, expectedException.TransactionID, result.TransactionID)
	assert.Equal(t, expectedException.Severity, result.Severity)
}

func TestGetException_NotFound(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{findException: nil}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	result, err := uc.GetException(ctx, uuid.New())

	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
	assert.Nil(t, result)
}

func TestGetException_RepositoryError(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{findErr: errTestFind}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	result, err := uc.GetException(ctx, uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, errTestFind)
	assert.Contains(t, err.Error(), "finding exception")
	assert.Nil(t, result)
}

func TestGetException_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	result, err := uc.GetException(ctx, uuid.New())

	require.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, result)
}

// ListExceptions Tests.
func TestListExceptions_Success(t *testing.T) {
	t.Parallel()

	exceptions := []*entities.Exception{
		newTestException(),
		newTestException(),
	}

	pagination := libHTTP.CursorPagination{
		Next: "next-cursor",
	}

	exceptionRepo := &stubExceptionRepository{
		listResult:     exceptions,
		listPagination: pagination,
	}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	query := ListQuery{
		Filter: repositories.ExceptionFilter{},
		Cursor: repositories.CursorFilter{Limit: 10},
	}

	results, pag, err := uc.ListExceptions(ctx, query)

	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "next-cursor", pag.Next)
}

func TestListExceptions_EmptyResult(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{
		listResult:     []*entities.Exception{},
		listPagination: libHTTP.CursorPagination{},
	}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	query := ListQuery{}

	results, pag, err := uc.ListExceptions(ctx, query)

	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Empty(t, pag.Next)
}

func TestListExceptions_WithFilters(t *testing.T) {
	t.Parallel()

	status := value_objects.ExceptionStatusOpen
	severity := value_objects.ExceptionSeverityHigh
	assignee := "analyst-1"

	exceptions := []*entities.Exception{newTestException()}
	exceptionRepo := &stubExceptionRepository{
		listResult: exceptions,
	}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	query := ListQuery{
		Filter: repositories.ExceptionFilter{
			Status:     &status,
			Severity:   &severity,
			AssignedTo: &assignee,
		},
		Cursor: repositories.CursorFilter{
			Limit:     20,
			SortBy:    "created_at",
			SortOrder: "desc",
		},
	}

	results, _, err := uc.ListExceptions(ctx, query)

	require.NoError(t, err)
	assert.Len(t, results, 1)
	require.NotNil(t, exceptionRepo.lastFilter.Status)
	require.NotNil(t, exceptionRepo.lastFilter.Severity)
	require.NotNil(t, exceptionRepo.lastFilter.AssignedTo)
	assert.Equal(t, status, *exceptionRepo.lastFilter.Status)
	assert.Equal(t, severity, *exceptionRepo.lastFilter.Severity)
	assert.Equal(t, assignee, *exceptionRepo.lastFilter.AssignedTo)
	assert.Equal(t, 20, exceptionRepo.lastCursor.Limit)
	assert.Equal(t, "created_at", exceptionRepo.lastCursor.SortBy)
	assert.Equal(t, "desc", exceptionRepo.lastCursor.SortOrder)
}

func TestListExceptions_RepositoryError(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepository{listErr: errTestList}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	results, pag, err := uc.ListExceptions(ctx, ListQuery{})

	require.Error(t, err)
	require.ErrorIs(t, err, errTestList)
	assert.Contains(t, err.Error(), "listing exceptions")
	assert.Nil(t, results)
	assert.Equal(t, libHTTP.CursorPagination{}, pag)
}

func TestListExceptions_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	results, pag, err := uc.ListExceptions(ctx, ListQuery{})

	require.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, results)
	assert.Equal(t, libHTTP.CursorPagination{}, pag)
}

// GetHistory Tests.
func TestGetHistory_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()
	actor := "user@example.com"
	now := time.Now().UTC()

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "CREATED",
			ActorID:    &actor,
			Changes:    []byte(`{"status":"OPEN"}`),
			CreatedAt:  now,
		},
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "ASSIGNED",
			ActorID:    &actor,
			Changes:    []byte(`{"status":"ASSIGNED"}`),
			CreatedAt:  now.Add(-1 * time.Hour),
		},
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{
		listResult:     auditLogs,
		listNextCursor: "next-page",
	}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 20)

	require.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, "next-page", nextCursor)
	assert.Equal(t, "CREATED", entries[0].Action)
	assert.Equal(t, &actor, entries[0].ActorID)
}

// TestGetHistory_SkipsNilAuditLogs mirrors the audit consumer's defensive
// guard: ListByEntity is expected never to return nil elements, but if the
// repository misbehaves we must not nil-deref building HistoryEntry rows.
func TestGetHistory_SkipsNilAuditLogs(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()
	now := time.Now().UTC()

	auditLogs := []*govEntities.AuditLog{
		nil,
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "CREATED",
			Changes:    []byte(`{}`),
			CreatedAt:  now,
		},
		nil,
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{
		listResult: auditLogs,
	}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, _, err := uc.GetHistory(ctx, exceptionID, "", 20)

	require.NoError(t, err)
	require.Len(t, entries, 1, "nil entries must be skipped")
	assert.Equal(t, "CREATED", entries[0].Action)
}

func TestGetHistory_EmptyResult(t *testing.T) {
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
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.NoError(t, err)
	assert.Empty(t, entries)
	assert.Empty(t, nextCursor)
}

func TestGetHistory_WithCursor(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	cursorTimestamp := time.Now().UTC()
	cursorID := uuid.New()
	encodedCursor, err := libHTTP.EncodeTimestampCursor(cursorTimestamp, cursorID)
	require.NoError(t, err)

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "RESOLVED",
			Changes:    []byte(`{}`),
			CreatedAt:  cursorTimestamp.Add(-1 * time.Hour),
		},
	}

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{
		listResult: auditLogs,
	}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, _, err := uc.GetHistory(ctx, exceptionID, encodedCursor, 10)

	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestGetHistory_InvalidCursor(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "invalid-cursor", 10)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding cursor")
	assert.Nil(t, entries)
	assert.Empty(t, nextCursor)
}

func TestGetHistory_DefaultLimit(t *testing.T) {
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

	_, _, err = uc.GetHistory(ctx, exceptionID, "", 0)
	require.NoError(t, err)
	assert.Equal(t, 20, auditRepo.capturedLimit)

	_, _, err = uc.GetHistory(ctx, exceptionID, "", -5)
	require.NoError(t, err)
	assert.Equal(t, 20, auditRepo.capturedLimit)
}

func TestGetHistory_TenantIDRequired(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(uuid.Nil))
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.ErrorIs(t, err, ErrTenantIDRequired)
	assert.Nil(t, entries)
	assert.Empty(t, nextCursor)
}

func TestGetHistory_NilTenantExtractor(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, nil)
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.ErrorIs(t, err, ErrTenantIDRequired)
	assert.Nil(t, entries)
	assert.Empty(t, nextCursor)
}

func TestGetHistory_RepositoryError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{listErr: errTestAudit}

	uc, err := NewUseCase(exceptionRepo, &stubDisputeRepository{}, auditRepo, tenantExtractor(tenantID))
	require.NoError(t, err)

	ctx := t.Context()
	entries, nextCursor, err := uc.GetHistory(ctx, exceptionID, "", 10)

	require.Error(t, err)
	require.ErrorIs(t, err, errTestAudit)
	assert.Contains(t, err.Error(), "fetching audit history")
	assert.Nil(t, entries)
	assert.Empty(t, nextCursor)
}

func TestGetHistory_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	entries, nextCursor, err := uc.GetHistory(ctx, uuid.New(), "", 10)

	require.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, entries)
	assert.Empty(t, nextCursor)
}

// HistoryEntry Tests.
func TestHistoryEntry_DateFormatting(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()
	actor := "user@test.com"
	createdAt := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "UPDATED",
			ActorID:    &actor,
			Changes:    []byte(`{"field":"value"}`),
			CreatedAt:  createdAt,
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
	assert.Equal(t, "2024-01-15T10:30:00Z", entries[0].CreatedAt)
}

func TestHistoryEntry_NilActorID(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	exceptionID := uuid.New()

	auditLogs := []*govEntities.AuditLog{
		{
			ID:         uuid.New(),
			TenantID:   tenantID,
			EntityType: "exception",
			EntityID:   exceptionID,
			Action:     "SYSTEM_UPDATE",
			ActorID:    nil,
			Changes:    []byte(`{}`),
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
	assert.Nil(t, entries[0].ActorID)
}

// ListQuery Tests.
func TestListQuery_EmptyQuery(t *testing.T) {
	t.Parallel()

	query := ListQuery{}

	assert.Nil(t, query.Filter.Status)
	assert.Nil(t, query.Filter.Severity)
	assert.Nil(t, query.Filter.AssignedTo)
	assert.Equal(t, 0, query.Cursor.Limit)
}

func TestListQuery_WithAllFilters(t *testing.T) {
	t.Parallel()

	status := value_objects.ExceptionStatusAssigned
	severity := value_objects.ExceptionSeverityCritical
	assignee := "team-lead"
	system := "JIRA"
	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()

	query := ListQuery{
		Filter: repositories.ExceptionFilter{
			Status:         &status,
			Severity:       &severity,
			AssignedTo:     &assignee,
			ExternalSystem: &system,
			DateFrom:       &from,
			DateTo:         &to,
		},
		Cursor: repositories.CursorFilter{
			Limit:     50,
			Cursor:    "abc123",
			SortBy:    "severity",
			SortOrder: "asc",
		},
	}

	assert.Equal(t, &status, query.Filter.Status)
	assert.Equal(t, &severity, query.Filter.Severity)
	assert.Equal(t, 50, query.Cursor.Limit)
	assert.Equal(t, "abc123", query.Cursor.Cursor)
}

// stubDisputeRepository implements repositories.DisputeRepository for testing.
type stubDisputeRepository struct {
	findDispute    *dispute.Dispute
	findErr        error
	listResult     []*dispute.Dispute
	listPagination libHTTP.CursorPagination
	listErr        error
}

func (repo *stubDisputeRepository) Create(
	_ context.Context,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return d, nil
}

func (repo *stubDisputeRepository) CreateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return repo.Create(ctx, d)
}

func (repo *stubDisputeRepository) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*dispute.Dispute, error) {
	if repo.findErr != nil {
		return nil, repo.findErr
	}

	return repo.findDispute, nil
}

func (repo *stubDisputeRepository) FindByExceptionID(
	_ context.Context,
	_ uuid.UUID,
) (*dispute.Dispute, error) {
	return repo.findDispute, repo.findErr
}

func (repo *stubDisputeRepository) List(
	_ context.Context,
	_ repositories.DisputeFilter,
	_ repositories.CursorFilter,
) ([]*dispute.Dispute, libHTTP.CursorPagination, error) {
	if repo.listErr != nil {
		return nil, libHTTP.CursorPagination{}, repo.listErr
	}

	return repo.listResult, repo.listPagination, nil
}

func (repo *stubDisputeRepository) Update(
	_ context.Context,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return d, nil
}

func (repo *stubDisputeRepository) UpdateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	d *dispute.Dispute,
) (*dispute.Dispute, error) {
	return repo.Update(ctx, d)
}

func newTestDispute() *dispute.Dispute {
	now := time.Now().UTC()

	return &dispute.Dispute{
		ID:          uuid.New(),
		ExceptionID: uuid.New(),
		Category:    dispute.DisputeCategoryBankFeeError,
		State:       dispute.DisputeStateOpen,
		Description: "test dispute",
		OpenedBy:    "analyst@test.com",
		Evidence:    []dispute.Evidence{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func newUseCaseWithDisputes(
	t *testing.T,
	disputeRepo *stubDisputeRepository,
) *UseCase {
	t.Helper()

	exceptionRepo := &stubExceptionRepository{}
	auditRepo := &stubAuditLogRepository{}

	uc, err := NewUseCase(exceptionRepo, disputeRepo, auditRepo, nil)
	require.NoError(t, err)

	return uc
}

// GetDispute Tests.
func TestGetDispute_Success(t *testing.T) {
	t.Parallel()

	expected := newTestDispute()
	disputeRepo := &stubDisputeRepository{findDispute: expected}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	result, err := uc.GetDispute(ctx, expected.ID)

	require.NoError(t, err)
	assert.Equal(t, expected.ID, result.ID)
	assert.Equal(t, expected.Category, result.Category)
}

func TestGetDispute_NotFound(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepository{findDispute: nil}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	result, err := uc.GetDispute(ctx, uuid.New())

	require.ErrorIs(t, err, ErrDisputeNotFound)
	assert.Nil(t, result)
}

func TestGetDispute_RepositoryError(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepository{findErr: errTestFind}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	result, err := uc.GetDispute(ctx, uuid.New())

	require.Error(t, err)
	require.ErrorIs(t, err, errTestFind)
	assert.Contains(t, err.Error(), "finding dispute")
	assert.Nil(t, result)
}

func TestGetDispute_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	result, err := uc.GetDispute(ctx, uuid.New())

	require.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, result)
}

// ListDisputes Tests.
func TestListDisputes_Success(t *testing.T) {
	t.Parallel()

	disputes := []*dispute.Dispute{newTestDispute(), newTestDispute()}
	pagination := libHTTP.CursorPagination{Next: "next-cursor"}

	disputeRepo := &stubDisputeRepository{
		listResult:     disputes,
		listPagination: pagination,
	}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	results, pag, err := uc.ListDisputes(ctx, DisputeListQuery{
		Cursor: repositories.CursorFilter{Limit: 10},
	})

	require.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, "next-cursor", pag.Next)
}

func TestListDisputes_EmptyResult(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepository{
		listResult: []*dispute.Dispute{},
	}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	results, pag, err := uc.ListDisputes(ctx, DisputeListQuery{})

	require.NoError(t, err)
	assert.Empty(t, results)
	assert.Empty(t, pag.Next)
}

func TestListDisputes_RepositoryError(t *testing.T) {
	t.Parallel()

	disputeRepo := &stubDisputeRepository{listErr: errTestList}
	uc := newUseCaseWithDisputes(t, disputeRepo)

	ctx := t.Context()
	results, pag, err := uc.ListDisputes(ctx, DisputeListQuery{})

	require.Error(t, err)
	require.ErrorIs(t, err, errTestList)
	assert.Contains(t, err.Error(), "listing disputes")
	assert.Nil(t, results)
	assert.Equal(t, libHTTP.CursorPagination{}, pag)
}

func TestListDisputes_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	results, pag, err := uc.ListDisputes(ctx, DisputeListQuery{})

	require.ErrorIs(t, err, ErrNilUseCase)
	assert.Nil(t, results)
	assert.Equal(t, libHTTP.CursorPagination{}, pag)
}
