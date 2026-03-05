//go:build unit

package ports

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"

	sharedHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestAuditLogRepositoryInterfaceExists(t *testing.T) {
	t.Parallel()

	var _ AuditLogRepository = (*mockAuditLogRepository)(nil)
}

type mockAuditLogRepository struct{}

func (m *mockAuditLogRepository) Create(_ context.Context, _ *sharedDomain.AuditLog) (*sharedDomain.AuditLog, error) {
	return nil, nil
}

func (m *mockAuditLogRepository) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	_ *sharedDomain.AuditLog,
) (*sharedDomain.AuditLog, error) {
	return nil, nil
}

func (m *mockAuditLogRepository) GetByID(_ context.Context, _ uuid.UUID) (*sharedDomain.AuditLog, error) {
	return nil, nil
}

func (m *mockAuditLogRepository) ListByEntity(
	_ context.Context,
	_ string,
	_ uuid.UUID,
	_ *sharedHTTP.TimestampCursor,
	_ int,
) ([]*sharedDomain.AuditLog, string, error) {
	return nil, "", nil
}

func (m *mockAuditLogRepository) List(
	_ context.Context,
	_ sharedDomain.AuditLogFilter,
	_ *sharedHTTP.TimestampCursor,
	_ int,
) ([]*sharedDomain.AuditLog, string, error) {
	return nil, "", nil
}
