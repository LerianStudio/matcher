//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
)

func TestAuditLogRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ AuditLogRepository = mocks.NewMockAuditLogRepository(ctrl)
}

func TestAuditLogRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockAuditLogRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}
