//go:build unit

package repositories

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/governance/domain/repositories/mocks"
)

func TestActorMappingRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ ActorMappingRepository = mocks.NewMockActorMappingRepository(ctrl)
}

func TestActorMappingRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockActorMappingRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}
