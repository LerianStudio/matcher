//go:build unit

package ports_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	"github.com/LerianStudio/matcher/internal/matching/ports/mocks"
)

func TestMatchEventPublisher_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := mocks.NewMockMatchEventPublisher(ctrl)

	require.NotNil(t, mock)

	var _ ports.MatchEventPublisher = mock
}
