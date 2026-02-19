//go:build unit

package ports_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	"github.com/LerianStudio/matcher/internal/exception/ports/mocks"
)

var (
	errNotFound  = errors.New("exception not found")
	errInvalidID = errors.New("invalid id")
)

func newExceptionFinderMock(t *testing.T) *mocks.MockExceptionFinder {
	t.Helper()

	ctrl := gomock.NewController(t)

	return mocks.NewMockExceptionFinder(ctrl)
}

func TestExceptionFinder_Interface(t *testing.T) {
	t.Parallel()

	t.Run("mock implements interface", func(t *testing.T) {
		t.Parallel()

		var finder ports.ExceptionFinder = newExceptionFinderMock(t)
		require.NotNil(t, finder)
	})
}

func TestExceptionFinder_FindByID(t *testing.T) {
	t.Parallel()

	t.Run("returns exception when found", func(t *testing.T) {
		t.Parallel()

		expectedID := uuid.New()
		now := time.Now().UTC()
		expectedReason := "Test reason"
		expectedException := &entities.Exception{
			ID:            expectedID,
			TransactionID: uuid.New(),
			Severity:      value_objects.ExceptionSeverityMedium,
			Status:        value_objects.ExceptionStatusOpen,
			Reason:        &expectedReason,
			CreatedAt:     now,
			UpdatedAt:     now,
		}

		finder := newExceptionFinderMock(t)
		finder.EXPECT().FindByID(gomock.Any(), expectedID).Return(expectedException, nil)

		result, err := finder.FindByID(context.Background(), expectedID)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, expectedID, result.ID)
		require.Equal(t, expectedException.TransactionID, result.TransactionID)
		require.Equal(t, value_objects.ExceptionSeverityMedium, result.Severity)
		require.Equal(t, value_objects.ExceptionStatusOpen, result.Status)
	})

	t.Run("returns error when not found", func(t *testing.T) {
		t.Parallel()

		finder := newExceptionFinderMock(t)
		finder.EXPECT().FindByID(gomock.Any(), gomock.Any()).Return(nil, errNotFound)

		result, err := finder.FindByID(context.Background(), uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, errNotFound)
		require.Nil(t, result)
	})

	t.Run("returns nil for nil UUID", func(t *testing.T) {
		t.Parallel()

		finder := newExceptionFinderMock(t)
		finder.EXPECT().FindByID(gomock.Any(), uuid.Nil).Return(nil, errInvalidID)

		result, err := finder.FindByID(context.Background(), uuid.Nil)
		require.Error(t, err)
		require.Nil(t, result)
	})

	t.Run("context is passed through", func(t *testing.T) {
		t.Parallel()

		type contextKey string

		key := contextKey("test-key")
		expectedValue := "test-value"

		var receivedValue any

		finder := newExceptionFinderMock(t)
		finder.EXPECT().
			FindByID(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ uuid.UUID) (*entities.Exception, error) {
				receivedValue = ctx.Value(key)
				return nil, nil
			})

		ctx := context.WithValue(context.Background(), key, expectedValue)
		_, _ = finder.FindByID(ctx, uuid.New())

		require.Equal(t, expectedValue, receivedValue)
	})

	t.Run("handles different exception statuses", func(t *testing.T) {
		t.Parallel()

		testExceptionStatuses(t)
	})

	t.Run("handles different severities", func(t *testing.T) {
		t.Parallel()

		testExceptionSeverities(t)
	})

	t.Run("returns exception with all fields populated", func(t *testing.T) {
		t.Parallel()

		expected := createFullyPopulatedException()

		finder := newExceptionFinderMock(t)
		finder.EXPECT().FindByID(gomock.Any(), expected.ID).Return(expected, nil)

		result, err := finder.FindByID(context.Background(), expected.ID)
		require.NoError(t, err)
		require.NotNil(t, result)
		verifyFullExceptionFields(t, expected, result)
	})
}

func testExceptionStatuses(t *testing.T) {
	t.Helper()

	statuses := []value_objects.ExceptionStatus{
		value_objects.ExceptionStatusOpen,
		value_objects.ExceptionStatusAssigned,
		value_objects.ExceptionStatusResolved,
	}

	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			finder := newExceptionFinderMock(t)
			finder.EXPECT().FindByID(gomock.Any(), gomock.Any()).Return(&entities.Exception{
				ID:     uuid.New(),
				Status: status,
			}, nil)

			result, err := finder.FindByID(context.Background(), uuid.New())
			require.NoError(t, err)
			require.Equal(t, status, result.Status)
		})
	}
}

func testExceptionSeverities(t *testing.T) {
	t.Helper()

	severities := []value_objects.ExceptionSeverity{
		value_objects.ExceptionSeverityLow,
		value_objects.ExceptionSeverityMedium,
		value_objects.ExceptionSeverityHigh,
		value_objects.ExceptionSeverityCritical,
	}

	for _, severity := range severities {
		t.Run(string(severity), func(t *testing.T) {
			t.Parallel()

			finder := newExceptionFinderMock(t)
			finder.EXPECT().FindByID(gomock.Any(), gomock.Any()).Return(&entities.Exception{
				ID:       uuid.New(),
				Severity: severity,
			}, nil)

			result, err := finder.FindByID(context.Background(), uuid.New())
			require.NoError(t, err)
			require.Equal(t, severity, result.Severity)
		})
	}
}

func createFullyPopulatedException() *entities.Exception {
	now := time.Now().UTC()
	dueAt := now.Add(24 * time.Hour)
	assignee := "analyst@example.com"
	resolution := "Fixed"
	resolutionType := "MANUAL"
	resolutionReason := "Duplicate entry"
	reason := "Amount mismatch"
	externalSystem := "JIRA"
	externalIssueID := "JIRA-123"

	return &entities.Exception{
		ID:               uuid.New(),
		TransactionID:    uuid.New(),
		Severity:         value_objects.ExceptionSeverityHigh,
		Status:           value_objects.ExceptionStatusResolved,
		ExternalSystem:   &externalSystem,
		ExternalIssueID:  &externalIssueID,
		AssignedTo:       &assignee,
		DueAt:            &dueAt,
		ResolutionNotes:  &resolution,
		ResolutionType:   &resolutionType,
		ResolutionReason: &resolutionReason,
		Reason:           &reason,
		Version:          5,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func verifyFullExceptionFields(t *testing.T, expected, actual *entities.Exception) {
	t.Helper()

	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.TransactionID, actual.TransactionID)
	require.Equal(t, expected.Severity, actual.Severity)
	require.Equal(t, expected.Status, actual.Status)
	require.Equal(t, expected.ExternalSystem, actual.ExternalSystem)
	require.Equal(t, expected.ExternalIssueID, actual.ExternalIssueID)
	require.Equal(t, expected.AssignedTo, actual.AssignedTo)
	require.Equal(t, expected.DueAt, actual.DueAt)
	require.Equal(t, expected.ResolutionNotes, actual.ResolutionNotes)
	require.Equal(t, expected.ResolutionType, actual.ResolutionType)
	require.Equal(t, expected.ResolutionReason, actual.ResolutionReason)
	require.Equal(t, expected.Reason, actual.Reason)
	require.Equal(t, expected.Version, actual.Version)
	require.Equal(t, expected.CreatedAt, actual.CreatedAt)
	require.Equal(t, expected.UpdatedAt, actual.UpdatedAt)
}
