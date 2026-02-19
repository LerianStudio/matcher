//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

func TestJobStatusParsing(t *testing.T) {
	t.Parallel()

	valid := []value_objects.JobStatus{
		value_objects.JobStatusQueued,
		value_objects.JobStatusProcessing,
		value_objects.JobStatusCompleted,
		value_objects.JobStatusFailed,
	}
	for _, status := range valid {
		parsed, err := value_objects.ParseJobStatus(status.String())
		require.NoError(t, err)
		require.Equal(t, status, parsed)
	}

	_, err := value_objects.ParseJobStatus("INVALID")
	require.Error(t, err)
}
