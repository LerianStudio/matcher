// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects_test

import (
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/value_objects"
)

func TestJobStatusRoundTripProperty(t *testing.T) {
	t.Parallel()

	prop := func(status string) bool {
		parsed, err := value_objects.ParseJobStatus(status)
		if err != nil {
			return status != value_objects.JobStatusQueued.String() &&
				status != value_objects.JobStatusProcessing.String() &&
				status != value_objects.JobStatusCompleted.String() &&
				status != value_objects.JobStatusFailed.String()
		}

		return parsed.String() == status
	}

	err := quick.Check(prop, nil)
	require.NoError(t, err)
}
