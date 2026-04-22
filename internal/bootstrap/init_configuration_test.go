// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInitConfigurationModule_NilOutboxRepository_ReturnsError verifies that
// initConfigurationModule fails when the outbox repository is nil, because the
// outbox-based audit publisher cannot be constructed without it.
func TestInitConfigurationModule_NilOutboxRepository_ReturnsError(t *testing.T) {
	t.Parallel()

	err := initConfigurationModule(nil, nil, nil, &sharedRepositories{}, false)
	require.Error(t, err)
}
