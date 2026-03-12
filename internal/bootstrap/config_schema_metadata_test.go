// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigSchemaMetadata_ContainsRepresentativeEntries(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, fieldDescriptions)
	require.NotEmpty(t, fieldLabels)
	require.NotEmpty(t, fieldConstraints)

	assert.Equal(t, "Application log verbosity level", fieldDescriptions["app.log_level"])
	assert.Equal(t, "Log Level", fieldLabels["app.log_level"])
	assert.Contains(t, fieldConstraints["app.log_level"], "enum:debug,info,warn,error,fatal")
	assert.Contains(t, fieldConstraints["rate_limit.max"], "min:1")
	assert.Contains(t, fieldConstraints["rate_limit.max"], "max:1000000")
	assert.Contains(t, fieldConstraints["rate_limit.expiry_sec"], "max:86400")
	assert.Contains(t, fieldConstraints["rate_limit.export_expiry_sec"], "max:86400")
	assert.Contains(t, fieldConstraints["rate_limit.dispatch_expiry_sec"], "max:86400")
	assert.Contains(t, fieldConstraints["webhook.timeout_sec"], "min:0")
	assert.Contains(t, fieldConstraints["webhook.timeout_sec"], "max:300")
}
