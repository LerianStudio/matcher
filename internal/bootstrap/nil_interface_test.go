// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"testing"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
)

func TestIsNilInterface(t *testing.T) {
	t.Parallel()

	var nilLogger *libLog.NopLogger
	var nilMap map[string]string

	assert.True(t, isNilInterface(nil))
	assert.True(t, isNilInterface(nilLogger))
	assert.True(t, isNilInterface(nilMap))
	assert.False(t, isNilInterface(&libLog.NopLogger{}))
	assert.False(t, isNilInterface(42))
}
