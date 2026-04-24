// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackageExists(t *testing.T) {
	t.Parallel()

	// commands.go contains package-level documentation only. This tiny test exists
	// so scripts/check-tests.sh can enforce one *_test.go companion per source file.
	assert.NotEmpty(t, "command", "package should exist")
}
