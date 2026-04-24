// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports_test

import (
	"testing"

	"github.com/LerianStudio/matcher/internal/matching/adapters/postgres/exception_creator"
	"github.com/LerianStudio/matcher/internal/matching/ports"
)

func TestExceptionCreator_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	// Verify the postgres implementation satisfies the interface
	var _ ports.ExceptionCreator = (*exception_creator.Repository)(nil)
}
