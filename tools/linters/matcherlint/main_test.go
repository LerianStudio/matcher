// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package main

import (
	"testing"

	"github.com/LerianStudio/matcher/tools/linters/entityconstructor"
	"github.com/LerianStudio/matcher/tools/linters/goroutineleak"
	"github.com/LerianStudio/matcher/tools/linters/observability"
	"github.com/LerianStudio/matcher/tools/linters/repositorytx"
	"github.com/stretchr/testify/assert"
)

func TestAnalyzersRegistered(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, entityconstructor.Analyzer, "entityconstructor.Analyzer should be registered")
	assert.NotNil(t, observability.Analyzer, "observability.Analyzer should be registered")
	assert.NotNil(t, repositorytx.Analyzer, "repositorytx.Analyzer should be registered")
	assert.NotNil(t, goroutineleak.Analyzer, "goroutineleak.Analyzer should be registered")
}

func TestAnalyzerNames(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "entityconstructor", entityconstructor.Analyzer.Name)
	assert.Equal(t, "observability", observability.Analyzer.Name)
	assert.Equal(t, "repositorytx", repositorytx.Analyzer.Name)
	assert.Equal(t, "goroutineleak", goroutineleak.Analyzer.Name)
}
