// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositorytx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzer(t *testing.T) {
	t.Parallel()

	assert.NotNil(t, Analyzer)
	assert.Equal(t, "repositorytx", Analyzer.Name)
	assert.NotEmpty(t, Analyzer.Doc)
	assert.NotNil(t, Analyzer.Run)
}

func TestIsPostgresAdapter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pkgPath  string
		expected bool
	}{
		{"postgres adapter", "github.com/example/internal/adapters/postgres/user", true},
		{"http adapter", "github.com/example/internal/adapters/http", false},
		{"services", "github.com/example/internal/services", false},
		{"domain", "github.com/example/internal/domain", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isPostgresAdapter(tt.pkgPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsWriteMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		expected bool
	}{
		{"Create method", "Create", true},
		{"CreateUser", "CreateUser", true},
		{"Update method", "Update", true},
		{"Delete method", "Delete", true},
		{"Save method", "Save", true},
		{"Insert method", "Insert", true},
		{"Upsert method", "Upsert", true},
		{"MarkMatched method", "MarkMatched", true},
		{"MarkPending method", "MarkPending", true},
		{"Get method", "Get", false},
		{"Find method", "Find", false},
		{"List method", "List", false},
		{"WithTx suffix", "CreateWithTx", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isWriteMethod(tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}
