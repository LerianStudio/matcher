//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntry_Construction(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	entry := Entry{
		Kind:      KindConfig,
		Scope:     ScopeGlobal,
		Subject:   "",
		Key:       "postgres.max_open_conns",
		Value:     25,
		Revision:  Revision(7),
		UpdatedAt: now,
		UpdatedBy: "admin-1",
		Source:    "api",
	}

	assert.Equal(t, KindConfig, entry.Kind)
	assert.Equal(t, ScopeGlobal, entry.Scope)
	assert.Equal(t, "postgres.max_open_conns", entry.Key)
	assert.Equal(t, 25, entry.Value)
	assert.Equal(t, Revision(7), entry.Revision)
	assert.Equal(t, now, entry.UpdatedAt)
	assert.Equal(t, "admin-1", entry.UpdatedBy)
	assert.Equal(t, "api", entry.Source)
}

func TestEntry_ZeroValue(t *testing.T) {
	t.Parallel()

	var entry Entry

	assert.Equal(t, "", entry.Key)
	assert.Nil(t, entry.Value)
	assert.True(t, entry.UpdatedAt.IsZero())
	assert.Equal(t, "", entry.UpdatedBy)
	assert.Equal(t, "", entry.Source)
}

func TestEntry_AnyValueTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{name: "string value", value: "hello"},
		{name: "int value", value: 42},
		{name: "bool value", value: true},
		{name: "float value", value: 3.14},
		{name: "nil value", value: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			entry := Entry{
				Key:   "test.key",
				Value: tt.value,
			}

			assert.Equal(t, tt.value, entry.Value)
		})
	}
}
