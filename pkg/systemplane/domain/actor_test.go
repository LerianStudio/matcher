//go:build unit

// Copyright 2025 Lerian Studio.

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActor_Construction(t *testing.T) {
	t.Parallel()

	actor := Actor{ID: "user-123"}

	assert.Equal(t, "user-123", actor.ID)
}

func TestActor_ZeroValue(t *testing.T) {
	t.Parallel()

	var actor Actor

	assert.Equal(t, "", actor.ID)
}
