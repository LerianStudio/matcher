//go:build unit

package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestTypes_CompileCheck(t *testing.T) {
	t.Parallel()

	// Verify all request DTO types are instantiable.
	// These are currently empty structs reserved for future extensibility.
	refresh := RefreshDiscoveryRequest{}
	test := TestConnectionRequest{}

	assert.IsType(t, RefreshDiscoveryRequest{}, refresh)
	assert.IsType(t, TestConnectionRequest{}, test)
}
