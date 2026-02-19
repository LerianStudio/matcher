//go:build unit

package ports_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
)

func TestFXSource_InterfaceDefinition(t *testing.T) {
	t.Parallel()

	var fxSource ports.FXSource
	require.Nil(t, fxSource)
}
