//go:build unit

package ports_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestM2MCredentials_StructTags(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeOf(ports.M2MCredentials{})

	// Verify ClientID field has json:"-" tag (redacted from serialization)
	clientIDField, ok := typ.FieldByName("ClientID")
	require.True(t, ok, "M2MCredentials should have a ClientID field")

	clientIDTag := clientIDField.Tag.Get("json")
	assert.Equal(t, "-", clientIDTag,
		"ClientID should have json:\"-\" tag to prevent serialization")

	// Verify ClientSecret field has json:"-" tag (redacted from serialization)
	clientSecretField, ok := typ.FieldByName("ClientSecret")
	require.True(t, ok, "M2MCredentials should have a ClientSecret field")

	clientSecretTag := clientSecretField.Tag.Get("json")
	assert.True(t, strings.HasPrefix(clientSecretTag, "-"),
		"ClientSecret should have json:\"-\" tag to prevent serialization")
}
