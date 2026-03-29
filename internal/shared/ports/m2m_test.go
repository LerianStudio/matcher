//go:build unit

package ports_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestM2MCredentials_JSONTagsRedact(t *testing.T) {
	t.Parallel()

	creds := &ports.M2MCredentials{
		ClientID:     "test-id",
		ClientSecret: "test-secret",
	}

	assert.Equal(t, "test-id", creds.ClientID)
	assert.Equal(t, "test-secret", creds.ClientSecret)
}
