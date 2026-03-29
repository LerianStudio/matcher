//go:build unit

package m2m_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/shared/adapters/m2m"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestNewM2MCredentialProvider_Fields(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{ClientID: "id", ClientSecret: "secret"},
	}

	provider := m2m.NewM2MCredentialProvider(
		mock, "prod", "matcher", "fetcher",
		10*time.Minute, nil,
	)

	assert.NotNil(t, provider, "Provider should be constructed successfully")
}

func TestM2MCredentialProvider_ImplementsM2MProvider(t *testing.T) {
	t.Parallel()

	// Compile-time verification that M2MCredentialProvider implements ports.M2MProvider.
	var _ ports.M2MProvider = (*m2m.M2MCredentialProvider)(nil)
}
