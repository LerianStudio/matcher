package postgres

import (
	sharedOutbox "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/outbox"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// Repository implements outbox event persistence using PostgreSQL.
type Repository = sharedOutbox.Repository

// NewRepository creates a new outbox Repository with the given provider.
func NewRepository(provider ports.InfrastructureProvider) *Repository {
	return sharedOutbox.NewRepository(provider)
}
