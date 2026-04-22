package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"

	configRepositories "github.com/LerianStudio/matcher/internal/configuration/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface satisfaction check.
var _ sharedPorts.ContextProvider = (*AutoMatchContextProviderAdapter)(nil)

// ErrNilContextRepository indicates the context repository dependency is
// required but was nil at construction time.
var ErrNilContextRepository = errors.New("context repository is required")

// AutoMatchContextProviderAdapter wraps a configuration ContextRepository
// to implement the ingestion ContextProvider port interface.
//
// Lives here (shared/adapters/cross) rather than in ingestion/adapters
// because ingestion cannot import configuration/domain/repositories per the
// cross-context depguard rules — the adapter IS the bridge. The work it
// does (FindByID → apply AutoMatchOnUpload && IsActive) is legitimate
// translation; collapsing it into ingestion would require widening the
// repo-interface deny and would leak configuration-specific business rules
// into the ingestion service.
//
// The sibling MatchTriggerAdapter was removed in T-004: the matching
// UseCase now implements sharedPorts.MatchTrigger directly (see
// matching/services/command/trigger_commands.go).
type AutoMatchContextProviderAdapter struct {
	repo configRepositories.ContextRepository
}

// NewAutoMatchContextProviderAdapter creates a new adapter.
func NewAutoMatchContextProviderAdapter(
	repo configRepositories.ContextRepository,
) (*AutoMatchContextProviderAdapter, error) {
	if repo == nil {
		return nil, ErrNilContextRepository
	}

	return &AutoMatchContextProviderAdapter{repo: repo}, nil
}

// IsAutoMatchEnabled checks whether auto-match on upload is enabled for the context.
func (adapter *AutoMatchContextProviderAdapter) IsAutoMatchEnabled(
	ctx context.Context,
	contextID uuid.UUID,
) (bool, error) {
	if adapter == nil || adapter.repo == nil {
		return false, ErrContextRepositoryRequired
	}

	ctxEntity, err := adapter.repo.FindByID(ctx, contextID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("check auto-match enabled: %w", err)
	}

	if ctxEntity == nil {
		return false, nil
	}

	return ctxEntity.AutoMatchOnUpload && ctxEntity.IsActive(), nil
}
