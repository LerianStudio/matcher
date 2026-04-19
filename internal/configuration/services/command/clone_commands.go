package command

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const maxClonePaginationLimit = 200

// ErrCloneNameRequired indicates the new context name was not provided for the clone operation.
var ErrCloneNameRequired = errors.New("new context name is required for clone")

// ErrCloneProviderRequired indicates the repository does not support transactional create for clone.
var ErrCloneProviderRequired = errors.New("repository does not support transactional create for clone")

type (
	// Transactional create interfaces for clone write operations.
	contextTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	}
	sourceTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	}
	fieldMapTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.FieldMap) (*entities.FieldMap, error)
	}
	matchRuleTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, entity *entities.MatchRule) (*entities.MatchRule, error)
	}
	feeRuleTxCreator interface {
		CreateWithTx(ctx context.Context, tx *sql.Tx, rule *fee.FeeRule) error
	}

	// Transactional read interfaces for clone snapshot consistency.
	// These enable child-record reads within the same transaction that
	// holds the FOR SHARE lock, preventing inconsistent snapshots.
	sourceTxFinder interface {
		FindByContextIDWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID, cursor string, limit int) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	}
	fieldMapTxExistsChecker interface {
		ExistsBySourceIDsWithTx(ctx context.Context, tx *sql.Tx, sourceIDs []uuid.UUID) (map[uuid.UUID]bool, error)
	}
	fieldMapTxFinder interface {
		FindBySourceIDWithTx(ctx context.Context, tx *sql.Tx, sourceID uuid.UUID) (*entities.FieldMap, error)
	}
	matchRuleTxFinder interface {
		FindByContextIDWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID, cursor string, limit int) (entities.MatchRules, libHTTP.CursorPagination, error)
	}
	contextTxFinder interface {
		FindByIDWithTx(ctx context.Context, tx *sql.Tx, id uuid.UUID) (*entities.ReconciliationContext, error)
	}
	feeRuleTxFinder interface {
		FindByContextIDWithTx(ctx context.Context, tx *sql.Tx, contextID uuid.UUID) ([]*fee.FeeRule, error)
	}
)

// WithInfrastructureProvider returns a UseCaseOption that sets the infrastructure provider for transactional clone operations.
func WithInfrastructureProvider(provider sharedPorts.InfrastructureProvider) UseCaseOption {
	return func(uc *UseCase) {
		if provider != nil {
			uc.infraProvider = provider
		}
	}
}

// CloneContextInput holds the parameters required to clone a reconciliation context.
type CloneContextInput struct {
	SourceContextID uuid.UUID
	NewName         string
	IncludeSources  bool
	IncludeRules    bool
}

// CloneContext creates a deep copy of a reconciliation context with its associated resources.
func (uc *UseCase) CloneContext(ctx context.Context, input CloneContextInput) (*entities.CloneResult, error) {
	if err := uc.validateCloneDependencies(input); err != nil {
		return nil, err
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "command.clone_context")
	defer span.End()

	sourceContext, err := uc.contextRepo.FindByID(ctx, input.SourceContextID)
	if err != nil {
		wrappedErr := fmt.Errorf("loading source context: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to load source context", wrappedErr)

		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to load source context")

		return nil, wrappedErr
	}

	if sourceContext == nil {
		wrappedErr := fmt.Errorf("loading source context: %w", ErrContextNotFound)
		libOpentelemetry.HandleSpanBusinessErrorEvent(span, "source context not found", wrappedErr)

		return nil, wrappedErr
	}

	autoMatchOnUpload := sourceContext.AutoMatchOnUpload

	if uc.infraProvider != nil {
		result, txErr := uc.cloneContextTransactional(ctx, input, sourceContext)
		if txErr != nil {
			libOpentelemetry.HandleSpanError(span, "failed to clone context (transactional)", txErr)

			logger.With(libLog.Err(txErr)).Log(ctx, libLog.LevelError, "failed to clone context (transactional)")

			return nil, txErr
		}

		uc.publishCloneAudit(ctx, input, result)

		return result, nil
	}

	logger.Log(ctx, libLog.LevelWarn, "clone operation running without transactional guarantee; set InfrastructureProvider via WithInfrastructureProvider to enable atomic clones")

	result, cloneErr := uc.cloneContextNonTransactional(ctx, input, sourceContext, autoMatchOnUpload)
	if cloneErr != nil {
		libOpentelemetry.HandleSpanError(span, "failed to clone context", cloneErr)

		logger.With(libLog.Err(cloneErr)).Log(ctx, libLog.LevelError, "failed to clone context")

		return nil, cloneErr
	}

	uc.publishCloneAudit(ctx, input, result)

	return result, nil
}

func (uc *UseCase) validateCloneDependencies(input CloneContextInput) error {
	if uc == nil || uc.contextRepo == nil {
		return ErrNilContextRepository
	}

	if input.IncludeSources && uc.sourceRepo == nil {
		return ErrNilSourceRepository
	}

	if input.IncludeSources && uc.fieldMapRepo == nil {
		return ErrNilFieldMapRepository
	}

	if input.IncludeRules && uc.matchRuleRepo == nil {
		return ErrNilMatchRuleRepository
	}

	if input.IncludeRules && uc.feeRuleRepo == nil {
		return ErrNilFeeRuleRepository
	}

	if strings.TrimSpace(input.NewName) == "" {
		return ErrCloneNameRequired
	}

	return nil
}

func (uc *UseCase) publishCloneAudit(ctx context.Context, input CloneContextInput, result *entities.CloneResult) {
	uc.publishAudit(ctx, "context", result.Context.ID, "clone", map[string]any{
		"source_context_id": input.SourceContextID.String(),
		"name":              result.Context.Name,
		"sources_cloned":    result.SourcesCloned,
		"rules_cloned":      result.RulesCloned,
		"fee_rules_cloned":  result.FeeRulesCloned,
		"field_maps_cloned": result.FieldMapsCloned,
	})
}
