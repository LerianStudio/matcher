package command

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func classifySources(
	contextType shared.ContextType,
	sources []*ports.SourceInfo,
) (map[uuid.UUID]struct{}, map[uuid.UUID]struct{}, error) {
	leftSourceIDs := make(map[uuid.UUID]struct{})
	rightSourceIDs := make(map[uuid.UUID]struct{})

	nonNil := make([]*ports.SourceInfo, 0, len(sources))
	for _, source := range sources {
		if source != nil {
			nonNil = append(nonNil, source)
		}
	}

	if len(nonNil) < 2 { //nolint:mnd // minimum 2 sources for matching
		return nil, nil, ErrAtLeastTwoSourcesRequired
	}

	for _, source := range nonNil {
		if !source.Side.IsExclusive() {
			return nil, nil, ErrSourceSideRequiredForMatching
		}

		if source.Side == fee.MatchingSideLeft {
			leftSourceIDs[source.ID] = struct{}{}
		} else {
			rightSourceIDs[source.ID] = struct{}{}
		}
	}

	if err := validateSourceCountForContextType(contextType, len(leftSourceIDs), len(rightSourceIDs)); err != nil {
		return nil, nil, err
	}

	return leftSourceIDs, rightSourceIDs, nil
}

func validateSourceCountForContextType(contextType shared.ContextType, leftCount, rightCount int) error {
	switch contextType {
	case shared.ContextTypeOneToOne:
		if leftCount != 1 {
			return ErrOneToOneRequiresExactlyOneLeftSource
		}

		if rightCount != 1 {
			return ErrOneToOneRequiresExactlyOneRightSource
		}
	case shared.ContextTypeOneToMany:
		if leftCount != 1 {
			return ErrOneToManyRequiresExactlyOneLeftSource
		}

		if rightCount == 0 {
			return ErrAtLeastOneRightSourceRequired
		}
	case shared.ContextTypeManyToMany:
		return fmt.Errorf("%w: %s", ErrUnsupportedContextType, contextType)
	}

	return nil
}
