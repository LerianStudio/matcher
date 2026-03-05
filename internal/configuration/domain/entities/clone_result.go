package entities

import (
	"context"
	"errors"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// ErrCloneResultContextRequired is returned when clone result context is nil.
var ErrCloneResultContextRequired = errors.New("cloned context is required")

// CloneResult holds the outcome of a context clone operation.
type CloneResult struct {
	Context            *ReconciliationContext
	SourcesCloned      int
	RulesCloned        int
	FieldMapsCloned    int
	FeeSchedulesCloned int
}

// NewCloneResult creates a clone result anchored to the cloned context.
func NewCloneResult(ctx context.Context, clonedContext *ReconciliationContext) (*CloneResult, error) {
	asserter := assert.New(ctx, nil, constants.ApplicationName, "configuration.clone_result.new")

	if err := asserter.NotNil(ctx, clonedContext, "cloned context is required"); err != nil {
		return nil, ErrCloneResultContextRequired
	}

	return &CloneResult{Context: clonedContext}, nil
}
