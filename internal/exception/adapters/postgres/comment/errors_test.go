//go:build unit

package comment

import (
	"testing"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/stretchr/testify/assert"
)

func TestErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	assert.NotErrorIs(t, ErrRepoNotInitialized, ErrCommentNil)
	assert.NotErrorIs(t, ErrRepoNotInitialized, ErrCommentNotFound)
	assert.NotErrorIs(t, ErrCommentNil, ErrCommentNotFound)
}

func TestErrCommentNotFound_MatchesDomain(t *testing.T) {
	t.Parallel()

	assert.ErrorIs(t, ErrCommentNotFound, entities.ErrCommentNotFound)
}
