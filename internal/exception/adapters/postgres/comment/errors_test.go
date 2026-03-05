//go:build unit

package comment

import (
	"errors"
	"testing"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
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

func TestErrTransactionRequired_CanonicalIdentity(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrTransactionRequired, pgcommon.ErrTransactionRequired))
}
