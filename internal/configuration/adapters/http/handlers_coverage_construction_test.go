// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"database/sql"
	"errors"
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/services/command"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// ─── NewHandler tests ─────────────────────────────────────────

func TestNewHandler_NilCommandUseCase(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(nil, queryUseCase, contextRepo, sourceRepo, matchRuleRepo, fieldMapRepo, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilCommandUseCase)
}

func TestNewHandler_NilQueryUseCase(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, nil, contextRepo, sourceRepo, matchRuleRepo, fieldMapRepo, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilQueryUseCase)
}

func TestNewHandler_NilContextRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, nil, sourceRepo, matchRuleRepo, fieldMapRepo, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilContextRepository)
}

func TestNewHandler_NilSourceRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, nil, matchRuleRepo, fieldMapRepo, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilSourceRepository)
}

func TestNewHandler_NilMatchRuleRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, sourceRepo, nil, fieldMapRepo, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilMatchRuleRepository)
}

func TestNewHandler_NilFieldMapRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, sourceRepo, matchRuleRepo, nil, feeRuleRepo, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilFieldMapRepository)
}

func TestNewHandler_NilFeeRuleRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, sourceRepo, matchRuleRepo, fieldMapRepo, nil, feeScheduleRepo, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilFeeRuleRepository)
}

func TestNewHandler_NilFeeScheduleRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	scheduleRepo := newScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, sourceRepo, matchRuleRepo, fieldMapRepo, feeRuleRepo, nil, scheduleRepo, false)
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestNewHandler_NilScheduleRepo(t *testing.T) {
	t.Parallel()

	contextRepo := newContextRepository()
	sourceRepo := newSourceRepository()
	fieldMapRepo := newFieldMapRepository()
	matchRuleRepo := newMatchRuleRepository()
	feeRuleRepo := newFeeRuleRepository()
	feeScheduleRepo := newFeeScheduleRepository()

	commandUseCase, err := command.NewUseCase(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	queryUseCase, err := newQueryUseCaseForTest(contextRepo, sourceRepo, fieldMapRepo, matchRuleRepo)
	require.NoError(t, err)

	_, err = NewHandler(commandUseCase, queryUseCase, contextRepo, sourceRepo, matchRuleRepo, fieldMapRepo, feeRuleRepo, feeScheduleRepo, nil, false)
	require.ErrorIs(t, err, ErrNilScheduleRepository)
}

// ─── safeClientMessage tests ──────────────────────────────────

func TestSafeClientMessage_NilError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", nil)
	assert.Equal(t, "default message", result)
}

func TestSafeClientMessage_UnsafeError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", errors.New("internal problem"))
	assert.Equal(t, "default message", result)
}

func TestSafeClientMessage_SafeError(t *testing.T) {
	t.Parallel()

	result := safeClientMessage("default message", entities.ErrContextNameRequired)
	assert.Equal(t, entities.ErrContextNameRequired.Error(), result)
}

// ─── writeServiceError tests ──────────────────────────────────

func TestWriteServiceError_SafeClientError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return writeServiceError(c, entities.ErrContextNameRequired)
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusBadRequest, resp.StatusCode)
}

func TestWriteServiceError_InternalError(t *testing.T) {
	t.Parallel()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		return writeServiceError(c, errors.New("some infrastructure error"))
	})

	resp := performRequest(t, app, http.MethodGet, "/test", nil)
	defer resp.Body.Close()

	assert.Equal(t, fiber.StatusInternalServerError, resp.StatusCode)
}

// ─── isClientSafeError tests ──────────────────────────────────

func TestIsClientSafeError_Comprehensive(t *testing.T) {
	t.Parallel()

	safeErrors := []error{
		dto.ErrDeprecatedRateID,
		entities.ErrNilReconciliationContext,
		entities.ErrContextNameRequired,
		entities.ErrContextNameTooLong,
		entities.ErrContextTypeInvalid,
		entities.ErrContextStatusInvalid,
		entities.ErrContextIntervalRequired,
		entities.ErrContextTenantRequired,
		entities.ErrSourceNameRequired,
		entities.ErrSourceNameTooLong,
		entities.ErrSourceTypeInvalid,
		entities.ErrSourceContextRequired,
		shared.ErrFieldMapNil,
		shared.ErrFieldMapContextRequired,
		shared.ErrFieldMapSourceRequired,
		shared.ErrFieldMapMappingRequired,
		shared.ErrFieldMapMappingValueEmpty,
		entities.ErrMatchRuleNil,
		entities.ErrRuleContextRequired,
		entities.ErrRulePriorityInvalid,
		entities.ErrRuleTypeInvalid,
		entities.ErrRuleConfigRequired,
		entities.ErrRuleConfigMissingRequiredKeys,
		entities.ErrRulePriorityConflict,
	}

	for _, safeErr := range safeErrors {
		assert.True(t, isClientSafeError(safeErr), "expected %v to be client safe", safeErr)
	}

	assert.False(t, isClientSafeError(errors.New("random error")))
	assert.False(t, isClientSafeError(sql.ErrNoRows))
}
