//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestBuildSourceTypeMap_NilSources_ReturnsNil(t *testing.T) {
	t.Parallel()

	result := buildSourceTypeMap(nil)
	assert.Nil(t, result)
}

func TestBuildSourceTypeMap_EmptySources_ReturnsNil(t *testing.T) {
	t.Parallel()

	result := buildSourceTypeMap([]*ports.SourceInfo{})
	assert.Nil(t, result)
}

func TestBuildSourceTypeMap_SkipsNil(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000240001")
	sources := []*ports.SourceInfo{
		nil,
		{ID: id, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		nil,
	}
	result := buildSourceTypeMap(sources)
	require.Len(t, result, 1)
	assert.Equal(t, string(ports.SourceTypeLedger), result[id])
}

func TestBuildSourceTypeMap_MultipleSources(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000240010")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000240011")
	sources := []*ports.SourceInfo{
		{ID: id1, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: id2, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}
	result := buildSourceTypeMap(sources)
	require.Len(t, result, 2)
	assert.Equal(t, string(ports.SourceTypeLedger), result[id1])
	assert.Equal(t, string(ports.SourceTypeFile), result[id2])
}

func TestLoadFeeRulesAndSchedules_NoRules_Returns_NilSlices(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: nil},
	}

	leftRules, rightRules, schedules, err := uc.loadFeeRulesAndSchedules(context.Background(), uuid.New())
	require.NoError(t, err)
	assert.Nil(t, leftRules)
	assert.Nil(t, rightRules)
	assert.Nil(t, schedules)
}

func TestLoadFeeRulesAndSchedules_ProviderError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{err: errors.New("provider error")},
	}

	leftRules, rightRules, schedules, err := uc.loadFeeRulesAndSchedules(context.Background(), uuid.New())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading fee rules")
	assert.Nil(t, leftRules)
	assert.Nil(t, rightRules)
	assert.Nil(t, schedules)
}

func TestLoadFeeRulesAndSchedules_NilFeeScheduleRepo(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000240020")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000240021")

	rules := []*fee.FeeRule{
		{
			ID:            uuid.New(),
			ContextID:     contextID,
			Side:          fee.MatchingSideAny,
			FeeScheduleID: scheduleID,
			Name:          "test",
			Priority:      1,
		},
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: nil,
	}

	_, _, _, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.ErrorIs(t, err, ErrNilFeeScheduleRepository)
}

func TestLoadFeeRulesAndSchedules_ScheduleLoadError(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000240030")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000240031")

	rules := []*fee.FeeRule{
		{
			ID:            uuid.New(),
			ContextID:     contextID,
			Side:          fee.MatchingSideLeft,
			FeeScheduleID: scheduleID,
			Name:          "test",
			Priority:      1,
		},
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			err: errors.New("db error"),
		},
	}

	_, _, _, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading fee schedules")
}

func TestLoadFeeRulesAndSchedules_MissingScheduleReference(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000240040")
	missingScheduleID := uuid.MustParse("00000000-0000-0000-0000-000000240041")

	rules := []*fee.FeeRule{{
		ID:            uuid.New(),
		ContextID:     contextID,
		Side:          fee.MatchingSideAny,
		FeeScheduleID: missingScheduleID,
		Name:          "Missing schedule",
		Priority:      1,
	}}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			schedules: map[uuid.UUID]*fee.FeeSchedule{},
		},
	}

	_, _, _, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFeeRulesReferenceMissingSchedules)
	assert.Contains(t, err.Error(), "1 missing references")
}

func TestLoadFeeRulesAndSchedules_RulesExceedMax(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	rules := make([]*fee.FeeRule, fee.MaxFeeRulesPerContext+1)
	for i := range rules {
		rules[i] = &fee.FeeRule{
			ID:            uuid.New(),
			ContextID:     contextID,
			Side:          fee.MatchingSideLeft,
			FeeScheduleID: uuid.New(),
			Name:          "rule",
			Priority:      i,
		}
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
	}

	_, _, _, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.ErrorIs(t, err, fee.ErrFeeRuleCountLimitExceeded)
}

func TestLoadFeeRulesAndSchedules_SortsRulesByPriority(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000240050")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000240051")

	schedule := &fee.FeeSchedule{ID: scheduleID, Currency: "USD"}
	rules := []*fee.FeeRule{
		{ID: uuid.New(), ContextID: contextID, Side: fee.MatchingSideLeft, FeeScheduleID: scheduleID, Name: "B", Priority: 3},
		{ID: uuid.New(), ContextID: contextID, Side: fee.MatchingSideLeft, FeeScheduleID: scheduleID, Name: "A", Priority: 1},
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
		},
	}

	leftRules, _, _, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.NoError(t, err)
	require.Len(t, leftRules, 2)
	assert.Equal(t, "A", leftRules[0].Name)
	assert.Equal(t, "B", leftRules[1].Name)
}

func TestLoadFeeRulesAndSchedules_Success_SplitsSides(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000240060")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000240061")
	schedule := &fee.FeeSchedule{ID: scheduleID, Currency: "USD"}

	rules := []*fee.FeeRule{
		{ID: uuid.New(), ContextID: contextID, Side: fee.MatchingSideLeft, FeeScheduleID: scheduleID, Name: "Left", Priority: 1},
		{ID: uuid.New(), ContextID: contextID, Side: fee.MatchingSideRight, FeeScheduleID: scheduleID, Name: "Right", Priority: 1},
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
		},
	}

	leftRules, rightRules, allSchedules, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.NoError(t, err)
	assert.Len(t, leftRules, 1)
	assert.Len(t, rightRules, 1)
	assert.NotNil(t, allSchedules[scheduleID])
}

func TestLoadFeeRulesAndSchedules_NilRulesSkippedInScheduleIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	scheduleID := uuid.New()
	schedule := &fee.FeeSchedule{ID: scheduleID, Currency: "USD"}

	rules := []*fee.FeeRule{
		nil,
		{ID: uuid.New(), ContextID: contextID, Side: fee.MatchingSideLeft, FeeScheduleID: scheduleID, Name: "Valid", Priority: 1},
		nil,
	}

	uc := &UseCase{
		feeRuleProvider: &stubFeeRuleProviderWithResult{rules: rules},
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
		},
	}

	leftRules, _, allSchedules, err := uc.loadFeeRulesAndSchedules(context.Background(), contextID)
	require.NoError(t, err)
	assert.Len(t, leftRules, 1)
	assert.NotNil(t, allSchedules[scheduleID])
}

func TestCompleteEmptyRun_SetsCorrectStats(t *testing.T) {
	t.Parallel()

	leftTx := []*shared.Transaction{{ID: uuid.New()}, {ID: uuid.New()}}
	rightTx := []*shared.Transaction{{ID: uuid.New()}}
	externalID := uuid.New()

	matchRunRepo := &stubMatchRunRepo{}
	exceptionCreator := &stubExceptionCreator{}

	uc := &UseCase{
		matchRunRepo:     matchRunRepo,
		exceptionCreator: exceptionCreator,
	}

	stats := make(map[string]int)

	run, groups, err := uc.completeEmptyRun(
		context.Background(),
		RunMatchInput{ContextID: uuid.New(), Mode: "dry_run"},
		stats,
		leftTx,
		rightTx,
		[]uuid.UUID{externalID},
		map[uuid.UUID]*shared.Transaction{externalID: {ID: externalID}},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, run)
	assert.Empty(t, groups)
	assert.Equal(t, 0, stats["matches"])
	assert.Equal(t, 2, stats["unmatched_left"])
	assert.Equal(t, 1, stats["unmatched_right"])
	assert.Equal(t, 1, stats["unmatched_external"])
}
