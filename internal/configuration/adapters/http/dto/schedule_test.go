// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
)

func TestScheduleToResponse_NilInput(t *testing.T) {
	t.Parallel()

	resp := ScheduleToResponse(nil)

	assert.Empty(t, resp.ID)
	assert.Empty(t, resp.ContextID)
	assert.Empty(t, resp.CronExpression)
	assert.False(t, resp.Enabled)
	assert.Nil(t, resp.LastRunAt)
	assert.Nil(t, resp.NextRunAt)
	assert.Empty(t, resp.CreatedAt)
	assert.Empty(t, resp.UpdatedAt)
}

func TestScheduleToResponse_ValidInput(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	scheduleID := uuid.New()
	contextID := uuid.New()

	schedule := &entities.ReconciliationSchedule{
		ID:             scheduleID,
		ContextID:      contextID,
		CronExpression: "0 0 * * *",
		Enabled:        true,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	resp := ScheduleToResponse(schedule)

	assert.Equal(t, scheduleID.String(), resp.ID)
	assert.Equal(t, contextID.String(), resp.ContextID)
	assert.Equal(t, "0 0 * * *", resp.CronExpression)
	assert.True(t, resp.Enabled)
	assert.Equal(t, now.Format(time.RFC3339), resp.CreatedAt)
	assert.Equal(t, now.Format(time.RFC3339), resp.UpdatedAt)
}

func TestScheduleToResponse_WithOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(23 * time.Hour)

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 6 * * *",
		Enabled:        true,
		LastRunAt:      &lastRun,
		NextRunAt:      &nextRun,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	resp := ScheduleToResponse(schedule)

	require.NotNil(t, resp.LastRunAt)
	require.NotNil(t, resp.NextRunAt)
	assert.Equal(t, lastRun.Format(time.RFC3339), *resp.LastRunAt)
	assert.Equal(t, nextRun.Format(time.RFC3339), *resp.NextRunAt)
}

func TestScheduleToResponse_WithoutOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	schedule := &entities.ReconciliationSchedule{
		ID:             uuid.New(),
		ContextID:      uuid.New(),
		CronExpression: "0 0 * * *",
		Enabled:        false,
		LastRunAt:      nil,
		NextRunAt:      nil,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	resp := ScheduleToResponse(schedule)

	assert.Nil(t, resp.LastRunAt)
	assert.Nil(t, resp.NextRunAt)
}

func TestSchedulesToResponse_EmptySlice(t *testing.T) {
	t.Parallel()

	resp := SchedulesToResponse(nil)

	assert.NotNil(t, resp)
	assert.Empty(t, resp)

	resp = SchedulesToResponse([]*entities.ReconciliationSchedule{})

	assert.NotNil(t, resp)
	assert.Empty(t, resp)
}

func TestSchedulesToResponse_SkipsNilEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	schedules := []*entities.ReconciliationSchedule{
		{
			ID:             uuid.New(),
			ContextID:      uuid.New(),
			CronExpression: "0 0 * * *",
			Enabled:        true,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		nil,
		{
			ID:             uuid.New(),
			ContextID:      uuid.New(),
			CronExpression: "0 6 * * *",
			Enabled:        false,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		nil,
	}

	resp := SchedulesToResponse(schedules)

	assert.Len(t, resp, 2)
	assert.Equal(t, "0 0 * * *", resp[0].CronExpression)
	assert.Equal(t, "0 6 * * *", resp[1].CronExpression)
}

func TestSchedulesToResponse_MultipleEntries(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	lastRun := now.Add(-2 * time.Hour)
	nextRun := now.Add(22 * time.Hour)

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	schedules := []*entities.ReconciliationSchedule{
		{
			ID:             id1,
			ContextID:      uuid.New(),
			CronExpression: "0 0 * * *",
			Enabled:        true,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			ID:             id2,
			ContextID:      uuid.New(),
			CronExpression: "0 6 * * MON-FRI",
			Enabled:        false,
			LastRunAt:      &lastRun,
			NextRunAt:      nil,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			ID:             id3,
			ContextID:      uuid.New(),
			CronExpression: "*/15 * * * *",
			Enabled:        true,
			LastRunAt:      &lastRun,
			NextRunAt:      &nextRun,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}

	resp := SchedulesToResponse(schedules)

	require.Len(t, resp, 3)
	assert.Equal(t, id1.String(), resp[0].ID)
	assert.Equal(t, id2.String(), resp[1].ID)
	assert.Equal(t, id3.String(), resp[2].ID)
	assert.True(t, resp[0].Enabled)
	assert.False(t, resp[1].Enabled)
	assert.True(t, resp[2].Enabled)
	assert.Nil(t, resp[0].LastRunAt)
	require.NotNil(t, resp[1].LastRunAt)
	assert.Nil(t, resp[1].NextRunAt)
	require.NotNil(t, resp[2].LastRunAt)
	require.NotNil(t, resp[2].NextRunAt)
}
