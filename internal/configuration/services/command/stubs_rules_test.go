// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedfee "github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

type matchRuleRepoStub struct {
	createFn                 func(context.Context, *entities.MatchRule) (*entities.MatchRule, error)
	findByIDFn               func(context.Context, uuid.UUID, uuid.UUID) (*entities.MatchRule, error)
	findByContextIDFn        func(context.Context, uuid.UUID, string, int) (entities.MatchRules, libHTTP.CursorPagination, error)
	findByContextIDAndTypeFn func(context.Context, uuid.UUID, shared.RuleType, string, int) (entities.MatchRules, libHTTP.CursorPagination, error)
	findByPriorityFn         func(context.Context, uuid.UUID, int) (*entities.MatchRule, error)
	updateFn                 func(context.Context, *entities.MatchRule) (*entities.MatchRule, error)
	deleteFn                 func(context.Context, uuid.UUID, uuid.UUID) error
	reorderFn                func(context.Context, uuid.UUID, []uuid.UUID) error
}

type feeRuleRepoStub struct {
	findByContextIDFn func(context.Context, uuid.UUID) ([]*sharedfee.FeeRule, error)
}

func (stub *feeRuleRepoStub) Create(_ context.Context, _ *sharedfee.FeeRule) error {
	return errCreateNotImplemented
}

func (stub *feeRuleRepoStub) CreateWithTx(_ context.Context, _ *sql.Tx, _ *sharedfee.FeeRule) error {
	return errCreateNotImplemented
}

func (stub *feeRuleRepoStub) FindByID(_ context.Context, _ uuid.UUID) (*sharedfee.FeeRule, error) {
	return nil, errFindByIDNotImplemented
}

func (stub *feeRuleRepoStub) FindByContextID(ctx context.Context, contextID uuid.UUID) ([]*sharedfee.FeeRule, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID)
	}

	return nil, errFindFeeRulesNotImplemented
}

func (stub *feeRuleRepoStub) Update(_ context.Context, _ *sharedfee.FeeRule) error {
	return errUpdateNotImplemented
}

func (stub *feeRuleRepoStub) UpdateWithTx(_ context.Context, _ *sql.Tx, _ *sharedfee.FeeRule) error {
	return errUpdateNotImplemented
}

func (stub *feeRuleRepoStub) Delete(_ context.Context, _, _ uuid.UUID) error {
	return errDeleteNotImplemented
}

func (stub *feeRuleRepoStub) DeleteWithTx(_ context.Context, _ *sql.Tx, _, _ uuid.UUID) error {
	return errDeleteNotImplemented
}

func (stub *matchRuleRepoStub) Create(
	ctx context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *matchRuleRepoStub) FindByID(
	ctx context.Context,
	contextID, identifier uuid.UUID,
) (*entities.MatchRule, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, contextID, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *matchRuleRepoStub) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

func (stub *matchRuleRepoStub) FindByContextIDAndType(
	ctx context.Context,
	contextID uuid.UUID,
	ruleType shared.RuleType,
	cursor string,
	limit int,
) (entities.MatchRules, libHTTP.CursorPagination, error) {
	if stub.findByContextIDAndTypeFn != nil {
		return stub.findByContextIDAndTypeFn(ctx, contextID, ruleType, cursor, limit)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextTypeNotImplemented
}

func (stub *matchRuleRepoStub) FindByPriority(
	ctx context.Context,
	contextID uuid.UUID,
	priority int,
) (*entities.MatchRule, error) {
	if stub.findByPriorityFn != nil {
		return stub.findByPriorityFn(ctx, contextID, priority)
	}

	return nil, errFindByPriorityNotImplemented
}

func (stub *matchRuleRepoStub) Update(
	ctx context.Context,
	entity *entities.MatchRule,
) (*entities.MatchRule, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *matchRuleRepoStub) Delete(ctx context.Context, contextID, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, contextID, identifier)
	}

	return errDeleteNotImplemented
}

func (stub *matchRuleRepoStub) ReorderPriorities(
	ctx context.Context,
	contextID uuid.UUID,
	ruleIDs []uuid.UUID,
) error {
	if stub.reorderFn != nil {
		return stub.reorderFn(ctx, contextID, ruleIDs)
	}

	return errReorderNotImplemented
}
