// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"

	"github.com/google/uuid"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Sentinel errors for stub repositories.
var (
	errCreateFailed                    = errors.New("create failed")
	errCreateNotImplemented            = errors.New("create not implemented")
	errFindByIDNotImplemented          = errors.New("find by id not implemented")
	errFindByNameNotImplemented        = errors.New("find by name not implemented")
	errFindAllNotImplemented           = errors.New("find all not implemented")
	errUpdateNotImplemented            = errors.New("update not implemented")
	errDeleteNotImplemented            = errors.New("delete not implemented")
	errCountNotImplemented             = errors.New("count not implemented")
	errFindByContextNotImplemented     = errors.New("find by context not implemented")
	errFindByContextTypeNotImplemented = errors.New("find by context and type not implemented")
	errFindBySourceNotImplemented      = errors.New("find by source not implemented")
	errFindByPriorityNotImplemented    = errors.New("find by priority not implemented")
	errReorderNotImplemented           = errors.New("reorder not implemented")
	errFindFeeRulesNotImplemented      = errors.New("find fee rules not implemented")
)

type contextRepoStub struct {
	createFn     func(context.Context, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	findByIDFn   func(context.Context, uuid.UUID) (*entities.ReconciliationContext, error)
	findByNameFn func(context.Context, string) (*entities.ReconciliationContext, error)
	updateFn     func(context.Context, *entities.ReconciliationContext) (*entities.ReconciliationContext, error)
	deleteFn     func(context.Context, uuid.UUID) error
}

func (stub *contextRepoStub) Create(
	ctx context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *contextRepoStub) FindByID(
	ctx context.Context,
	identifier uuid.UUID,
) (*entities.ReconciliationContext, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *contextRepoStub) FindByName(
	ctx context.Context,
	name string,
) (*entities.ReconciliationContext, error) {
	if stub.findByNameFn != nil {
		return stub.findByNameFn(ctx, name)
	}

	return nil, errFindByNameNotImplemented
}

func (stub *contextRepoStub) FindAll(
	_ context.Context,
	_ string,
	_ int,
	_ *shared.ContextType,
	_ *value_objects.ContextStatus,
) ([]*entities.ReconciliationContext, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, errFindAllNotImplemented
}

func (stub *contextRepoStub) Update(
	ctx context.Context,
	entity *entities.ReconciliationContext,
) (*entities.ReconciliationContext, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *contextRepoStub) Delete(ctx context.Context, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, identifier)
	}

	return errDeleteNotImplemented
}

func (stub *contextRepoStub) Count(_ context.Context) (int64, error) {
	return 0, errCountNotImplemented
}

type sourceRepoStub struct {
	createFn                 func(context.Context, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	findByIDFn               func(context.Context, uuid.UUID, uuid.UUID) (*entities.ReconciliationSource, error)
	findByContextIDFn        func(context.Context, uuid.UUID) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	findByContextIDAndTypeFn func(context.Context, uuid.UUID, value_objects.SourceType) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error)
	updateFn                 func(context.Context, *entities.ReconciliationSource) (*entities.ReconciliationSource, error)
	deleteFn                 func(context.Context, uuid.UUID, uuid.UUID) error
}

func (stub *sourceRepoStub) Create(
	ctx context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *sourceRepoStub) FindByID(
	ctx context.Context,
	contextID, identifier uuid.UUID,
) (*entities.ReconciliationSource, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, contextID, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *sourceRepoStub) FindByContextID(
	ctx context.Context,
	contextID uuid.UUID,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if stub.findByContextIDFn != nil {
		return stub.findByContextIDFn(ctx, contextID)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextNotImplemented
}

func (stub *sourceRepoStub) FindByContextIDAndType(
	ctx context.Context,
	contextID uuid.UUID,
	sourceType value_objects.SourceType,
	_ string,
	_ int,
) ([]*entities.ReconciliationSource, libHTTP.CursorPagination, error) {
	if stub.findByContextIDAndTypeFn != nil {
		return stub.findByContextIDAndTypeFn(ctx, contextID, sourceType)
	}

	return nil, libHTTP.CursorPagination{}, errFindByContextTypeNotImplemented
}

func (stub *sourceRepoStub) Update(
	ctx context.Context,
	entity *entities.ReconciliationSource,
) (*entities.ReconciliationSource, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *sourceRepoStub) Delete(ctx context.Context, contextID, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, contextID, identifier)
	}

	return errDeleteNotImplemented
}

type fieldMapRepoStub struct {
	createFn         func(context.Context, *shared.FieldMap) (*shared.FieldMap, error)
	findByIDFn       func(context.Context, uuid.UUID) (*shared.FieldMap, error)
	findBySourceIDFn func(context.Context, uuid.UUID) (*shared.FieldMap, error)
	updateFn         func(context.Context, *shared.FieldMap) (*shared.FieldMap, error)
	deleteFn         func(context.Context, uuid.UUID) error
}

func (stub *fieldMapRepoStub) Create(
	ctx context.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if stub.createFn != nil {
		return stub.createFn(ctx, entity)
	}

	return nil, errCreateNotImplemented
}

func (stub *fieldMapRepoStub) FindByID(
	ctx context.Context,
	identifier uuid.UUID,
) (*shared.FieldMap, error) {
	if stub.findByIDFn != nil {
		return stub.findByIDFn(ctx, identifier)
	}

	return nil, errFindByIDNotImplemented
}

func (stub *fieldMapRepoStub) FindBySourceID(
	ctx context.Context,
	sourceID uuid.UUID,
) (*shared.FieldMap, error) {
	if stub.findBySourceIDFn != nil {
		return stub.findBySourceIDFn(ctx, sourceID)
	}

	return nil, errFindBySourceNotImplemented
}

func (stub *fieldMapRepoStub) Update(
	ctx context.Context,
	entity *shared.FieldMap,
) (*shared.FieldMap, error) {
	if stub.updateFn != nil {
		return stub.updateFn(ctx, entity)
	}

	return nil, errUpdateNotImplemented
}

func (stub *fieldMapRepoStub) ExistsBySourceIDs(
	_ context.Context,
	sourceIDs []uuid.UUID,
) (map[uuid.UUID]bool, error) {
	return make(map[uuid.UUID]bool, len(sourceIDs)), nil
}

func (stub *fieldMapRepoStub) Delete(ctx context.Context, identifier uuid.UUID) error {
	if stub.deleteFn != nil {
		return stub.deleteFn(ctx, identifier)
	}

	return errDeleteNotImplemented
}
