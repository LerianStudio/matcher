// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	portsmocks "github.com/LerianStudio/matcher/internal/exception/ports/mocks"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Sentinel errors for dispatch testing.
var (
	errTestDispatchFind      = errors.New("test: dispatch find failed")
	errTestDispatchConnector = errors.New("test: connector dispatch failed")
	errTestDispatchAudit     = errors.New("test: dispatch audit failed")
)

func newDispatchMocks(
	t *testing.T,
) (*portsmocks.MockExceptionFinder, *portsmocks.MockExternalConnector) {
	t.Helper()

	ctrl := gomock.NewController(t)

	return portsmocks.NewMockExceptionFinder(ctrl), portsmocks.NewMockExternalConnector(ctrl)
}

func newTestDispatchException() *entities.Exception {
	reason := "Amount mismatch detected"

	return &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusOpen,
		Reason:        &reason,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
}

// NewDispatchUseCase Tests.
func TestNewDispatchUseCase_Success(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))

	require.NoError(t, err)
	require.NotNil(t, uc)
	// finder is wrapped by wrapFinder to satisfy the full ExceptionRepository
	// surface; the merged use case no longer exposes an exceptionFinder field.
	assert.NotNil(t, uc.exceptionRepo)
	assert.Equal(t, connector, uc.connector)
	assert.Equal(t, audit, uc.auditPublisher)
	assert.Equal(t, actor, uc.actorExtractor)
	assert.NotNil(t, uc.infraProvider)
}

func TestNewDispatchUseCase_NilExceptionFinder(t *testing.T) {
	t.Parallel()

	_, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(nil, actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

// TestNewDispatchUseCase_NilExternalConnector verifies the method-level
// validation that now owns the optional-dependency check: the merged
// constructor no longer rejects a nil connector (it is optional), so the
// caller discovers the missing dependency when invoking Dispatch.
func TestNewDispatchUseCase_NilExternalConnector(t *testing.T) {
	t.Parallel()

	finder, _ := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{})
	require.NoError(t, err)
	require.NotNil(t, uc)

	_, err = uc.Dispatch(context.Background(), DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "JIRA",
	})

	require.ErrorIs(t, err, ErrNilExternalConnector)
}

func TestNewDispatchUseCase_NilAuditPublisher(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, nil, &stubInfraProvider{}, WithExternalConnector(connector))

	require.ErrorIs(t, err, ErrNilAuditPublisher)
	assert.Nil(t, uc)
}

func TestNewDispatchUseCase_NilActorExtractor(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}

	uc, err := NewExceptionUseCase(wrapFinder(finder), nil, audit, &stubInfraProvider{}, WithExternalConnector(connector))

	require.ErrorIs(t, err, ErrNilActorExtractor)
	assert.Nil(t, uc)
}

func TestNewDispatchUseCase_NilInfraProvider(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, nil, WithExternalConnector(connector))

	require.ErrorIs(t, err, ErrNilInfraProvider)
	assert.Nil(t, uc)
}

func TestNewDispatchUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewExceptionUseCase(nil, nil, nil, nil, WithExternalConnector(nil))

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

// TestNewDispatchUseCase_ValidationOrder verifies the merged constructor
// validates its required dependencies (repo, actor, audit, infra) in the
// documented order. The external connector is optional and its nil check
// now lives on the Dispatch method — see
// TestNewDispatchUseCase_NilExternalConnector.
func TestNewDispatchUseCase_ValidationOrder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		nilFinder   bool
		nilAudit    bool
		nilActor    bool
		nilInfra    bool
		expectedErr error
	}{
		{
			name:        "nil finder first",
			nilFinder:   true,
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name:        "nil actor second",
			nilActor:    true,
			expectedErr: ErrNilActorExtractor,
		},
		{
			name:        "nil audit third",
			nilAudit:    true,
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name:        "nil infra provider fourth",
			nilInfra:    true,
			expectedErr: ErrNilInfraProvider,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockFinder, mockConnector := newDispatchMocks(t)

			var finder ports.ExceptionFinder = mockFinder

			var audit ports.AuditPublisher = &stubAuditPublisher{}

			var actor ports.ActorExtractor = actorExtractor("a")

			var infra sharedPorts.InfrastructureProvider = &stubInfraProvider{}

			if tc.nilFinder {
				finder = nil
			}

			if tc.nilAudit {
				audit = nil
			}

			if tc.nilActor {
				actor = nil
			}

			if tc.nilInfra {
				infra = nil
			}

			uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, infra, WithExternalConnector(mockConnector))

			require.ErrorIs(t, err, tc.expectedErr)
			assert.Nil(t, uc)
		})
	}
}

// Dispatch Tests.
func TestDispatch_Success(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().
		Dispatch(gomock.Any(), exception.ID.String(), gomock.Eq(services.RoutingDecision{
			Target: services.RoutingTargetJira,
			Queue:  "support-queue",
		}), gomock.Any()).
		Return(ports.DispatchResult{
			Target:            services.RoutingTargetJira,
			ExternalReference: "JIRA-12345",
			Acknowledged:      true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
		Queue:        "support-queue",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, exception.ID, result.ExceptionID)
	assert.Equal(t, "JIRA", result.Target)
	assert.Equal(t, "JIRA-12345", result.ExternalReference)
	assert.True(t, result.Acknowledged)
	assert.False(t, result.DispatchedAt.IsZero())

	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, exception.ID, audit.lastEvent.ExceptionID)
	assert.Equal(t, "DISPATCH", audit.lastEvent.Action)
	assert.Equal(t, "analyst@example.com", audit.lastEvent.Actor)
}

func TestDispatch_AllTargetSystems(t *testing.T) {
	t.Parallel()

	targets := []struct {
		name   string
		target string
		result services.RoutingTarget
	}{
		{name: "JIRA", target: "JIRA", result: services.RoutingTargetJira},
		{name: "SERVICENOW", target: "SERVICENOW", result: services.RoutingTargetServiceNow},
		{name: "WEBHOOK", target: "WEBHOOK", result: services.RoutingTargetWebhook},
		{name: "MANUAL", target: "MANUAL", result: services.RoutingTargetManual},
		{name: "lowercase jira", target: "jira", result: services.RoutingTargetJira},
		{
			name:   "mixed case ServiceNow",
			target: "ServiceNow",
			result: services.RoutingTargetServiceNow,
		},
	}

	for _, tc := range targets {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception := newTestDispatchException()
			finder, connector := newDispatchMocks(t)
			finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
			connector.EXPECT().
				Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
				Return(ports.DispatchResult{
					Target:       tc.result,
					Acknowledged: true,
				}, nil)

			audit := &stubAuditPublisher{}
			actor := actorExtractor("user@test.com")

			uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
			require.NoError(t, err)

			ctx := ctxWithActor("user@test.com")
			cmd := DispatchCommand{
				ExceptionID:  exception.ID,
				TargetSystem: tc.target,
			}

			result, err := uc.Dispatch(ctx, cmd)

			require.NoError(t, err)
			assert.Equal(t, string(tc.result), result.Target)
		})
	}
}

func TestDispatch_NilExceptionID(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  uuid.Nil,
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrExceptionIDRequired)
	assert.Nil(t, result)
}

func TestDispatch_EmptyTargetSystem(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrTargetSystemRequired)
	assert.Nil(t, result)
}

func TestDispatch_WhitespaceTargetSystem(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "   ",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrTargetSystemRequired)
	assert.Nil(t, result)
}

func TestDispatch_UnsupportedTargetSystem(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "UNKNOWN_SYSTEM",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrUnsupportedTargetSystem)
	assert.Contains(t, err.Error(), "UNKNOWN_SYSTEM")
	assert.Nil(t, result)
}

func TestDispatch_EmptyActor(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrActorRequired)
	assert.Nil(t, result)
}

func TestDispatch_WhitespaceActor(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	audit := &stubAuditPublisher{}
	actor := actorExtractor("   ")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("   ")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.ErrorIs(t, err, ErrActorRequired)
	assert.Nil(t, result)
}

func TestDispatch_ExceptionFinderError(t *testing.T) {
	t.Parallel()

	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), gomock.Any()).Return(nil, errTestDispatchFind)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  uuid.New(),
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.Error(t, err)
	require.ErrorIs(t, err, errTestDispatchFind)
	assert.Contains(t, err.Error(), "find exception")
	assert.Nil(t, result)
}

func TestDispatch_ConnectorError(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{}, errTestDispatchConnector)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.Error(t, err)
	require.ErrorIs(t, err, errTestDispatchConnector)
	assert.Contains(t, err.Error(), "dispatch to JIRA")
	assert.Nil(t, result)
}

func TestDispatch_AuditPublisherError(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetJira,
			Acknowledged: true,
		}, nil)

	audit := &stubAuditPublisher{err: errTestDispatchAudit}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.Error(t, err)
	require.ErrorIs(t, err, errTestDispatchAudit)
	assert.Contains(t, err.Error(), "publish audit")
	assert.Nil(t, result)
}

func TestDispatch_WithQueue(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetJira,
			Acknowledged: true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
		Queue:        "priority-queue",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, "priority-queue", audit.lastEvent.Metadata["queue"])
}

func TestDispatch_WithExternalReference(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:            services.RoutingTargetServiceNow,
			ExternalReference: "INC0012345",
			Acknowledged:      true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "SERVICENOW",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	assert.Equal(t, "INC0012345", result.ExternalReference)

	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, "INC0012345", audit.lastEvent.Metadata["external_reference"])
}

func TestDispatch_NotAcknowledged(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetWebhook,
			Acknowledged: false,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "WEBHOOK",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	assert.False(t, result.Acknowledged)
}

func TestDispatch_ExceptionWithReason(t *testing.T) {
	t.Parallel()

	reason := "Payment failed due to insufficient funds"
	exception := &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityCritical,
		Status:        value_objects.ExceptionStatusOpen,
		Reason:        &reason,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetJira,
			Acknowledged: true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestDispatch_ExceptionWithoutReason(t *testing.T) {
	t.Parallel()

	exception := &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityLow,
		Status:        value_objects.ExceptionStatusOpen,
		Reason:        nil,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetManual,
			Acknowledged: true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("analyst@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "MANUAL",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestDispatch_AuditEventMetadata(t *testing.T) {
	t.Parallel()

	exception := newTestDispatchException()
	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:            services.RoutingTargetJira,
			ExternalReference: "PROJ-999",
			Acknowledged:      true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("team-lead@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("team-lead@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "jira",
		Queue:        "escalation",
	}

	_, err = uc.Dispatch(ctx, cmd)
	require.NoError(t, err)

	require.NotNil(t, audit.lastEvent)
	assert.Equal(t, "JIRA", audit.lastEvent.Metadata["target"])
	assert.Equal(t, "escalation", audit.lastEvent.Metadata["queue"])
	assert.Equal(t, "PROJ-999", audit.lastEvent.Metadata["external_reference"])
	assert.Contains(t, audit.lastEvent.Notes, "Dispatched to JIRA")
}

// DispatchCommand Tests.
func TestDispatchCommand_ZeroValue(t *testing.T) {
	t.Parallel()

	cmd := DispatchCommand{}

	assert.Equal(t, uuid.Nil, cmd.ExceptionID)
	assert.Empty(t, cmd.TargetSystem)
	assert.Empty(t, cmd.Queue)
}

// DispatchResult Tests.
func TestDispatchResult_JSONFields(t *testing.T) {
	t.Parallel()

	result := DispatchResult{
		ExceptionID:       uuid.New(),
		Target:            "JIRA",
		ExternalReference: "JIRA-100",
		Acknowledged:      true,
		DispatchedAt:      time.Now().UTC(),
	}

	assert.NotEqual(t, uuid.Nil, result.ExceptionID)
	assert.Equal(t, "JIRA", result.Target)
	assert.Equal(t, "JIRA-100", result.ExternalReference)
	assert.True(t, result.Acknowledged)
	assert.False(t, result.DispatchedAt.IsZero())
}

func TestDispatchResult_EmptyExternalReference(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	result := DispatchResult{
		ExceptionID: exceptionID,
		Target:      "WEBHOOK",
	}

	assert.Equal(t, exceptionID, result.ExceptionID)
	assert.Equal(t, "WEBHOOK", result.Target)
	assert.Empty(t, result.ExternalReference)
	assert.False(t, result.Acknowledged)
	assert.True(t, result.DispatchedAt.IsZero())
}

// Tests for dispatch payload helper functions.
func TestCalculateAgeDays(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		createdAt time.Time
		wantDays  int
	}{
		{
			name:      "created today",
			createdAt: baseTime,
			wantDays:  0,
		},
		{
			name:      "created 3 days ago",
			createdAt: baseTime.Add(-3 * 24 * time.Hour),
			wantDays:  3,
		},
		{
			name:      "created 30 days ago",
			createdAt: baseTime.Add(-30 * 24 * time.Hour),
			wantDays:  30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := calculateAgeDays(tt.createdAt, baseTime)
			assert.Equal(t, tt.wantDays, got)
		})
	}
}

func TestFormatOptionalTime(t *testing.T) {
	t.Parallel()

	t.Run("nil time returns nil", func(t *testing.T) {
		t.Parallel()

		result := formatOptionalTime(nil)
		assert.Nil(t, result)
	})

	t.Run("valid time returns RFC3339 string", func(t *testing.T) {
		t.Parallel()

		testTime := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)
		result := formatOptionalTime(&testTime)

		require.NotNil(t, result)
		assert.Equal(t, "2025-06-15T14:30:00Z", *result)
	})
}

func TestDispatch_WithAssignedExceptionIncludesContext(t *testing.T) {
	t.Parallel()

	assignee := "senior-analyst@example.com"
	dueAt := time.Now().UTC().Add(48 * time.Hour)
	exception := &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      sharedexception.ExceptionSeverityHigh,
		Status:        value_objects.ExceptionStatusAssigned,
		AssignedTo:    &assignee,
		DueAt:         &dueAt,
		CreatedAt:     time.Now().UTC().Add(-5 * 24 * time.Hour),
		UpdatedAt:     time.Now().UTC(),
	}

	finder, connector := newDispatchMocks(t)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	connector.EXPECT().
		Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:       services.RoutingTargetJira,
			Acknowledged: true,
		}, nil)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("dispatcher@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ctx := ctxWithActor("dispatcher@example.com")
	cmd := DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
	}

	result, err := uc.Dispatch(ctx, cmd)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "JIRA", result.Target)
}

// --- BulkDispatch Concurrency ---

// countingExceptionFinder is a thread-safe ExceptionFinder for
// BulkDispatch parallelism tests. All requested ids resolve to the same
// pre-built exception so the parallel workers can run without touching
// gomock (gomock controllers are not safe for parallel use even with
// AnyTimes matchers when the same call pattern fires from multiple
// goroutines).
type countingExceptionFinder struct {
	exception *entities.Exception
}

func (f *countingExceptionFinder) FindByID(
	_ context.Context,
	_ uuid.UUID,
) (*entities.Exception, error) {
	return f.exception, nil
}

// concurrencyTrackingConnector records the peak number of in-flight
// Dispatch calls so BulkDispatch's errgroup.SetLimit(10) can be asserted
// directly. The ready channel gates the start of each call so the
// pool is guaranteed to hit saturation before any call completes.
type concurrencyTrackingConnector struct {
	inFlight    atomic.Int64
	peak        atomic.Int64
	totalCalls  atomic.Int64
	releaseOnce sync.Once
	ready       chan struct{}
	release     chan struct{}
	startCount  atomic.Int64
}

func newConcurrencyTrackingConnector(saturation int64) *concurrencyTrackingConnector {
	return &concurrencyTrackingConnector{
		ready:   make(chan struct{}, saturation),
		release: make(chan struct{}),
	}
}

func (c *concurrencyTrackingConnector) Dispatch(
	_ context.Context,
	exceptionID string,
	_ services.RoutingDecision,
	_ []byte,
) (ports.DispatchResult, error) {
	now := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)

	for {
		peak := c.peak.Load()
		if now <= peak {
			break
		}

		if c.peak.CompareAndSwap(peak, now) {
			break
		}
	}

	// Signal that this worker has started and block until release fires.
	// The test closes release after the ready channel reaches capacity,
	// which proves the group hit its concurrency bound.
	c.ready <- struct{}{}
	c.startCount.Add(1)

	<-c.release

	c.totalCalls.Add(1)

	return ports.DispatchResult{
		Target:            services.RoutingTargetJira,
		ExternalReference: "EXT-" + exceptionID,
		Acknowledged:      true,
	}, nil
}

// TestBulkDispatch_RespectsConcurrencyLimit asserts that BulkDispatch
// caps in-flight Dispatch calls at bulkDispatchConcurrency even when the
// batch is larger than the limit. A 20-item batch with a connector that
// blocks on a shared channel must plateau at 10 concurrent calls: if the
// cap were absent or too high, all 20 workers would enter Dispatch and
// the test would deadlock (the release channel is only closed after the
// ready channel reports saturation).
func TestBulkDispatch_RespectsConcurrencyLimit(t *testing.T) {
	t.Parallel()

	const batchSize = 20

	exception := newTestDispatchException()
	finder := &countingExceptionFinder{exception: exception}
	connector := newConcurrencyTrackingConnector(bulkDispatchConcurrency)

	audit := &stubAuditPublisher{}
	actor := actorExtractor("dispatcher@example.com")

	uc, err := NewExceptionUseCase(wrapFinder(finder), actor, audit, &stubInfraProvider{}, WithExternalConnector(connector))
	require.NoError(t, err)

	ids := make([]uuid.UUID, 0, batchSize)
	for range batchSize {
		ids = append(ids, uuid.New())
	}

	// Drive the release channel on a watchdog goroutine: once the
	// connector reports saturation it means the group hit the cap, and
	// we can unblock the pool so the test does not deadlock.
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Wait until bulkDispatchConcurrency workers have signalled ready.
		for range bulkDispatchConcurrency {
			select {
			case <-connector.ready:
			case <-time.After(5 * time.Second):
				t.Errorf("timed out waiting for dispatch pool to saturate")
				return
			}
		}

		// Pool is saturated -- now prove no additional worker entered
		// before we release. A brief sleep here is acceptable: the test
		// is asserting an upper bound, not a lower bound.
		time.Sleep(50 * time.Millisecond)

		assert.LessOrEqual(t, connector.inFlight.Load(), int64(bulkDispatchConcurrency),
			"in-flight dispatches must not exceed bulkDispatchConcurrency")

		// Release all workers and drain ready signals as later workers arrive.
		connector.releaseOnce.Do(func() { close(connector.release) })

		for i := bulkDispatchConcurrency; i < batchSize; i++ {
			select {
			case <-connector.ready:
			case <-time.After(5 * time.Second):
				t.Errorf("timed out draining ready channel after release")
				return
			}
		}
	}()

	ctx := ctxWithActor("dispatcher@example.com")

	result, err := uc.BulkDispatch(ctx, BulkDispatchInput{
		ExceptionIDs: ids,
		TargetSystem: "JIRA",
	})

	<-done

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Succeeded, batchSize, "all dispatches should succeed")
	assert.Empty(t, result.Failed)

	assert.LessOrEqual(t, connector.peak.Load(), int64(bulkDispatchConcurrency),
		"peak in-flight dispatches must stay at or below the configured cap")
	assert.Equal(t, int64(batchSize), connector.totalCalls.Load(),
		"every dispatched item must complete its connector call")
}
