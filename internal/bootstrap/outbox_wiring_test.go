//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedDomain "github.com/LerianStudio/matcher/internal/shared/domain"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// --- Fakes ---

type fakeTenantDiscoverer struct {
	tenants []string
	err     error
	calls   int
}

func (f *fakeTenantDiscoverer) DiscoverTenants(_ context.Context) ([]string, error) {
	f.calls++
	return f.tenants, f.err
}

type fakeIngestionPub struct {
	completed     int
	failed        int
	lastCompleted *sharedDomain.IngestionCompletedEvent
	lastFailed    *sharedDomain.IngestionFailedEvent
	pubErr        error
}

func (f *fakeIngestionPub) PublishIngestionCompleted(
	_ context.Context,
	event *sharedDomain.IngestionCompletedEvent,
) error {
	f.completed++
	f.lastCompleted = event
	return f.pubErr
}

func (f *fakeIngestionPub) PublishIngestionFailed(
	_ context.Context,
	event *sharedDomain.IngestionFailedEvent,
) error {
	f.failed++
	f.lastFailed = event
	return f.pubErr
}

var _ sharedPorts.IngestionEventPublisher = (*fakeIngestionPub)(nil)

type fakeMatchPub struct {
	confirmed     int
	unmatched     int
	lastConfirmed *sharedDomain.MatchConfirmedEvent
	lastUnmatched *sharedDomain.MatchUnmatchedEvent
	pubErr        error
}

func (f *fakeMatchPub) PublishMatchConfirmed(
	_ context.Context,
	event *sharedDomain.MatchConfirmedEvent,
) error {
	f.confirmed++
	f.lastConfirmed = event
	return f.pubErr
}

func (f *fakeMatchPub) PublishMatchUnmatched(
	_ context.Context,
	event *sharedDomain.MatchUnmatchedEvent,
) error {
	f.unmatched++
	f.lastUnmatched = event
	return f.pubErr
}

var _ sharedDomain.MatchEventPublisher = (*fakeMatchPub)(nil)

type fakeAuditPub struct {
	created int
	lastLog *sharedDomain.AuditLogCreatedEvent
	pubErr  error
}

func (f *fakeAuditPub) PublishAuditLogCreated(
	_ context.Context,
	event *sharedDomain.AuditLogCreatedEvent,
) error {
	f.created++
	f.lastLog = event
	return f.pubErr
}

var _ sharedDomain.AuditEventPublisher = (*fakeAuditPub)(nil)

// --- defaultTenantDiscoverer tests ---

func TestDefaultTenantDiscoverer_AppendsDefaultWhenMissing(t *testing.T) {
	t.Parallel()

	tenantA := uuid.NewString()
	inner := &fakeTenantDiscoverer{tenants: []string{tenantA}}
	wrapper := &defaultTenantDiscoverer{inner: inner}

	got, err := wrapper.DiscoverTenants(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, tenantA, got[0])
	assert.Equal(t, auth.GetDefaultTenantID(), got[1])
	assert.Equal(t, 1, inner.calls)
}

func TestDefaultTenantDiscoverer_SkipsAppendWhenAlreadyPresent(t *testing.T) {
	t.Parallel()

	tenantA := uuid.NewString()
	defaultID := auth.GetDefaultTenantID()
	require.NotEmpty(t, defaultID, "default tenant ID must be set via compile-time constant")

	inner := &fakeTenantDiscoverer{tenants: []string{defaultID, tenantA}}
	wrapper := &defaultTenantDiscoverer{inner: inner}

	got, err := wrapper.DiscoverTenants(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, defaultID, got[0])
	assert.Equal(t, tenantA, got[1])
}

func TestDefaultTenantDiscoverer_EmptyInnerReturnsDefaultOnly(t *testing.T) {
	t.Parallel()

	inner := &fakeTenantDiscoverer{tenants: []string{}}
	wrapper := &defaultTenantDiscoverer{inner: inner}

	got, err := wrapper.DiscoverTenants(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, auth.GetDefaultTenantID(), got[0])
}

func TestDefaultTenantDiscoverer_PropagatesInnerError(t *testing.T) {
	t.Parallel()

	innerErr := errors.New("discover boom")
	inner := &fakeTenantDiscoverer{err: innerErr}
	wrapper := &defaultTenantDiscoverer{inner: inner}

	got, err := wrapper.DiscoverTenants(context.Background())
	require.Error(t, err)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, innerErr)
}

func TestDefaultTenantDiscoverer_NilReceiver(t *testing.T) {
	t.Parallel()

	var wrapper *defaultTenantDiscoverer

	got, err := wrapper.DiscoverTenants(context.Background())
	require.ErrorIs(t, err, errDefaultTenantDiscovererUninitialized)
	assert.Nil(t, got)
}

func TestDefaultTenantDiscoverer_NilInner(t *testing.T) {
	t.Parallel()

	wrapper := &defaultTenantDiscoverer{inner: nil}

	got, err := wrapper.DiscoverTenants(context.Background())
	require.ErrorIs(t, err, errDefaultTenantDiscovererUninitialized)
	assert.Nil(t, got)
}

// --- isNonRetryableOutboxError tests ---

func TestIsNonRetryableOutboxError_Nil(t *testing.T) {
	t.Parallel()
	assert.False(t, isNonRetryableOutboxError(nil))
}

func TestIsNonRetryableOutboxError_UnrelatedError(t *testing.T) {
	t.Parallel()
	assert.False(t, isNonRetryableOutboxError(errors.New("random failure")))
}

func TestIsNonRetryableOutboxError_EachNonRetryableSentinel(t *testing.T) {
	t.Parallel()
	require.NotEmpty(t, nonRetryableErrors)
	for _, sentinel := range nonRetryableErrors {
		sentinel := sentinel
		t.Run(sentinel.Error(), func(t *testing.T) {
			t.Parallel()
			assert.True(t, isNonRetryableOutboxError(sentinel))
		})
	}
}

func TestIsNonRetryableOutboxError_WrappedSentinel(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("context: %w", errInvalidPayload)
	assert.True(t, isNonRetryableOutboxError(wrapped))
}

func TestIsNonRetryableOutboxError_DoubleWrappedSentinel(t *testing.T) {
	t.Parallel()
	inner := fmt.Errorf("outer: %w", errMissingJobID)
	outer := fmt.Errorf("handler: %w", inner)
	assert.True(t, isNonRetryableOutboxError(outer))
}

func TestIsNonRetryableOutboxError_CanonicalOutboxEventRequired(t *testing.T) {
	t.Parallel()
	// Ensure canonical lib-commons sentinel flows through the classifier.
	assert.True(t, isNonRetryableOutboxError(outbox.ErrOutboxEventRequired))
}

func TestIsNonRetryableOutboxError_PayloadTooLarge(t *testing.T) {
	t.Parallel()
	// v5 outbox enforces a 1 MiB payload cap; callers must treat this as
	// terminal so an oversized event does not retry forever.
	assert.True(t, isNonRetryableOutboxError(outbox.ErrOutboxEventPayloadTooLarge))
	wrapped := fmt.Errorf("persist outbox event: %w", outbox.ErrOutboxEventPayloadTooLarge)
	assert.True(t, isNonRetryableOutboxError(wrapped))
}

func TestIsNonRetryableOutboxError_PayloadNotJSON(t *testing.T) {
	t.Parallel()
	// JSONB storage requires valid JSON; a malformed payload is a
	// structural defect no retry will repair.
	assert.True(t, isNonRetryableOutboxError(outbox.ErrOutboxEventPayloadNotJSON))
	wrapped := fmt.Errorf("persist outbox event: %w", outbox.ErrOutboxEventPayloadNotJSON)
	assert.True(t, isNonRetryableOutboxError(wrapped))
}

// --- validateIngestionCompletedPayload tests ---

func validIngestionCompletedEvent() sharedDomain.IngestionCompletedEvent {
	return sharedDomain.IngestionCompletedEvent{
		EventType: sharedDomain.EventTypeIngestionCompleted,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
	}
}

func TestValidateIngestionCompletedPayload_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateIngestionCompletedPayload(validIngestionCompletedEvent()))
}

func TestValidateIngestionCompletedPayload_MissingJobID(t *testing.T) {
	t.Parallel()
	ev := validIngestionCompletedEvent()
	ev.JobID = uuid.Nil

	err := validateIngestionCompletedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingJobID)
}

func TestValidateIngestionCompletedPayload_MissingContextID(t *testing.T) {
	t.Parallel()
	ev := validIngestionCompletedEvent()
	ev.ContextID = uuid.Nil

	err := validateIngestionCompletedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingContextID)
}

func TestValidateIngestionCompletedPayload_MissingSourceID(t *testing.T) {
	t.Parallel()
	ev := validIngestionCompletedEvent()
	ev.SourceID = uuid.Nil

	err := validateIngestionCompletedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingSourceID)
}

// --- validateIngestionFailedPayload tests ---

func validIngestionFailedEvent() sharedDomain.IngestionFailedEvent {
	return sharedDomain.IngestionFailedEvent{
		EventType: sharedDomain.EventTypeIngestionFailed,
		JobID:     uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Error:     "source parse failure",
	}
}

func TestValidateIngestionFailedPayload_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateIngestionFailedPayload(validIngestionFailedEvent()))
}

func TestValidateIngestionFailedPayload_MissingJobID(t *testing.T) {
	t.Parallel()
	ev := validIngestionFailedEvent()
	ev.JobID = uuid.Nil

	err := validateIngestionFailedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingJobID)
}

func TestValidateIngestionFailedPayload_MissingContextID(t *testing.T) {
	t.Parallel()
	ev := validIngestionFailedEvent()
	ev.ContextID = uuid.Nil

	err := validateIngestionFailedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingContextID)
}

func TestValidateIngestionFailedPayload_MissingSourceID(t *testing.T) {
	t.Parallel()
	ev := validIngestionFailedEvent()
	ev.SourceID = uuid.Nil

	err := validateIngestionFailedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingSourceID)
}

func TestValidateIngestionFailedPayload_MissingError(t *testing.T) {
	t.Parallel()
	ev := validIngestionFailedEvent()
	ev.Error = ""

	err := validateIngestionFailedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingError)
}

// --- validateMatchConfirmedPayload tests ---

func validMatchConfirmedEvent() sharedDomain.MatchConfirmedEvent {
	return sharedDomain.MatchConfirmedEvent{
		EventType:      sharedDomain.EventTypeMatchConfirmed,
		TenantID:       uuid.New(),
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New(), uuid.New()},
	}
}

func TestValidateMatchConfirmedPayload_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateMatchConfirmedPayload(validMatchConfirmedEvent()))
}

func TestValidateMatchConfirmedPayload_MissingTenantID(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.TenantID = uuid.Nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTenantID)
}

func TestValidateMatchConfirmedPayload_MissingContextID(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.ContextID = uuid.Nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingContextID)
}

func TestValidateMatchConfirmedPayload_MissingRunID(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.RunID = uuid.Nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingMatchRunID)
}

func TestValidateMatchConfirmedPayload_MissingMatchID(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.MatchID = uuid.Nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingMatchID)
}

func TestValidateMatchConfirmedPayload_MissingRuleID(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.RuleID = uuid.Nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingMatchRuleID)
}

func TestValidateMatchConfirmedPayload_MissingTransactionIDs(t *testing.T) {
	t.Parallel()
	ev := validMatchConfirmedEvent()
	ev.TransactionIDs = nil

	err := validateMatchConfirmedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTransactionIDs)
}

// --- validateMatchUnmatchedPayload tests ---

func validMatchUnmatchedEvent() sharedDomain.MatchUnmatchedEvent {
	return sharedDomain.MatchUnmatchedEvent{
		EventType:      sharedDomain.EventTypeMatchUnmatched,
		TenantID:       uuid.New(),
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New()},
		Reason:         "user revoked",
	}
}

func TestValidateMatchUnmatchedPayload_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateMatchUnmatchedPayload(validMatchUnmatchedEvent()))
}

func TestValidateMatchUnmatchedPayload_MissingTenantID(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.TenantID = uuid.Nil

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTenantID)
}

func TestValidateMatchUnmatchedPayload_MissingContextID(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.ContextID = uuid.Nil

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingContextID)
}

func TestValidateMatchUnmatchedPayload_MissingRunID(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.RunID = uuid.Nil

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingMatchRunID)
}

func TestValidateMatchUnmatchedPayload_MissingMatchID(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.MatchID = uuid.Nil

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingMatchID)
}

func TestValidateMatchUnmatchedPayload_MissingTransactionIDs(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.TransactionIDs = nil

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTransactionIDs)
}

func TestValidateMatchUnmatchedPayload_MissingReason(t *testing.T) {
	t.Parallel()
	ev := validMatchUnmatchedEvent()
	ev.Reason = ""

	err := validateMatchUnmatchedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingReason)
}

// --- validateAuditLogCreatedPayload tests ---

func validAuditLogCreatedEvent() sharedDomain.AuditLogCreatedEvent {
	return sharedDomain.AuditLogCreatedEvent{
		UniqueID:   uuid.New(),
		EventType:  sharedDomain.EventTypeAuditLogCreated,
		TenantID:   uuid.New(),
		EntityType: "context",
		EntityID:   uuid.New(),
		Action:     "create",
	}
}

func TestValidateAuditLogCreatedPayload_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateAuditLogCreatedPayload(validAuditLogCreatedEvent()))
}

func TestValidateAuditLogCreatedPayload_MissingTenantID(t *testing.T) {
	t.Parallel()
	ev := validAuditLogCreatedEvent()
	ev.TenantID = uuid.Nil

	err := validateAuditLogCreatedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTenantID)
}

func TestValidateAuditLogCreatedPayload_MissingEntityType(t *testing.T) {
	t.Parallel()
	ev := validAuditLogCreatedEvent()
	ev.EntityType = ""

	err := validateAuditLogCreatedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingEntityType)
}

func TestValidateAuditLogCreatedPayload_MissingEntityID(t *testing.T) {
	t.Parallel()
	ev := validAuditLogCreatedEvent()
	ev.EntityID = uuid.Nil

	err := validateAuditLogCreatedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingEntityID)
}

func TestValidateAuditLogCreatedPayload_MissingAction(t *testing.T) {
	t.Parallel()
	ev := validAuditLogCreatedEvent()
	ev.Action = ""

	err := validateAuditLogCreatedPayload(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingAction)
}

// --- publishIngestionCompleted tests ---

func TestPublishIngestionCompleted_Success(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}
	event := validIngestionCompletedEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = publishIngestionCompleted(context.Background(), pub, payload)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.completed)
	require.NotNil(t, pub.lastCompleted)
	assert.Equal(t, event.JobID, pub.lastCompleted.JobID)
	assert.Equal(t, event.ContextID, pub.lastCompleted.ContextID)
	assert.Equal(t, event.SourceID, pub.lastCompleted.SourceID)
}

func TestPublishIngestionCompleted_InvalidJSON(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}

	err := publishIngestionCompleted(context.Background(), pub, []byte("not-json"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidPayload)
	assert.Equal(t, 0, pub.completed)
}

func TestPublishIngestionCompleted_ValidationFailure(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}
	ev := validIngestionCompletedEvent()
	ev.JobID = uuid.Nil
	payload, err := json.Marshal(ev)
	require.NoError(t, err)

	err = publishIngestionCompleted(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingJobID)
	assert.Equal(t, 0, pub.completed)
}

func TestPublishIngestionCompleted_PublisherError(t *testing.T) {
	t.Parallel()

	pubErr := errors.New("broker offline")
	pub := &fakeIngestionPub{pubErr: pubErr}
	payload, err := json.Marshal(validIngestionCompletedEvent())
	require.NoError(t, err)

	err = publishIngestionCompleted(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	assert.Equal(t, 1, pub.completed)
}

// --- publishIngestionFailed tests ---

func TestPublishIngestionFailed_Success(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}
	event := validIngestionFailedEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = publishIngestionFailed(context.Background(), pub, payload)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.failed)
	require.NotNil(t, pub.lastFailed)
	assert.Equal(t, event.Error, pub.lastFailed.Error)
}

func TestPublishIngestionFailed_InvalidJSON(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}

	err := publishIngestionFailed(context.Background(), pub, []byte("{"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidPayload)
	assert.Equal(t, 0, pub.failed)
}

func TestPublishIngestionFailed_ValidationFailure(t *testing.T) {
	t.Parallel()

	pub := &fakeIngestionPub{}
	ev := validIngestionFailedEvent()
	ev.Error = ""
	payload, err := json.Marshal(ev)
	require.NoError(t, err)

	err = publishIngestionFailed(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingError)
	assert.Equal(t, 0, pub.failed)
}

func TestPublishIngestionFailed_PublisherError(t *testing.T) {
	t.Parallel()

	pubErr := errors.New("broker offline")
	pub := &fakeIngestionPub{pubErr: pubErr}
	payload, err := json.Marshal(validIngestionFailedEvent())
	require.NoError(t, err)

	err = publishIngestionFailed(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	assert.Equal(t, 1, pub.failed)
}

// --- publishMatchConfirmed tests ---

func TestPublishMatchConfirmed_Success(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}
	event := validMatchConfirmedEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = publishMatchConfirmed(context.Background(), pub, payload)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.confirmed)
	require.NotNil(t, pub.lastConfirmed)
	assert.Equal(t, event.MatchID, pub.lastConfirmed.MatchID)
}

func TestPublishMatchConfirmed_InvalidJSON(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}

	err := publishMatchConfirmed(context.Background(), pub, []byte("bogus"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidPayload)
	assert.Equal(t, 0, pub.confirmed)
}

func TestPublishMatchConfirmed_ValidationFailure(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}
	ev := validMatchConfirmedEvent()
	ev.TenantID = uuid.Nil
	payload, err := json.Marshal(ev)
	require.NoError(t, err)

	err = publishMatchConfirmed(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingTenantID)
	assert.Equal(t, 0, pub.confirmed)
}

func TestPublishMatchConfirmed_PublisherError(t *testing.T) {
	t.Parallel()

	pubErr := errors.New("queue down")
	pub := &fakeMatchPub{pubErr: pubErr}
	payload, err := json.Marshal(validMatchConfirmedEvent())
	require.NoError(t, err)

	err = publishMatchConfirmed(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	assert.Equal(t, 1, pub.confirmed)
}

// --- publishMatchUnmatched tests ---

func TestPublishMatchUnmatched_Success(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}
	event := validMatchUnmatchedEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = publishMatchUnmatched(context.Background(), pub, payload)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.unmatched)
	require.NotNil(t, pub.lastUnmatched)
	assert.Equal(t, event.Reason, pub.lastUnmatched.Reason)
}

func TestPublishMatchUnmatched_InvalidJSON(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}

	err := publishMatchUnmatched(context.Background(), pub, []byte("]"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidPayload)
	assert.Equal(t, 0, pub.unmatched)
}

func TestPublishMatchUnmatched_ValidationFailure(t *testing.T) {
	t.Parallel()

	pub := &fakeMatchPub{}
	ev := validMatchUnmatchedEvent()
	ev.Reason = ""
	payload, err := json.Marshal(ev)
	require.NoError(t, err)

	err = publishMatchUnmatched(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingReason)
	assert.Equal(t, 0, pub.unmatched)
}

func TestPublishMatchUnmatched_PublisherError(t *testing.T) {
	t.Parallel()

	pubErr := errors.New("queue down")
	pub := &fakeMatchPub{pubErr: pubErr}
	payload, err := json.Marshal(validMatchUnmatchedEvent())
	require.NoError(t, err)

	err = publishMatchUnmatched(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	assert.Equal(t, 1, pub.unmatched)
}

// --- publishAuditLogCreated tests ---

func TestPublishAuditLogCreated_Success(t *testing.T) {
	t.Parallel()

	pub := &fakeAuditPub{}
	event := validAuditLogCreatedEvent()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	err = publishAuditLogCreated(context.Background(), pub, payload)
	require.NoError(t, err)
	assert.Equal(t, 1, pub.created)
	require.NotNil(t, pub.lastLog)
	assert.Equal(t, event.EntityType, pub.lastLog.EntityType)
	assert.Equal(t, event.Action, pub.lastLog.Action)
}

func TestPublishAuditLogCreated_NilPublisher(t *testing.T) {
	t.Parallel()

	// Typed-nil publisher: interface is non-nil but underlying concrete value is nil.
	var pub *fakeAuditPub
	payload, err := json.Marshal(validAuditLogCreatedEvent())
	require.NoError(t, err)

	err = publishAuditLogCreated(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errAuditPublisherNotConfigured)
}

func TestPublishAuditLogCreated_InvalidJSON(t *testing.T) {
	t.Parallel()

	pub := &fakeAuditPub{}

	err := publishAuditLogCreated(context.Background(), pub, []byte("garbage"))
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidPayload)
	assert.Equal(t, 0, pub.created)
}

func TestPublishAuditLogCreated_ValidationFailure(t *testing.T) {
	t.Parallel()

	pub := &fakeAuditPub{}
	ev := validAuditLogCreatedEvent()
	ev.EntityType = ""
	payload, err := json.Marshal(ev)
	require.NoError(t, err)

	err = publishAuditLogCreated(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, errMissingEntityType)
	assert.Equal(t, 0, pub.created)
}

func TestPublishAuditLogCreated_PublisherError(t *testing.T) {
	t.Parallel()

	pubErr := errors.New("audit sink down")
	pub := &fakeAuditPub{pubErr: pubErr}
	payload, err := json.Marshal(validAuditLogCreatedEvent())
	require.NoError(t, err)

	err = publishAuditLogCreated(context.Background(), pub, payload)
	require.Error(t, err)
	assert.ErrorIs(t, err, pubErr)
	assert.Equal(t, 1, pub.created)
}

// --- registerOutboxHandlers tests ---

func TestRegisterOutboxHandlers_RegistersAllFiveEventTypes(t *testing.T) {
	t.Parallel()

	registry := outbox.NewHandlerRegistry()
	ingestPub := &fakeIngestionPub{}
	matchPub := &fakeMatchPub{}
	auditPub := &fakeAuditPub{}

	require.NoError(t, registerOutboxHandlers(registry, ingestPub, matchPub, auditPub))

	// Handle is the public way to confirm a type is wired; we send a well-formed
	// event per type and verify the correct fake publisher is invoked exactly once.
	cases := []struct {
		name      string
		eventType string
		payload   any
		verify    func(t *testing.T)
	}{
		{
			name:      "ingestion completed",
			eventType: sharedDomain.EventTypeIngestionCompleted,
			payload:   validIngestionCompletedEvent(),
			verify:    func(t *testing.T) { assert.Equal(t, 1, ingestPub.completed) },
		},
		{
			name:      "ingestion failed",
			eventType: sharedDomain.EventTypeIngestionFailed,
			payload:   validIngestionFailedEvent(),
			verify:    func(t *testing.T) { assert.Equal(t, 1, ingestPub.failed) },
		},
		{
			name:      "match confirmed",
			eventType: sharedDomain.EventTypeMatchConfirmed,
			payload:   validMatchConfirmedEvent(),
			verify:    func(t *testing.T) { assert.Equal(t, 1, matchPub.confirmed) },
		},
		{
			name:      "match unmatched",
			eventType: sharedDomain.EventTypeMatchUnmatched,
			payload:   validMatchUnmatchedEvent(),
			verify:    func(t *testing.T) { assert.Equal(t, 1, matchPub.unmatched) },
		},
		{
			name:      "audit log created",
			eventType: sharedDomain.EventTypeAuditLogCreated,
			payload:   validAuditLogCreatedEvent(),
			verify:    func(t *testing.T) { assert.Equal(t, 1, auditPub.created) },
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(tc.payload)
			require.NoError(t, err)

			event := &outbox.OutboxEvent{
				ID:        uuid.New(),
				EventType: tc.eventType,
				Payload:   payload,
			}

			require.NoError(t, registry.Handle(context.Background(), event))
			tc.verify(t)
		})
	}
}

func TestRegisterOutboxHandlers_DuplicateRegistrationFails(t *testing.T) {
	t.Parallel()

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, registerOutboxHandlers(registry, &fakeIngestionPub{}, &fakeMatchPub{}, &fakeAuditPub{}))

	// Second call should fail because the first already claimed every event type.
	err := registerOutboxHandlers(registry, &fakeIngestionPub{}, &fakeMatchPub{}, &fakeAuditPub{})
	require.Error(t, err)
}
