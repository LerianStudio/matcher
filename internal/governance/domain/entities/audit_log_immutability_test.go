//go:build unit

package entities

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLog_Immutability_ChangesIsolated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	entityID := uuid.New()
	originalChanges := []byte(`{"key":"original"}`)

	log, err := NewAuditLog(ctx, tenantID, "match_run", entityID, "CREATED", nil, originalChanges)
	require.NoError(t, err)

	originalChanges[0] = 'X'
	originalChanges[1] = 'X'

	assert.JSONEq(
		t,
		`{"key":"original"}`,
		string(log.Changes),
		"AuditLog.Changes must be isolated from original slice",
	)
}

func TestAuditLog_Immutability_NoSetterMethods(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	entityID := uuid.New()

	log, err := NewAuditLog(ctx, tenantID, "transaction", entityID, "MATCHED", nil, []byte(`{}`))
	require.NoError(t, err)

	// Setter-like prefixes that would indicate mutable API on AuditLog.
	setterPrefixes := []string{"Set", "With", "Update", "Modify", "Reset"}

	// Check both value receiver (AuditLog) and pointer receiver (*AuditLog).
	types := []reflect.Type{
		reflect.TypeOf(*log), // AuditLog  — value receiver methods
		reflect.TypeOf(log),  // *AuditLog — pointer receiver methods
	}

	for _, typ := range types {
		for i := range typ.NumMethod() {
			method := typ.Method(i)

			for _, prefix := range setterPrefixes {
				if strings.HasPrefix(method.Name, prefix) {
					t.Errorf(
						"AuditLog must be immutable: found setter-like method %q (prefix %q) on %v",
						method.Name, prefix, typ,
					)
				}
			}
		}
	}
}

func TestAuditLog_Immutability_ActorIDIsolated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	entityID := uuid.New()
	actorID := "system-user"

	log, err := NewAuditLog(ctx, tenantID, "match_run", entityID, "CREATED", &actorID, []byte(`{}`))
	require.NoError(t, err)
	require.NotNil(t, log.ActorID)

	actorID = "modified-user"

	assert.Equal(
		t,
		"system-user",
		*log.ActorID,
		"AuditLog.ActorID must be isolated from original pointer",
	)
}

func TestAuditLog_Immutability_CreatedAtNeverZero(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	log, err := NewAuditLog(ctx, uuid.New(), "entity", uuid.New(), "action", nil, []byte(`{}`))
	require.NoError(t, err)

	assert.False(t, log.CreatedAt.IsZero(), "CreatedAt must be set automatically and never be zero")
}

func TestAuditLog_Immutability_IDAlwaysGenerated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tenantID := uuid.New()
	entityID := uuid.New()

	log1, err := NewAuditLog(ctx, tenantID, "entity", entityID, "action", nil, []byte(`{}`))
	require.NoError(t, err)

	log2, err := NewAuditLog(ctx, tenantID, "entity", entityID, "action", nil, []byte(`{}`))
	require.NoError(t, err)

	assert.NotEqual(t, uuid.Nil, log1.ID, "ID must be generated")
	assert.NotEqual(t, uuid.Nil, log2.ID, "ID must be generated")
	assert.NotEqual(t, log1.ID, log2.ID, "Each AuditLog must have unique ID")
}

func TestAuditLog_Immutability_MultipleCreationsAreIndependent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	changes := []byte(`{"key":"shared"}`)

	log1, err := NewAuditLog(ctx, uuid.New(), "entity1", uuid.New(), "action1", nil, changes)
	require.NoError(t, err)

	changes = []byte(`{"key":"modded"}`)

	log2, err := NewAuditLog(ctx, uuid.New(), "entity2", uuid.New(), "action2", nil, changes)
	require.NoError(t, err)

	assert.NotEqual(
		t,
		log1.Changes,
		log2.Changes,
		"Each AuditLog should have independent Changes copy",
	)
	assert.JSONEq(
		t,
		`{"key":"shared"}`,
		string(log1.Changes),
		"First log should preserve original changes",
	)
	assert.JSONEq(
		t,
		`{"key":"modded"}`,
		string(log2.Changes),
		"Second log should have modified changes",
	)
}
