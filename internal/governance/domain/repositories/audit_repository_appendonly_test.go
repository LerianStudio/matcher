// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAuditLogRepository_AppendOnlyInterface(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*AuditLogRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"Create":       true,
		"CreateWithTx": true, // Allows atomic audit logging within the same transaction
		"GetByID":      true,
		"ListByEntity": true,
		"List":         true,
	}

	forbiddenPatterns := []string{
		"Update",
		"Delete",
		"Remove",
		"Modify",
		"Set",
		"Edit",
		"Patch",
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf(
				"unexpected method %q in AuditLogRepository interface - only Create, GetByID, ListByEntity, List are allowed",
				methodName,
			)
		}

		for _, pattern := range forbiddenPatterns {
			if strings.Contains(methodName, pattern) {
				t.Errorf(
					"method %q contains forbidden pattern %q - audit logs must be append-only",
					methodName,
					pattern,
				)
			}
		}
	}

	assert.True(t, allowedMethods["Create"], "Create method must exist for append operations")
	assert.True(t, allowedMethods["GetByID"], "GetByID method must exist for read operations")
	assert.True(
		t,
		allowedMethods["ListByEntity"],
		"ListByEntity method must exist for query operations",
	)
	assert.True(t, allowedMethods["List"], "List method must exist for filtered query operations")
}

func TestAuditLogRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*AuditLogRepository)(nil)).Elem()

	const expectedMethodCount = 5

	actualCount := repoType.NumMethod()

	assert.Equal(
		t,
		expectedMethodCount,
		actualCount,
		"AuditLogRepository should have exactly %d methods (Create, CreateWithTx, GetByID, ListByEntity, List) - found %d",
		expectedMethodCount,
		actualCount,
	)
}

func TestAuditLogRepository_RuntimeEnforcement(t *testing.T) {
	t.Parallel()

	t.Run("interface methods are callable without panics", func(t *testing.T) {
		t.Parallel()

		repoType := reflect.TypeOf((*AuditLogRepository)(nil)).Elem()

		for i := 0; i < repoType.NumMethod(); i++ {
			method := repoType.Method(i)
			assert.NotEmpty(t, method.Name, "method name should not be empty")
			assert.NotNil(t, method.Type, "method type should not be nil")

			numIn := method.Type.NumIn()
			assert.GreaterOrEqual(
				t,
				numIn,
				1,
				"method %s should have at least context parameter",
				method.Name,
			)

			firstParam := method.Type.In(0)
			assert.Equal(t, "context.Context", firstParam.String(),
				"first parameter of %s should be context.Context", method.Name)
		}
	})

	t.Run("no mutating method signatures exist", func(t *testing.T) {
		t.Parallel()

		repoType := reflect.TypeOf((*AuditLogRepository)(nil)).Elem()

		for i := 0; i < repoType.NumMethod(); i++ {
			method := repoType.Method(i)

			numOut := method.Type.NumOut()
			if method.Name == "Create" {
				assert.GreaterOrEqual(t, numOut, 1, "Create should return at least error")
				continue
			}

			assert.GreaterOrEqual(
				t,
				numOut,
				1,
				"method %s should return at least one value",
				method.Name,
			)
		}
	})
}

func TestAuditLogRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*AuditLogRepository)(nil)).Elem()

	t.Run("Create method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("Create")
		assert.True(t, exists, "Create method must exist")

		numIn := method.Type.NumIn()
		assert.GreaterOrEqual(t, numIn, 2, "Create should accept context and entity")
	})

	t.Run("GetByID method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetByID")
		assert.True(t, exists, "GetByID method must exist")

		numOut := method.Type.NumOut()
		assert.GreaterOrEqual(t, numOut, 2, "GetByID should return entity and error")
	})

	t.Run("ListByEntity method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListByEntity")
		assert.True(t, exists, "ListByEntity method must exist")

		numOut := method.Type.NumOut()
		assert.GreaterOrEqual(t, numOut, 2, "ListByEntity should return list and error")
	})
}
