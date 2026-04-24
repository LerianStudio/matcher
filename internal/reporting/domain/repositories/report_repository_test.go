// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package repositories

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories/mocks"
)

func TestReportRepository_MockImplementsInterface(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var _ ReportRepository = mocks.NewMockReportRepository(ctrl)
}

func TestReportRepository_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := mocks.NewMockReportRepository(ctrl)
	assert.NotNil(t, mock)
	assert.NotNil(t, mock.EXPECT())
}

func TestReportRepository_ReadOnlyInterface(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*ReportRepository)(nil)).Elem()

	allowedMethods := map[string]bool{
		"ListMatched":            true,
		"ListUnmatched":          true,
		"GetSummary":             true,
		"GetVarianceReport":      true,
		"ListMatchedForExport":   true,
		"ListUnmatchedForExport": true,
		"ListVarianceForExport":  true,
		"ListMatchedPage":        true,
		"ListUnmatchedPage":      true,
		"ListVariancePage":       true,
		"CountMatched":           true,
		"CountUnmatched":         true,
		"CountTransactions":      true,
		"CountExceptions":        true,
	}

	forbiddenPatterns := []string{
		"Create",
		"Update",
		"Delete",
		"Remove",
		"Insert",
		"Save",
	}

	for i := 0; i < repoType.NumMethod(); i++ {
		method := repoType.Method(i)
		methodName := method.Name

		if !allowedMethods[methodName] {
			t.Errorf(
				"unexpected method %q in ReportRepository interface - should be read-only",
				methodName,
			)
		}

		for _, pattern := range forbiddenPatterns {
			if containsSubstringReport(methodName, pattern) {
				t.Errorf(
					"method %q contains forbidden pattern %q - report repository must be read-only",
					methodName,
					pattern,
				)
			}
		}
	}
}

func TestReportRepository_MethodCount(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*ReportRepository)(nil)).Elem()

	const expectedMethodCount = 14

	actualCount := repoType.NumMethod()

	assert.Equal(t, expectedMethodCount, actualCount,
		"ReportRepository should have exactly %d methods - found %d",
		expectedMethodCount, actualCount)
}

func TestReportRepository_InterfaceContract(t *testing.T) {
	t.Parallel()

	repoType := reflect.TypeOf((*ReportRepository)(nil)).Elem()

	t.Run("ListMatched method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListMatched")
		assert.True(t, exists, "ListMatched method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "ListMatched should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 3, numOut, "ListMatched should return items, pagination, and error")
	})

	t.Run("ListUnmatched method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListUnmatched")
		assert.True(t, exists, "ListUnmatched method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "ListUnmatched should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 3, numOut, "ListUnmatched should return items, pagination, and error")
	})

	t.Run("GetSummary method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetSummary")
		assert.True(t, exists, "GetSummary method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "GetSummary should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "GetSummary should return summary and error")
	})

	t.Run("GetVarianceReport method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("GetVarianceReport")
		assert.True(t, exists, "GetVarianceReport method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "GetVarianceReport should accept context and filter")

		numOut := method.Type.NumOut()
		assert.Equal(t, 3, numOut, "GetVarianceReport should return rows, pagination, and error")
	})

	t.Run("ListMatchedForExport method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListMatchedForExport")
		assert.True(t, exists, "ListMatchedForExport method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(
			t,
			3,
			numIn,
			"ListMatchedForExport should accept context, filter, and maxRecords",
		)

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "ListMatchedForExport should return items and error")
	})

	t.Run("ListUnmatchedForExport method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListUnmatchedForExport")
		assert.True(t, exists, "ListUnmatchedForExport method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(
			t,
			3,
			numIn,
			"ListUnmatchedForExport should accept context, filter, and maxRecords",
		)

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "ListUnmatchedForExport should return items and error")
	})

	t.Run("ListVarianceForExport method exists with correct signature", func(t *testing.T) {
		t.Parallel()

		method, exists := repoType.MethodByName("ListVarianceForExport")
		assert.True(t, exists, "ListVarianceForExport method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(
			t,
			3,
			numIn,
			"ListVarianceForExport should accept context, filter, and maxRecords",
		)

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "ListVarianceForExport should return rows and error")
	})
}

func containsSubstringReport(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
