//go:build unit

package value_objects_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestIdempotencyStatus_Constants(t *testing.T) {
	t.Parallel()

	t.Run("IdempotencyStatusUnknown has correct value", func(t *testing.T) {
		t.Parallel()

		require.Equal(
			t,
			value_objects.IdempotencyStatusUnknown,
			value_objects.IdempotencyStatus("unknown"),
		)
	})

	t.Run("IdempotencyStatusPending has correct value", func(t *testing.T) {
		t.Parallel()

		require.Equal(
			t,
			value_objects.IdempotencyStatusPending,
			value_objects.IdempotencyStatus("pending"),
		)
	})

	t.Run("IdempotencyStatusComplete has correct value", func(t *testing.T) {
		t.Parallel()

		require.Equal(
			t,
			value_objects.IdempotencyStatusComplete,
			value_objects.IdempotencyStatus("complete"),
		)
	})

	t.Run("IdempotencyStatusFailed has correct value", func(t *testing.T) {
		t.Parallel()

		require.Equal(
			t,
			value_objects.IdempotencyStatusFailed,
			value_objects.IdempotencyStatus("failed"),
		)
	})
}

func TestIdempotencyStatus_StringConversion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.IdempotencyStatus
		expected string
	}{
		{"unknown status to string", value_objects.IdempotencyStatusUnknown, "unknown"},
		{"pending status to string", value_objects.IdempotencyStatusPending, "pending"},
		{"complete status to string", value_objects.IdempotencyStatusComplete, "complete"},
		{"failed status to string", value_objects.IdempotencyStatusFailed, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestIdempotencyStatus_Equality(t *testing.T) {
	t.Parallel()

	t.Run("same status values are equal", func(t *testing.T) {
		t.Parallel()

		status1 := value_objects.IdempotencyStatusComplete
		status2 := value_objects.IdempotencyStatusComplete
		require.Equal(t, status1, status2)
	})

	t.Run("different status values are not equal", func(t *testing.T) {
		t.Parallel()

		require.NotEqual(
			t,
			value_objects.IdempotencyStatusPending,
			value_objects.IdempotencyStatusComplete,
		)
		require.NotEqual(
			t,
			value_objects.IdempotencyStatusUnknown,
			value_objects.IdempotencyStatusFailed,
		)
		require.NotEqual(
			t,
			value_objects.IdempotencyStatusComplete,
			value_objects.IdempotencyStatusFailed,
		)
	})

	t.Run("status can be compared with string cast", func(t *testing.T) {
		t.Parallel()

		status := value_objects.IdempotencyStatusComplete
		require.Equal(t, value_objects.IdempotencyStatus("complete"), status)
	})
}

func TestIdempotencyResult_Fields(t *testing.T) {
	t.Parallel()

	t.Run("zero value has empty fields", func(t *testing.T) {
		t.Parallel()

		var result value_objects.IdempotencyResult
		require.Equal(t, value_objects.IdempotencyStatus(""), result.Status)
		require.Nil(t, result.Response)
		require.Equal(t, 0, result.HTTPStatus)
	})

	t.Run("complete result with all fields populated", func(t *testing.T) {
		t.Parallel()

		response := []byte(`{"id":"123","name":"test"}`)
		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusComplete,
			Response:   response,
			HTTPStatus: 200,
		}

		require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
		require.Equal(t, response, result.Response)
		require.Equal(t, 200, result.HTTPStatus)
	})

	t.Run("pending result has empty response", func(t *testing.T) {
		t.Parallel()

		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusPending,
			Response:   nil,
			HTTPStatus: 0,
		}

		require.Equal(t, value_objects.IdempotencyStatusPending, result.Status)
		require.Nil(t, result.Response)
		require.Equal(t, 0, result.HTTPStatus)
	})

	t.Run("failed result has empty response", func(t *testing.T) {
		t.Parallel()

		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusFailed,
			Response:   nil,
			HTTPStatus: 0,
		}

		require.Equal(t, value_objects.IdempotencyStatusFailed, result.Status)
		require.Nil(t, result.Response)
		require.Equal(t, 0, result.HTTPStatus)
	})
}

func TestIdempotencyResult_Equality(t *testing.T) {
	t.Parallel()

	t.Run("identical results are equal", func(t *testing.T) {
		t.Parallel()

		response := []byte(`{"data":"value"}`)
		result1 := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusComplete,
			Response:   response,
			HTTPStatus: 201,
		}
		result2 := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusComplete,
			Response:   response,
			HTTPStatus: 201,
		}

		require.Equal(t, result1.Status, result2.Status)
		require.Equal(t, result1.Response, result2.Response)
		require.Equal(t, result1.HTTPStatus, result2.HTTPStatus)
	})

	t.Run("different status makes results unequal", func(t *testing.T) {
		t.Parallel()

		result1 := value_objects.IdempotencyResult{Status: value_objects.IdempotencyStatusComplete}
		result2 := value_objects.IdempotencyResult{Status: value_objects.IdempotencyStatusPending}

		require.NotEqual(t, result1.Status, result2.Status)
	})

	t.Run("different HTTP status makes results unequal", func(t *testing.T) {
		t.Parallel()

		result1 := value_objects.IdempotencyResult{HTTPStatus: 200}
		result2 := value_objects.IdempotencyResult{HTTPStatus: 201}

		require.NotEqual(t, result1.HTTPStatus, result2.HTTPStatus)
	})
}

func TestIdempotencyResult_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty response byte slice", func(t *testing.T) {
		t.Parallel()

		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusComplete,
			Response:   []byte{},
			HTTPStatus: 204,
		}

		require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
		require.Empty(t, result.Response)
		require.Equal(t, 204, result.HTTPStatus)
	})

	t.Run("large response payload", func(t *testing.T) {
		t.Parallel()

		largePayload := make([]byte, 1024*1024)
		for i := range largePayload {
			largePayload[i] = byte(i % 256)
		}

		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusComplete,
			Response:   largePayload,
			HTTPStatus: 200,
		}

		require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
		require.Len(t, result.Response, 1024*1024)
		require.Equal(t, 200, result.HTTPStatus)
	})

	t.Run("negative HTTP status", func(t *testing.T) {
		t.Parallel()

		result := value_objects.IdempotencyResult{
			Status:     value_objects.IdempotencyStatusFailed,
			HTTPStatus: -1,
		}

		require.Equal(t, value_objects.IdempotencyStatusFailed, result.Status)
		require.Equal(t, -1, result.HTTPStatus)
	})

	t.Run("custom status value", func(t *testing.T) {
		t.Parallel()

		customStatus := value_objects.IdempotencyStatus("custom")
		result := value_objects.IdempotencyResult{
			Status: customStatus,
		}

		require.Equal(t, customStatus, result.Status)
	})

	t.Run("HTTP status codes coverage", func(t *testing.T) {
		t.Parallel()

		httpCodes := []int{100, 200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}

		for _, code := range httpCodes {
			t.Run(fmt.Sprintf("status-%d", code), func(t *testing.T) {
				t.Parallel()

				result := value_objects.IdempotencyResult{
					Status:     value_objects.IdempotencyStatusComplete,
					HTTPStatus: code,
				}

				require.Equal(t, value_objects.IdempotencyStatusComplete, result.Status)
				require.Equal(t, code, result.HTTPStatus)
			})
		}
	})
}
