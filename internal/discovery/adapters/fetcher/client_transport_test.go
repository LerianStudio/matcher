// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package fetcher

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- readBoundedBody tests ---

func TestReadBoundedBody_NormalBody_Success(t *testing.T) {
	t.Parallel()

	body := bytes.NewReader([]byte(`{"status":"ok"}`))

	result, err := readBoundedBody(body)

	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(result))
}

func TestReadBoundedBody_EmptyBody_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	body := bytes.NewReader([]byte{})

	result, err := readBoundedBody(body)

	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestReadBoundedBody_ExactlyAtLimit_Success(t *testing.T) {
	t.Parallel()

	data := make([]byte, maxResponseBodySize)
	for i := range data {
		data[i] = 'x'
	}

	body := bytes.NewReader(data)

	result, err := readBoundedBody(body)

	require.NoError(t, err)
	assert.Len(t, result, maxResponseBodySize)
}

func TestReadBoundedBody_ExceedsLimit_ReturnsError(t *testing.T) {
	t.Parallel()

	data := make([]byte, maxResponseBodySize+1)
	for i := range data {
		data[i] = 'x'
	}

	body := bytes.NewReader(data)

	result, err := readBoundedBody(body)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), fmt.Sprintf("exceeds %d bytes", maxResponseBodySize))
}

func TestReadBoundedBody_ReadError_Propagates(t *testing.T) {
	t.Parallel()

	errRead := errors.New("disk I/O failure")
	body := &errorReader{err: errRead}

	result, err := readBoundedBody(body)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "read response body")
}

// errorReader is a test double that always returns an error on Read.
type errorReader struct {
	err error
}

func (e *errorReader) Read(_ []byte) (int, error) {
	return 0, e.err
}

// --- rejectEmptyOrNullBody tests ---

func TestRejectEmptyOrNullBody_ValidJSON_ReturnsNil(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte(`{"items":[]}`))

	assert.NoError(t, err)
}

func TestRejectEmptyOrNullBody_EmptyBytes_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte{})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "null/empty payload")
}

func TestRejectEmptyOrNullBody_NullString_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte("null"))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
	assert.Contains(t, err.Error(), "null/empty payload")
}

func TestRejectEmptyOrNullBody_WhitespaceOnly_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte("   \t\n  "))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestRejectEmptyOrNullBody_NullWithWhitespace_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte("  null  "))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestRejectEmptyOrNullBody_NonEmptyString_Success(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte(`"hello"`))

	assert.NoError(t, err)
}

func TestRejectEmptyOrNullBody_Array_Success(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte(`[]`))

	assert.NoError(t, err)
}

func TestRejectEmptyOrNullBody_NullAsPartOfValidJSON_Success(t *testing.T) {
	t.Parallel()

	// "nullified" is not "null" — trimmed value is "nullified", not "null".
	err := rejectEmptyOrNullBody([]byte(`nullified`))

	assert.NoError(t, err)
}

// --- isBreakerRejection tests ---

func TestIsBreakerRejection_OpenState_ReturnsTrue(t *testing.T) {
	t.Parallel()

	assert.True(t, isBreakerRejection(gobreaker.ErrOpenState))
}

func TestIsBreakerRejection_TooManyRequests_ReturnsTrue(t *testing.T) {
	t.Parallel()

	assert.True(t, isBreakerRejection(gobreaker.ErrTooManyRequests))
}

func TestIsBreakerRejection_WrappedOpenState_ReturnsTrue(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("circuit breaker: %w", gobreaker.ErrOpenState)

	assert.True(t, isBreakerRejection(wrapped))
}

func TestIsBreakerRejection_WrappedTooManyRequests_ReturnsTrue(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("circuit breaker: %w", gobreaker.ErrTooManyRequests)

	assert.True(t, isBreakerRejection(wrapped))
}

func TestIsBreakerRejection_OtherErrors_ReturnsFalse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"generic error", errors.New("connection refused")},
		{"nil error", nil},
		{"timeout error", errors.New("i/o timeout")},
		{"HTTP error", errors.New("HTTP 500")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.False(t, isBreakerRejection(tt.err))
		})
	}
}

// --- httpAttemptResult type tests ---

func TestHttpAttemptResult_HoldsBodyAndStatusCode(t *testing.T) {
	t.Parallel()

	result := &httpAttemptResult{
		body:       []byte(`{"id":"job-1"}`),
		statusCode: http.StatusOK,
	}

	assert.Equal(t, http.StatusOK, result.statusCode)
	assert.Equal(t, `{"id":"job-1"}`, string(result.body))
}

func TestHttpAttemptResult_NilBody(t *testing.T) {
	t.Parallel()

	result := &httpAttemptResult{
		body:       nil,
		statusCode: http.StatusNoContent,
	}

	assert.Nil(t, result.body)
	assert.Equal(t, http.StatusNoContent, result.statusCode)
}

// --- maxBackoffDelay constant test ---

func TestMaxBackoffDelay_Is30Seconds(t *testing.T) {
	t.Parallel()

	// Documents the constant's value so refactors that change it are intentional.
	assert.Equal(t, 30_000_000_000, int(maxBackoffDelay.Nanoseconds()))
}

// --- readBoundedBody with large payload streaming ---

func TestReadBoundedBody_StreamingExcessivePayload_ReturnsError(t *testing.T) {
	t.Parallel()

	// Use a reader that generates data lazily (not pre-allocated)
	// to verify the limit check works with streaming readers.
	body := io.LimitReader(
		&infiniteReader{b: 'A'},
		int64(maxResponseBodySize)+100,
	)

	result, err := readBoundedBody(body)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

// infiniteReader produces an endless stream of the same byte.
type infiniteReader struct {
	b byte
}

func (r *infiniteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
	}

	return len(p), nil
}

// --- rejectEmptyOrNullBody edge cases ---

func TestRejectEmptyOrNullBody_NilSlice_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestRejectEmptyOrNullBody_SingleSpace_ReturnsError(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte(" "))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}

func TestRejectEmptyOrNullBody_ValidObject_Success(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte(`{"key":"value"}`))

	assert.NoError(t, err)
}

func TestRejectEmptyOrNullBody_BooleanFalse_Success(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte("false"))

	assert.NoError(t, err)
}

func TestRejectEmptyOrNullBody_NumberZero_Success(t *testing.T) {
	t.Parallel()

	err := rejectEmptyOrNullBody([]byte("0"))

	assert.NoError(t, err)
}

// --- readBoundedBody boundary ---

func TestReadBoundedBody_OneBelowLimit_Success(t *testing.T) {
	t.Parallel()

	data := strings.Repeat("x", maxResponseBodySize-1)

	result, err := readBoundedBody(strings.NewReader(data))

	require.NoError(t, err)
	assert.Len(t, result, maxResponseBodySize-1)
}

func TestReadBoundedBody_OneAboveLimit_ReturnsError(t *testing.T) {
	t.Parallel()

	data := strings.Repeat("x", maxResponseBodySize+1)

	result, err := readBoundedBody(strings.NewReader(data))

	require.Error(t, err)
	assert.Nil(t, result)
	assert.ErrorIs(t, err, ErrFetcherBadResponse)
}
