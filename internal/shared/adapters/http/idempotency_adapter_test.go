//go:build unit

package http

import (
	"context"
	"errors"
	"sync"
	"testing"

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	"github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vo "github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

var errRedisDown = errors.New("redis down")

// spyLogger captures log calls for test assertions. Implements log.Logger (v2).
type spyLogger struct {
	mu       sync.Mutex
	warnMsgs []string
}

func (l *spyLogger) Log(_ context.Context, level log.Level, msg string, _ ...log.Field) {
	if level == log.LevelWarn {
		l.mu.Lock()
		defer l.mu.Unlock()

		l.warnMsgs = append(l.warnMsgs, msg)
	}
}

//nolint:ireturn
func (l *spyLogger) With(_ ...log.Field) log.Logger { return l }

//nolint:ireturn
func (l *spyLogger) WithGroup(_ string) log.Logger { return l }
func (l *spyLogger) Enabled(_ log.Level) bool      { return true }
func (l *spyLogger) Sync(_ context.Context) error  { return nil }

func (l *spyLogger) warnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.warnMsgs)
}

func (l *spyLogger) lastWarn() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.warnMsgs) == 0 {
		return ""
	}

	return l.warnMsgs[len(l.warnMsgs)-1]
}

func contextWithSpyLogger(spy *spyLogger) context.Context {
	return libCommons.ContextWithLogger(context.Background(), spy)
}

type stubExceptionRepo struct {
	acquireResult   bool
	acquireErr      error
	cachedResult    *vo.IdempotencyResult
	getCachedErr    error
	markCompleteErr error
	markFailedErr   error
	reacquireResult bool
	reacquireErr    error
	returnNilResult bool

	lastKey vo.IdempotencyKey
}

func (repo *stubExceptionRepo) TryAcquire(_ context.Context, key vo.IdempotencyKey) (bool, error) {
	repo.lastKey = key

	return repo.acquireResult, repo.acquireErr
}

func (repo *stubExceptionRepo) MarkComplete(
	_ context.Context,
	key vo.IdempotencyKey,
	_ []byte,
	_ int,
) error {
	repo.lastKey = key

	return repo.markCompleteErr
}

func (repo *stubExceptionRepo) MarkFailed(_ context.Context, key vo.IdempotencyKey) error {
	repo.lastKey = key

	return repo.markFailedErr
}

func (repo *stubExceptionRepo) TryReacquireFromFailed(
	_ context.Context,
	key vo.IdempotencyKey,
) (bool, error) {
	repo.lastKey = key

	return repo.reacquireResult, repo.reacquireErr
}

func (repo *stubExceptionRepo) GetCachedResult(
	_ context.Context,
	key vo.IdempotencyKey,
) (*vo.IdempotencyResult, error) {
	repo.lastKey = key

	if repo.cachedResult != nil {
		return repo.cachedResult, repo.getCachedErr
	}

	if repo.returnNilResult {
		return nil, repo.getCachedErr
	}

	return &vo.IdempotencyResult{Status: vo.IdempotencyStatusUnknown}, repo.getCachedErr
}

func TestIdempotencyRepositoryAdapter_TryAcquire(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{acquireResult: true}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	acquired, err := adapter.TryAcquire(context.Background(), IdempotencyKey("test-key"))

	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, vo.IdempotencyKey("test-key"), exceptionRepo.lastKey)
}

func TestIdempotencyRepositoryAdapter_TryAcquire_Error(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{acquireErr: errRedisDown}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	acquired, err := adapter.TryAcquire(context.Background(), IdempotencyKey("test-key"))

	require.Error(t, err)
	assert.False(t, acquired)
}

func TestIdempotencyRepositoryAdapter_MarkComplete(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	err := adapter.MarkComplete(
		context.Background(),
		IdempotencyKey("test-key"),
		[]byte(`{"id":"123"}`),
		201,
	)

	require.NoError(t, err)
	assert.Equal(t, vo.IdempotencyKey("test-key"), exceptionRepo.lastKey)
}

func TestIdempotencyRepositoryAdapter_MarkFailed(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	err := adapter.MarkFailed(context.Background(), IdempotencyKey("test-key"))

	require.NoError(t, err)
	assert.Equal(t, vo.IdempotencyKey("test-key"), exceptionRepo.lastKey)
}

func TestIdempotencyRepositoryAdapter_TryReacquireFromFailed(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{reacquireResult: true}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	acquired, err := adapter.TryReacquireFromFailed(context.Background(), IdempotencyKey("test-key"))

	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, vo.IdempotencyKey("test-key"), exceptionRepo.lastKey)
}

func TestIdempotencyRepositoryAdapter_GetCachedResult_NilResult(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{returnNilResult: true}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	result, err := adapter.GetCachedResult(context.Background(), IdempotencyKey("test-key"))

	require.NoError(t, err)
	assert.Equal(t, IdempotencyStatusUnknown, result.Status)
}

func TestIdempotencyRepositoryAdapter_GetCachedResult(t *testing.T) {
	t.Parallel()

	exceptionRepo := &stubExceptionRepo{
		cachedResult: &vo.IdempotencyResult{
			Status:     vo.IdempotencyStatusComplete,
			Response:   []byte(`{"cached":"data"}`),
			HTTPStatus: 200,
		},
	}
	adapter := NewIdempotencyRepositoryAdapter(exceptionRepo)

	result, err := adapter.GetCachedResult(context.Background(), IdempotencyKey("test-key"))

	require.NoError(t, err)
	assert.Equal(t, IdempotencyStatusComplete, result.Status)
	assert.JSONEq(t, `{"cached":"data"}`, string(result.Response))
	assert.Equal(t, 200, result.HTTPStatus)
}

func TestIdempotencyRepositoryAdapter_NilRepo(t *testing.T) {
	t.Parallel()

	spy := &spyLogger{}
	ctx := contextWithSpyLogger(spy)
	adapter := NewIdempotencyRepositoryAdapter(nil)

	acquired, err := adapter.TryAcquire(ctx, IdempotencyKey("key"))
	require.NoError(t, err)
	assert.False(t, acquired)

	err = adapter.MarkComplete(ctx, IdempotencyKey("key"), nil, 0)
	require.NoError(t, err)

	err = adapter.MarkFailed(ctx, IdempotencyKey("key"))
	require.NoError(t, err)

	result, err := adapter.GetCachedResult(ctx, IdempotencyKey("key"))
	require.NoError(t, err)
	assert.Equal(t, IdempotencyStatusUnknown, result.Status)
}

func TestIdempotencyRepositoryAdapter_NilRepo_WarnsOnce(t *testing.T) {
	t.Parallel()

	spy := &spyLogger{}
	ctx := contextWithSpyLogger(spy)
	adapter := NewIdempotencyRepositoryAdapter(nil)

	// Call multiple methods that all trigger the nil-repo path.
	acquired, err := adapter.TryAcquire(ctx, IdempotencyKey("k1"))
	require.NoError(t, err)
	assert.False(t, acquired)

	require.NoError(t, adapter.MarkComplete(ctx, IdempotencyKey("k2"), nil, 0))
	require.NoError(t, adapter.MarkFailed(ctx, IdempotencyKey("k3")))

	result, err := adapter.GetCachedResult(ctx, IdempotencyKey("k4"))
	require.NoError(t, err)
	assert.Equal(t, IdempotencyStatusUnknown, result.Status)

	// Warning must be emitted exactly once (sync.Once).
	assert.Equal(t, 1, spy.warnCount())
	assert.Contains(t, spy.lastWarn(),
		"idempotency repository is nil, idempotency protection is disabled")
}

func TestIdempotencyRepositoryAdapter_NilRepo_NoLoggerInContext(t *testing.T) {
	t.Parallel()

	// When no logger is in the context, the adapter must not panic.
	adapter := NewIdempotencyRepositoryAdapter(nil)

	acquired, err := adapter.TryAcquire(context.Background(), IdempotencyKey("key"))
	require.NoError(t, err)
	assert.False(t, acquired)
}
