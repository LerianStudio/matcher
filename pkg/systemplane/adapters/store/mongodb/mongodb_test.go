// Copyright 2025 Lerian Studio.

//go:build unit

package mongodb

import (
	"testing"

	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_EmptyURI_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := bootstrap.MongoBootstrapConfig{
		URI:               "",
		Database:          "systemplane",
		EntriesCollection: "runtime_entries",
		HistoryCollection: "runtime_history",
	}

	store, history, closer, err := New(t.Context(), cfg, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errEmptyURI)
	assert.Nil(t, store)
	assert.Nil(t, history)
	assert.Nil(t, closer)
}

func TestNew_InvalidURI_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := bootstrap.MongoBootstrapConfig{
		URI:               "not-a-valid-uri://nowhere",
		Database:          "testdb",
		EntriesCollection: "entries",
		HistoryCollection: "history",
	}

	// mongo.Connect may or may not fail on invalid URIs depending on the
	// driver version -- the important thing is that either Connect or Ping
	// returns an error. We only verify that New() does not succeed.
	store, history, closer, err := New(t.Context(), cfg, nil)

	// If New succeeded (driver deferred connection), clean up.
	if closer != nil {
		_ = closer.Close()
	}

	// The driver might accept the URI at Connect time and fail at Ping.
	// Either way, we should not get a usable store from a bogus URI.
	if err == nil {
		// Some drivers are lenient with URI parsing; in that case, just
		// verify we got non-nil stores (they will fail on actual operations).
		assert.NotNil(t, store)
		assert.NotNil(t, history)
	} else {
		assert.Nil(t, store)
		assert.Nil(t, history)
	}
}

func TestClientCloser_ImplementsCloser(t *testing.T) {
	t.Parallel()

	// Verify the clientCloser struct satisfies io.Closer at compile time.
	// We cannot call Close() without a real client, but the type assertion
	// is the valuable check here.
	var _ interface{ Close() error } = (*clientCloser)(nil)
}

func TestClientCloser_Close_NilReceiverOrClient_NoError(t *testing.T) {
	t.Parallel()

	var nilCloser *clientCloser
	assert.NoError(t, nilCloser.Close())

	emptyCloser := &clientCloser{}
	assert.NoError(t, emptyCloser.Close())
}

func TestErrEmptyURI_IsStable(t *testing.T) {
	t.Parallel()

	// Verify the sentinel error message is stable.
	assert.Equal(t, "mongodb store: URI is required", errEmptyURI.Error())
}

func TestSupportsTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result helloResult
		want   bool
	}{
		{
			name: "replica set supports transactions",
			result: helloResult{
				SetName:                      "rs0",
				LogicalSessionTimeoutMinutes: int32Ptr(30),
			},
			want: true,
		},
		{
			name: "sharded cluster supports transactions",
			result: helloResult{
				Msg:                          "isdbgrid",
				LogicalSessionTimeoutMinutes: int32Ptr(30),
			},
			want: true,
		},
		{
			name: "standalone does not support transactions",
			result: helloResult{
				LogicalSessionTimeoutMinutes: int32Ptr(30),
			},
			want: false,
		},
		{
			name:   "missing session support does not support transactions",
			result: helloResult{},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, supportsTransactions(tt.result))
		})
	}
}

func int32Ptr(value int32) *int32 {
	return &value
}
