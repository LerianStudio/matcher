//go:build unit

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProbe_HealthyServer_ReturnsZero(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	result := probe(srv.URL)
	assert.Equal(t, 0, result)
}

func TestProbe_ServiceUnavailable_ReturnsOne(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	result := probe(srv.URL)
	assert.Equal(t, 1, result)
}

func TestProbe_ConnectionRefused_ReturnsOne(t *testing.T) {
	t.Parallel()

	// Use a URL with a port that is guaranteed to be closed.
	result := probe("http://127.0.0.1:1")
	assert.Equal(t, 1, result)
}
