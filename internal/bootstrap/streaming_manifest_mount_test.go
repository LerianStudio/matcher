// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streamingbootstrap "github.com/LerianStudio/matcher/internal/streaming/bootstrap"
	streamingcatalog "github.com/LerianStudio/matcher/internal/streaming/catalog"
)

func TestMountStreamingManifestAPI_AuthEnabledMissingTokenReturns401(t *testing.T) {
	app, _ := mountSystemplaneForCRUDTest(t, true)
	bundle := newStreamingManifestTestBundle(t)

	require.NoError(t, MountStreamingManifestAPI(app, bundle, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, StreamingManifestRoutePath, http.NoBody))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestMountStreamingManifestAPI_PublicStreamingRouteIsNotMounted(t *testing.T) {
	app, _ := mountSystemplaneForCRUDTest(t, false)
	bundle := newStreamingManifestTestBundle(t)

	require.NoError(t, MountStreamingManifestAPI(app, bundle, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/streaming", http.NoBody))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMountStreamingManifestAPI_ManifestContainsCatalogAndPublisherMetadata(t *testing.T) {
	app, _ := mountSystemplaneForCRUDTest(t, false)
	bundle := newStreamingManifestTestBundle(t)

	require.NoError(t, MountStreamingManifestAPI(app, bundle, nil))

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, StreamingManifestRoutePath, http.NoBody))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

	var manifest struct {
		Version   string `json:"version"`
		Publisher struct {
			ServiceName     string `json:"serviceName"`
			SourceBase      string `json:"sourceBase"`
			RoutePath       string `json:"routePath"`
			OutboxSupported bool   `json:"outboxSupported"`
			AppVersion      string `json:"appVersion"`
			LibVersion      string `json:"libVersion"`
			ProducerID      string `json:"producerId"`
		} `json:"publisher"`
		Events []struct {
			Key           string `json:"key"`
			ResourceType  string `json:"resourceType"`
			EventType     string `json:"eventType"`
			DefaultPolicy struct {
				Enabled bool   `json:"enabled"`
				Direct  string `json:"direct"`
				Outbox  string `json:"outbox"`
				DLQ     string `json:"dlq"`
			} `json:"defaultPolicy"`
		} `json:"events"`
	}

	require.NoError(t, json.NewDecoder(resp.Body).Decode(&manifest))

	assert.Equal(t, "1.0.0", manifest.Version)
	assert.Equal(t, "matcher", manifest.Publisher.ServiceName)
	assert.Equal(t, "matcher", manifest.Publisher.SourceBase)
	assert.Equal(t, StreamingManifestRoutePath, manifest.Publisher.RoutePath)
	assert.True(t, manifest.Publisher.OutboxSupported)
	assert.NotEmpty(t, manifest.Publisher.AppVersion)
	assert.NotEmpty(t, manifest.Publisher.LibVersion)
	assert.Empty(t, manifest.Publisher.ProducerID, "noop emitter should not fabricate a producer id")
	require.Len(t, manifest.Events, 46)

	byKey := make(map[string]struct {
		ResourceType string
		EventType    string
		Direct       string
		Outbox       string
		DLQ          string
	}, len(manifest.Events))
	for _, event := range manifest.Events {
		byKey[event.Key] = struct {
			ResourceType string
			EventType    string
			Direct       string
			Outbox       string
			DLQ          string
		}{
			ResourceType: event.ResourceType,
			EventType:    event.EventType,
			Direct:       event.DefaultPolicy.Direct,
			Outbox:       event.DefaultPolicy.Outbox,
			DLQ:          event.DefaultPolicy.DLQ,
		}
	}

	created := byKey["reconciliation_context.created"]
	assert.Equal(t, "reconciliation_context", created.ResourceType)
	assert.Equal(t, "created", created.EventType)
	assert.Equal(t, "direct", created.Direct)
	assert.Equal(t, "fallback_on_circuit_open", created.Outbox)
	assert.Equal(t, "on_routable_failure", created.DLQ)

	archived := byKey["audit_log.created"]
	assert.Equal(t, "skip", archived.Direct)
	assert.Equal(t, "always", archived.Outbox)
	assert.Equal(t, "on_routable_failure", archived.DLQ)
}

func newStreamingManifestTestBundle(t *testing.T) streamingbootstrap.ProducerBundle {
	t.Helper()

	catalog, err := streamingcatalog.NewCatalog()
	require.NoError(t, err)

	return streamingbootstrap.ProducerBundle{Catalog: catalog}
}
