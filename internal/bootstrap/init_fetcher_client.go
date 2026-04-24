// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

// HTTP transport + object-storage probe used by the Fetcher bridge. Split
// from init_fetcher_bridge.go so the transport concerns (SSRF guard, redirect
// policy, probe bounds) stay together and are callable by the adapter /
// worker wiring without pulling in the custody and cgroup-memory code.

package bootstrap

import (
	"context"
	"net/http"
	"time"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	"github.com/LerianStudio/matcher/internal/shared/objectstorage"
)

// artifactRetrievalTimeoutPadSec is the additional allowance we apply on
// top of the extraction request timeout when sizing the artifact
// download HTTP client. Downloading a completed artifact is I/O bound;
// allow enough headroom for a slow S3 round-trip on top of whatever the
// operator has configured for extraction polling.
const artifactRetrievalTimeoutPadSec = 60

// newArtifactHTTPClient builds an http.Client suited for artifact
// downloads. We explicitly disable redirect following so any Fetcher
// misconfiguration that emits a 3xx surfaces as a retrieval failure
// rather than silently following a redirect to an attacker-controlled
// host.
//
// T-003 P2 hardening: the transport reuses the SSRF-guarded DialContext
// from the shared fetcher HTTP client config so artifact downloads
// cannot bypass the private-IP guard. Without this, Fetcher could
// redirect matcher into pulling from 169.254.169.254/latest/meta-data/
// or any internal service. We also bump MaxIdleConnsPerHost so bursty
// concurrent bridge work doesn't starve the connection pool.
func newArtifactHTTPClient(cfg *Config) *http.Client {
	timeout := time.Duration(cfg.Fetcher.RequestTimeoutSec+artifactRetrievalTimeoutPadSec) * time.Second

	// Reuse the SSRF-guarded transport from the fetcher HTTP client so
	// artifact downloads inherit the same private-IP protection.
	clientCfg := fetcher.DefaultConfig()
	clientCfg.BaseURL = cfg.Fetcher.URL
	clientCfg.AllowPrivateIPs = cfg.Fetcher.AllowPrivateIPs
	clientCfg.RequestTimeout = timeout

	transport := fetcher.BuildArtifactTransport(clientCfg)

	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// objectStorageAvailable runs a trial Exists call against a sentinel key to
// verify that the dynamic object storage wrapper has a usable delegate at
// runtime. The dynamicObjectStorageClient always returns a non-nil pointer,
// so the bare `deps.ObjectStorage == nil` check never fires. This probe
// surfaces the "configured but unreachable" state at bootstrap time
// instead of deferring the discovery to the first real custody write.
//
// A timeout bounds the probe so a transient storage outage at startup does
// not block the whole service from coming up.
//
// T-003 P5 hardening.
func objectStorageAvailable(ctx context.Context, client objectstorage.Backend) bool {
	if client == nil {
		return false
	}

	probeCtx, cancel := context.WithTimeout(ctx, objectStorageProbeTimeout)
	defer cancel()

	// A non-existent sentinel key is a cheap probe: storage backends
	// respond with a quick "does not exist" and do NOT fail unless
	// credentials/connectivity are broken.
	_, err := client.Exists(probeCtx, objectStorageProbeKey)

	return err == nil
}

const (
	objectStorageProbeKey     = "matcher/bootstrap/probe.keep"
	objectStorageProbeTimeout = 5 * time.Second
)
