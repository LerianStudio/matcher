// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"encoding/base64"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	ingestionCommand "github.com/LerianStudio/matcher/internal/ingestion/services/command"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

// stubObjectStorage is the minimal ObjectStorageClient needed to exercise
// the verified-artifact pipeline wiring. Every method returns zero
// values; wiring logic does not call any of them during bootstrap.
type stubObjectStorage struct{}

func (s *stubObjectStorage) Upload(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
) (string, error) {
	return "", nil
}

func (s *stubObjectStorage) UploadWithOptions(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
	_ ...storageopt.UploadOption,
) (string, error) {
	return "", nil
}

func (s *stubObjectStorage) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil //nolint:nilnil // test stub
}

func (s *stubObjectStorage) Delete(_ context.Context, _ string) error { return nil }

func (s *stubObjectStorage) GeneratePresignedURL(
	_ context.Context,
	_ string,
	_ time.Duration,
) (string, error) {
	return "", nil
}

func (s *stubObjectStorage) Exists(_ context.Context, _ string) (bool, error) { return false, nil }

// validBase64MasterKey returns a 32-byte master key encoded with
// std-base64 — the happy-path format operators will use.
func validBase64MasterKey() string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}

	return base64.StdEncoding.EncodeToString(raw)
}

func TestDecodeMasterKey_EmptyReturnsRequiredSentinel(t *testing.T) {
	t.Parallel()

	_, err := decodeMasterKey("")
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyRequired)
}

func TestDecodeMasterKey_WhitespaceOnlyTreatedAsEmpty(t *testing.T) {
	t.Parallel()

	_, err := decodeMasterKey("   \n  ")
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyRequired)
}

func TestDecodeMasterKey_NotBase64ReturnsInvalid(t *testing.T) {
	t.Parallel()

	_, err := decodeMasterKey("!!!not base64!!!")
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyInvalid)
}

func TestDecodeMasterKey_ShortKeyReturnsInvalid(t *testing.T) {
	t.Parallel()

	short := base64.StdEncoding.EncodeToString(make([]byte, 16))
	_, err := decodeMasterKey(short)
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyInvalid)
}

func TestDecodeMasterKey_HappyPathStdEncoding(t *testing.T) {
	t.Parallel()

	decoded, err := decodeMasterKey(validBase64MasterKey())
	require.NoError(t, err)
	require.Len(t, decoded, 32)
}

func TestDecodeMasterKey_HappyPathURLEncoding(t *testing.T) {
	t.Parallel()

	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i * 3)
	}

	decoded, err := decodeMasterKey(base64.URLEncoding.EncodeToString(raw))
	require.NoError(t, err)
	require.Equal(t, raw, decoded)
}

func TestInitFetcherBridgeAdapters_RejectsNilLogger(t *testing.T) {
	t.Parallel()

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{})
	require.Nil(t, bundle)
	require.ErrorIs(t, err, errFetcherBridgeMissingLogger)
}

func TestInitFetcherBridgeAdapters_MissingIngestionReturnsNil(t *testing.T) {
	t.Parallel()

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err)
	require.Nil(t, bundle, "bridge is soft-disabled when ingestion is missing")
}

func TestInitFetcherBridgeAdapters_MissingExtractionRepoReturnsNil(t *testing.T) {
	t.Parallel()

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{
		Logger:           &libLog.NopLogger{},
		IngestionUseCase: &ingestionCommand.UseCase{},
	})
	require.NoError(t, err)
	require.Nil(t, bundle, "bridge is soft-disabled when extraction repo is missing")
}

// TestInitFetcherBridgeAdapters_EmptyAppEncKey_SoftDisablesVerifiedPipeline
// asserts the documented behaviour: an empty APP_ENC_KEY does not block
// bootstrap; it only leaves the verified-artifact pipeline unwired. The
// T-001 intake path still works. The caller (T-003 bridge worker) is
// expected to check bundle.VerifiedArtifactOrchestrator before use.
//
// This test does not exercise the full intake path because that would
// require a real ingestion UseCase with all its repositories; the
// important invariant here is that empty APP_ENC_KEY must not surface as
// an error.
func TestInitFetcherBridgeAdapters_EmptyAppEncKey_SoftDisablesVerifiedPipeline(t *testing.T) {
	t.Parallel()

	// We cannot exercise the full success path without a populated
	// ingestion UseCase and a real extraction repository. What we CAN
	// assert is that decodeMasterKey alone produces the documented
	// soft-disable signal when APP_ENC_KEY is empty.
	_, err := decodeMasterKey("")
	assert.ErrorIs(t, err, ErrFetcherBridgeMasterKeyRequired)
}

// TestInitFetcherBridgeAdapters_HappyPath_WiresFullBundle drives the full
// successful construction path of initFetcherBridgeAdapters: non-nil
// ingestion UseCase, non-nil extraction repository, object storage,
// valid APP_ENC_KEY. Exercises the sections below the nil-guard early
// returns (intake adapter, link writer adapter, verified-artifact
// pipeline, describe log line) that the other tests bypass via
// short-circuit branches.
func TestInitFetcherBridgeAdapters_HappyPath_WiresFullBundle(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = validBase64MasterKey()
	cfg.Fetcher.RequestTimeoutSec = 15

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{
		Config:           cfg,
		IngestionUseCase: &ingestionCommand.UseCase{},
		ExtractionRepo:   &discoveryExtractionRepo.Repository{},
		ObjectStorage:    &stubObjectStorage{},
		Logger:           &libLog.NopLogger{},
	})
	require.NoError(t, err)
	require.NotNil(t, bundle)

	// T-001 intake path wired.
	assert.NotNil(t, bundle.Intake, "trusted intake adapter constructed")
	assert.NotNil(t, bundle.LinkWrite, "extraction lifecycle link writer constructed")

	// T-002 verified-artifact pipeline wired.
	assert.NotNil(t, bundle.ArtifactRetrieval, "artifact retrieval gateway constructed")
	assert.NotNil(t, bundle.ArtifactVerifier, "artifact verifier constructed")
	assert.NotNil(t, bundle.ArtifactCustody, "artifact custody store constructed")
	assert.NotNil(t,
		bundle.VerifiedArtifactOrchestrator,
		"verified-artifact orchestrator constructed",
	)
}

// TestInitFetcherBridgeAdapters_PipelineDisabled_IntakeStillWired asserts
// that a missing APP_ENC_KEY leaves the T-001 intake path intact while
// soft-disabling only the T-002 verified-artifact pipeline. This is the
// documented degradation contract: intake keeps running without verified
// retrieval.
func TestInitFetcherBridgeAdapters_PipelineDisabled_IntakeStillWired(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	// APP_ENC_KEY intentionally empty — pipeline soft-disables.

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{
		Config:           cfg,
		IngestionUseCase: &ingestionCommand.UseCase{},
		ExtractionRepo:   &discoveryExtractionRepo.Repository{},
		ObjectStorage:    &stubObjectStorage{},
		Logger:           &libLog.NopLogger{},
	})
	require.NoError(t, err)
	require.NotNil(t, bundle)

	// Intake path still wired…
	assert.NotNil(t, bundle.Intake)
	assert.NotNil(t, bundle.LinkWrite)

	// …but verified-artifact pipeline is disabled.
	assert.Nil(t, bundle.ArtifactRetrieval)
	assert.Nil(t, bundle.ArtifactVerifier)
	assert.Nil(t, bundle.ArtifactCustody)
	assert.Nil(t, bundle.VerifiedArtifactOrchestrator)
}

// TestInitFetcherBridgeAdapters_InvalidKeyPropagates asserts a malformed
// APP_ENC_KEY surfaces as a hard error from initFetcherBridgeAdapters
// itself (not swallowed by the nil-returning soft-disable branches). An
// operator who typed the key wrong must see the failure loud and clear.
func TestInitFetcherBridgeAdapters_InvalidKeyPropagates(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = "not-valid-base64!!!"

	bundle, err := initFetcherBridgeAdapters(context.Background(), FetcherBridgeDeps{
		Config:           cfg,
		IngestionUseCase: &ingestionCommand.UseCase{},
		ExtractionRepo:   &discoveryExtractionRepo.Repository{},
		ObjectStorage:    &stubObjectStorage{},
		Logger:           &libLog.NopLogger{},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyInvalid)
	assert.Nil(t, bundle, "no partial bundle on hard error")
}

// TestWireVerifiedArtifactPipeline_NilConfig_SkipsWiring asserts the
// pipeline wiring gracefully skips when config is nil. In production
// bootstrap cfg is always non-nil; this is purely a defensive guard
// against mis-wiring at callsites.
func TestWireVerifiedArtifactPipeline_NilConfig_SkipsWiring(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{}

	err := wireVerifiedArtifactPipeline(context.Background(), bundle, FetcherBridgeDeps{
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err)
	assert.Nil(t, bundle.VerifiedArtifactOrchestrator)
}

// TestWireVerifiedArtifactPipeline_NilStorage_SkipsWiring asserts the
// pipeline is left unwired when APP_ENC_KEY is valid but object storage
// is unavailable. Nowhere to write the custody copy means the pipeline
// cannot function, and we would rather warn than crash on startup.
func TestWireVerifiedArtifactPipeline_NilStorage_SkipsWiring(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{}

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = validBase64MasterKey()

	err := wireVerifiedArtifactPipeline(context.Background(), bundle, FetcherBridgeDeps{
		Config: cfg,
		Logger: &libLog.NopLogger{},
	})
	require.NoError(t, err)
	assert.Nil(t, bundle.VerifiedArtifactOrchestrator)
}

// TestWireVerifiedArtifactPipeline_ValidConfig_WiresOrchestrator asserts
// the happy-path wiring produces a non-nil orchestrator, retrieval
// client, verifier, and custody store.
func TestWireVerifiedArtifactPipeline_ValidConfig_WiresOrchestrator(t *testing.T) {
	t.Parallel()

	bundle := &FetcherBridgeAdapters{}

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = validBase64MasterKey()
	cfg.Fetcher.RequestTimeoutSec = 30

	err := wireVerifiedArtifactPipeline(context.Background(), bundle, FetcherBridgeDeps{
		Config:        cfg,
		ObjectStorage: &stubObjectStorage{},
		Logger:        &libLog.NopLogger{},
	})
	require.NoError(t, err)

	assert.NotNil(t, bundle.ArtifactRetrieval)
	assert.NotNil(t, bundle.ArtifactVerifier)
	assert.NotNil(t, bundle.ArtifactCustody)
	assert.NotNil(t, bundle.VerifiedArtifactOrchestrator)
}

// TestWireVerifiedArtifactPipeline_InvalidKey_Fails asserts invalid keys
// produce a hard error (not a soft disable). Operators who set the key
// but got it wrong should see a loud failure, not a silent feature
// disable.
func TestWireVerifiedArtifactPipeline_InvalidKey_Fails(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = "!!!not base64!!!"

	bundle := &FetcherBridgeAdapters{}

	err := wireVerifiedArtifactPipeline(context.Background(), bundle, FetcherBridgeDeps{
		Config:        cfg,
		ObjectStorage: &stubObjectStorage{},
		Logger:        &libLog.NopLogger{},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFetcherBridgeMasterKeyInvalid)
}

func TestDescribeBridgeWiring_WithOrchestrator(t *testing.T) {
	t.Parallel()

	// Smoke test on the log-line builder so we do not ship a typo into
	// operator-facing log output.
	bundle := &FetcherBridgeAdapters{}
	assert.Contains(t, describeBridgeWiring(bundle), "intake")
	assert.NotContains(t, describeBridgeWiring(bundle), "verified-artifact")

	cfg := &Config{}
	cfg.Fetcher.AppEncKey = validBase64MasterKey()

	fullBundle := &FetcherBridgeAdapters{}
	err := wireVerifiedArtifactPipeline(context.Background(), fullBundle, FetcherBridgeDeps{
		Config:        cfg,
		ObjectStorage: &stubObjectStorage{},
		Logger:        &libLog.NopLogger{},
	})
	require.NoError(t, err)

	assert.Contains(t, describeBridgeWiring(fullBundle), "verified-artifact")
}

func TestNewArtifactHTTPClient_DisablesRedirects(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.RequestTimeoutSec = 10

	client := newArtifactHTTPClient(cfg)
	require.NotNil(t, client)
	require.NotNil(t, client.CheckRedirect)

	// Any number of prior requests should result in ErrUseLastResponse so
	// the caller sees the raw 3xx status.
	err := client.CheckRedirect(nil, nil)
	assert.Error(t, err)
}

func TestNewArtifactHTTPClient_TimeoutHasPad(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.RequestTimeoutSec = 30

	client := newArtifactHTTPClient(cfg)
	require.NotNil(t, client)

	// Timeout equals configured + pad. Use Greater so future tuning of the
	// pad does not break this assertion.
	assert.Greater(
		t,
		client.Timeout,
		time.Duration(cfg.Fetcher.RequestTimeoutSec)*time.Second,
	)
}

// sharedPortsInterfaceCheck ensures the exported interface types still
// match. Kept lightweight as a compile-time probe; runtime effect is
// simply "the build stays clean".
var (
	_ sharedPorts.ArtifactRetrievalGateway = (*sharedPortsInterfaceCheckProbe)(nil)
	_ sharedPorts.ArtifactTrustVerifier    = (*sharedPortsInterfaceCheckProbe)(nil)
	_ sharedPorts.ArtifactCustodyStore     = (*sharedPortsInterfaceCheckProbe)(nil)
)

type sharedPortsInterfaceCheckProbe struct{}

func (p *sharedPortsInterfaceCheckProbe) Retrieve(
	_ context.Context,
	_ sharedPorts.ArtifactRetrievalDescriptor,
) (*sharedPorts.ArtifactRetrievalResult, error) {
	return nil, nil //nolint:nilnil // compile probe
}

func (p *sharedPortsInterfaceCheckProbe) VerifyAndDecrypt(
	_ context.Context,
	_ io.Reader,
	_ string,
	_ string,
) (io.Reader, error) {
	return nil, nil //nolint:nilnil // compile probe
}

func (p *sharedPortsInterfaceCheckProbe) Store(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyWriteInput,
) (*sharedPorts.ArtifactCustodyReference, error) {
	return nil, nil //nolint:nilnil // compile probe
}

func (p *sharedPortsInterfaceCheckProbe) Open(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) (io.ReadCloser, error) {
	return nil, nil //nolint:nilnil // compile probe
}

func (p *sharedPortsInterfaceCheckProbe) Delete(
	_ context.Context,
	_ sharedPorts.ArtifactCustodyReference,
) error {
	return nil
}
