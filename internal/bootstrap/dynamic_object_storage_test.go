// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type stubObjectStorageClient struct {
	prefix string
}

func (client *stubObjectStorageClient) Upload(_ context.Context, key string, _ io.Reader, _ string) (string, error) {
	return client.prefix + key, nil
}

func (client *stubObjectStorageClient) UploadIfAbsent(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	return client.Upload(ctx, key, reader, contentType)
}

func (client *stubObjectStorageClient) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, _ ...sharedPorts.UploadOption) (string, error) {
	return client.Upload(ctx, key, reader, contentType)
}

func (client *stubObjectStorageClient) Download(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(client.prefix + key)), nil
}

func (client *stubObjectStorageClient) Delete(context.Context, string) error { return nil }

func (client *stubObjectStorageClient) GeneratePresignedURL(_ context.Context, key string, _ time.Duration) (string, error) {
	return client.prefix + key, nil
}

func (client *stubObjectStorageClient) Exists(_ context.Context, key string) (bool, error) {
	return strings.HasPrefix(client.prefix+key, client.prefix), nil
}

func TestDynamicObjectStorageClient_UsesFallbackUntilRuntimeAvailable(t *testing.T) {
	t.Parallel()

	fallback := &stubObjectStorageClient{prefix: "fallback:"}
	var active sharedPorts.ObjectStorageClient
	client := newDynamicObjectStorageClient(func() sharedPorts.ObjectStorageClient { return active }, fallback)

	url, err := client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "fallback:file", url)

	active = &stubObjectStorageClient{prefix: "runtime:"}
	url, err = client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "runtime:file", url)
}

func TestDynamicObjectStorageClient_FailsAfterRuntimeDelegateDisappears(t *testing.T) {
	t.Parallel()

	fallback := &stubObjectStorageClient{prefix: "fallback:"}
	active := sharedPorts.ObjectStorageClient(&stubObjectStorageClient{prefix: "runtime:"})
	client := newDynamicObjectStorageClient(func() sharedPorts.ObjectStorageClient { return active }, fallback)

	_, err := client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)

	active = nil
	_, err = client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}
