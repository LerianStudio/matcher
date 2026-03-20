// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"io"
	"sync"
	"time"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/pkg/storageopt"
)

type dynamicObjectStorageClient struct {
	getter          func() sharedPorts.ObjectStorageClient
	fallback        sharedPorts.ObjectStorageClient
	mu              sync.Mutex
	runtimeObserved bool
}

func newDynamicObjectStorageClient(
	getter func() sharedPorts.ObjectStorageClient,
	fallback sharedPorts.ObjectStorageClient,
) sharedPorts.ObjectStorageClient {
	if getter == nil {
		return fallback
	}

	return &dynamicObjectStorageClient{getter: getter, fallback: fallback}
}

func (client *dynamicObjectStorageClient) Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", err
	}

	return delegate.Upload(ctx, key, reader, contentType)
}

func (client *dynamicObjectStorageClient) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...storageopt.UploadOption) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", err
	}

	return delegate.UploadWithOptions(ctx, key, reader, contentType, opts...)
}

func (client *dynamicObjectStorageClient) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, err
	}

	return delegate.Download(ctx, key)
}

func (client *dynamicObjectStorageClient) Delete(ctx context.Context, key string) error {
	delegate, err := client.current()
	if err != nil {
		return err
	}

	return delegate.Delete(ctx, key)
}

func (client *dynamicObjectStorageClient) GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", err
	}

	return delegate.GeneratePresignedURL(ctx, key, expiry)
}

func (client *dynamicObjectStorageClient) Exists(ctx context.Context, key string) (bool, error) {
	delegate, err := client.current()
	if err != nil {
		return false, err
	}

	return delegate.Exists(ctx, key)
}

func (client *dynamicObjectStorageClient) current() (sharedPorts.ObjectStorageClient, error) {
	if client == nil {
		return nil, sharedPorts.ErrObjectStorageUnavailable
	}

	if client.getter != nil {
		if delegate := client.getter(); !isNilInterface(delegate) {
			client.mu.Lock()
			client.runtimeObserved = true
			client.mu.Unlock()
			return delegate, nil
		}

		client.mu.Lock()
		runtimeObserved := client.runtimeObserved
		client.mu.Unlock()
		if runtimeObserved {
			return nil, sharedPorts.ErrObjectStorageUnavailable
		}
	}

	if !isNilInterface(client.fallback) {
		return client.fallback, nil
	}

	return nil, sharedPorts.ErrObjectStorageUnavailable
}
