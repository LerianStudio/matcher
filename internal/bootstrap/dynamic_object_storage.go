// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
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

// Upload delegates object upload to the current runtime storage client.
func (client *dynamicObjectStorageClient) Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for upload: %w", err)
	}

	result, err := delegate.Upload(ctx, key, reader, contentType)
	if err != nil {
		return "", fmt.Errorf("upload object: %w", err)
	}

	return result, nil
}

// UploadWithOptions delegates object upload with options to the current runtime storage client.
func (client *dynamicObjectStorageClient) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...storageopt.UploadOption) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for upload with options: %w", err)
	}

	result, err := delegate.UploadWithOptions(ctx, key, reader, contentType, opts...)
	if err != nil {
		return "", fmt.Errorf("upload object with options: %w", err)
	}

	return result, nil
}

// Download delegates object download to the current runtime storage client.
func (client *dynamicObjectStorageClient) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	delegate, err := client.current()
	if err != nil {
		return nil, fmt.Errorf("resolve object storage client for download: %w", err)
	}

	reader, err := delegate.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download object: %w", err)
	}

	return reader, nil
}

// Delete delegates object deletion to the current runtime storage client.
func (client *dynamicObjectStorageClient) Delete(ctx context.Context, key string) error {
	delegate, err := client.current()
	if err != nil {
		return fmt.Errorf("resolve object storage client for delete: %w", err)
	}

	if err := delegate.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	return nil
}

// GeneratePresignedURL delegates presigned URL generation to the current runtime storage client.
func (client *dynamicObjectStorageClient) GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	delegate, err := client.current()
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for presigned url: %w", err)
	}

	url, err := delegate.GeneratePresignedURL(ctx, key, expiry)
	if err != nil {
		return "", fmt.Errorf("generate presigned url: %w", err)
	}

	return url, nil
}

// Exists delegates object existence checks to the current runtime storage client.
func (client *dynamicObjectStorageClient) Exists(ctx context.Context, key string) (bool, error) {
	delegate, err := client.current()
	if err != nil {
		return false, fmt.Errorf("resolve object storage client for existence check: %w", err)
	}

	exists, err := delegate.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check object existence: %w", err)
	}

	return exists, nil
}

func (client *dynamicObjectStorageClient) current() (sharedPorts.ObjectStorageClient, error) {
	if client == nil {
		return nil, sharedPorts.ErrObjectStorageUnavailable
	}

	if client.getter != nil {
		if delegate := client.getter(); !sharedPorts.IsNilValue(delegate) {
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

	if !sharedPorts.IsNilValue(client.fallback) {
		return client.fallback, nil
	}

	return nil, sharedPorts.ErrObjectStorageUnavailable
}
