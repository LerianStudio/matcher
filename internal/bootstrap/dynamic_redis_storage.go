// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"

	"github.com/LerianStudio/lib-commons/v4/commons/net/http/ratelimit"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

type dynamicRedisStorage struct {
	getter        func() *libRedis.Client
	fallback      fiber.Storage
	cachedStorage fiber.Storage
	lastClient    *libRedis.Client
}

func newDynamicRedisStorage(getter func() *libRedis.Client, fallback *libRedis.Client) fiber.Storage {
	if getter == nil {
		return ratelimit.NewRedisStorage(fallback)
	}

	return &dynamicRedisStorage{
		getter:   getter,
		fallback: ratelimit.NewRedisStorage(fallback),
	}
}

// Get retrieves a value from the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Get(key string) ([]byte, error) {
	delegate := storage.current()
	if delegate == nil {
		return nil, nil // rate-limit storage unavailable: permit the request
	}

	value, err := delegate.Get(key)
	if err != nil {
		return nil, fmt.Errorf("get redis storage value: %w", err)
	}

	return value, nil
}

// Set stores a value in the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Set(key string, val []byte, exp time.Duration) error {
	delegate := storage.current()
	if delegate == nil {
		return nil // rate-limit storage unavailable: permit the request
	}

	if err := delegate.Set(key, val, exp); err != nil {
		return fmt.Errorf("set redis storage value: %w", err)
	}

	return nil
}

// Delete removes a value from the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Delete(key string) error {
	delegate := storage.current()
	if delegate == nil {
		return nil // rate-limit storage unavailable: permit the request
	}

	if err := delegate.Delete(key); err != nil {
		return fmt.Errorf("delete redis storage value: %w", err)
	}

	return nil
}

// Reset clears the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Reset() error {
	delegate := storage.current()
	if delegate == nil {
		return nil // rate-limit storage unavailable: permit the request
	}

	if err := delegate.Reset(); err != nil {
		return fmt.Errorf("reset redis storage: %w", err)
	}

	return nil
}

// Close leaves the shared storage backend open for the owning runtime bundle.
func (storage *dynamicRedisStorage) Close() error {
	return nil
}

func (storage *dynamicRedisStorage) current() fiber.Storage {
	if storage == nil {
		return nil
	}

	if storage.getter != nil {
		if client := storage.getter(); client != nil {
			// Reuse cached wrapper when the underlying client hasn't changed.
			if client == storage.lastClient && storage.cachedStorage != nil {
				return storage.cachedStorage
			}

			storage.cachedStorage = ratelimit.NewRedisStorage(client)
			storage.lastClient = client

			return storage.cachedStorage
		}
	}

	return storage.fallback
}
