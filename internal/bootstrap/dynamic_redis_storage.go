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
	getter   func() *libRedis.Client
	fallback fiber.Storage
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
	value, err := storage.current().Get(key)
	if err != nil {
		return nil, fmt.Errorf("get redis storage value: %w", err)
	}

	return value, nil
}

// Set stores a value in the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Set(key string, val []byte, exp time.Duration) error {
	if err := storage.current().Set(key, val, exp); err != nil {
		return fmt.Errorf("set redis storage value: %w", err)
	}

	return nil
}

// Delete removes a value from the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Delete(key string) error {
	if err := storage.current().Delete(key); err != nil {
		return fmt.Errorf("delete redis storage value: %w", err)
	}

	return nil
}

// Reset clears the current rate-limit storage backend.
func (storage *dynamicRedisStorage) Reset() error {
	if err := storage.current().Reset(); err != nil {
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
			return ratelimit.NewRedisStorage(client)
		}
	}

	return storage.fallback
}
