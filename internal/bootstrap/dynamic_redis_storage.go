// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/net/http/ratelimit"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/gofiber/fiber/v2"
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

func (storage *dynamicRedisStorage) Get(key string) ([]byte, error) {
	return storage.current().Get(key)
}

func (storage *dynamicRedisStorage) Set(key string, val []byte, exp time.Duration) error {
	return storage.current().Set(key, val, exp)
}

func (storage *dynamicRedisStorage) Delete(key string) error {
	return storage.current().Delete(key)
}

func (storage *dynamicRedisStorage) Reset() error {
	return storage.current().Reset()
}

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
