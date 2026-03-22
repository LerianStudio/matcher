// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
)

func TestDynamicRedisStorage_SwitchesToCurrentRedisClient(t *testing.T) {
	t.Parallel()

	serverA := miniredis.RunT(t)
	serverB := miniredis.RunT(t)

	clientA := mustCreateTestRedisClient(t, serverA.Addr())
	clientB := mustCreateTestRedisClient(t, serverB.Addr())
	defer func() { _ = clientA.Close() }()
	defer func() { _ = clientB.Close() }()

	active := clientA
	storage := newDynamicRedisStorage(func() *libRedis.Client { return active }, clientA)

	require.NoError(t, storage.Set("key", []byte("from-a"), time.Minute))
	value, err := storage.Get("key")
	require.NoError(t, err)
	assert.Equal(t, []byte("from-a"), value)

	active = clientB
	value, err = storage.Get("key")
	require.NoError(t, err)
	assert.Nil(t, value)

	require.NoError(t, storage.Set("key", []byte("from-b"), time.Minute))
	value, err = storage.Get("key")
	require.NoError(t, err)
	assert.Equal(t, []byte("from-b"), value)
}

func mustCreateTestRedisClient(t *testing.T, addr string) *libRedis.Client {
	t.Helper()

	client, err := libRedis.New(context.Background(), libRedis.Config{
		Topology: libRedis.Topology{
			Standalone: &libRedis.StandaloneTopology{Address: addr},
		},
	})
	require.NoError(t, err)

	return client
}
