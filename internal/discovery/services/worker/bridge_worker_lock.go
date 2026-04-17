// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

// acquireLock is a thin wrapper over the infrastructure provider's Redis
// client. Mirrors the pattern in scheduler/archival/discovery workers.
func (worker *BridgeWorker) acquireLock(ctx context.Context) (bool, string, error) {
	connLease, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis connection: %w", err)
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return false, "", ErrBridgeRedisConnectionNil
	}

	rdb, err := conn.GetClient(ctx)
	if err != nil {
		return false, "", fmt.Errorf("get redis client: %w", err)
	}

	token := uuid.New().String()

	ok, err := rdb.SetNX(ctx, bridgeWorkerLockKey, token, bridgeLockTTL(worker.cfg.Interval)).Result()
	if err != nil {
		return false, "", fmt.Errorf("redis setnx for bridge lock: %w", err)
	}

	return ok, token, nil
}

// releaseLock uses a Lua script to avoid releasing a lock that has already
// expired and been re-acquired by another instance.
func (worker *BridgeWorker) releaseLock(ctx context.Context, token string) {
	connLease, err := worker.infraProvider.GetRedisConnection(ctx)
	if err != nil {
		worker.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to get redis connection for lock release")

		return
	}
	defer connLease.Release()

	conn := connLease.Connection()
	if conn == nil {
		return
	}

	rdb, clientErr := conn.GetClient(ctx)
	if clientErr != nil {
		worker.logger.With(libLog.Any("error", clientErr.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to get redis client for lock release")

		return
	}

	if _, err := rdb.Eval(ctx, redisLockReleaseLua, []string{bridgeWorkerLockKey}, token).Result(); err != nil {
		worker.logger.With(libLog.String("error", err.Error())).
			Log(ctx, libLog.LevelWarn, "bridge: failed to release lock")
	}
}
