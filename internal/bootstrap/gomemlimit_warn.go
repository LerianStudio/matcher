// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"strings"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// recommendedMemLimitHeadroomPct mirrors the gomemlimitHeadroomPct used by
// the Fetcher bridge's applyGOMEMLIMIT and is quoted in the warning message
// so operators have a ready-made recommendation.
const recommendedMemLimitHeadroomPct = 85

// shouldWarnOnGOMEMLIMIT decides whether the current process is a candidate
// for a startup warning about an unset GOMEMLIMIT. Returns true when:
//   - GOMEMLIMIT is not set (or is whitespace-only), AND
//   - the memory-limit reader successfully discovered a non-zero cgroup
//     ceiling (i.e., we are running in a cgroup-capped container).
//
// Returns false on bare-metal / macOS / runtimes with no cgroup files, and
// when GOMEMLIMIT is already set explicitly.
//
// Pure function — no side effects — so it can be unit-tested without
// touching /sys/fs/cgroup or the process environment.
func shouldWarnOnGOMEMLIMIT(gomemlimit string, reader memoryLimitReader) bool {
	if strings.TrimSpace(gomemlimit) != "" {
		return false
	}

	if reader == nil {
		return false
	}

	limit, _, err := reader()
	if err != nil {
		return false
	}

	return limit > 0
}

// warnOnMissingGOMEMLIMIT emits a single WARN log line when the process is
// running in a cgroup-capped container without GOMEMLIMIT configured. The
// Go runtime's default soft memory limit is math.MaxInt64, which means the
// heap can grow beyond the cgroup ceiling before the GC reacts — the
// kernel's OOM killer wins that race.
//
// The Fetcher bridge sets GOMEMLIMIT itself when it initializes; this
// warning is the companion for non-Fetcher deployments and for any path
// that executes before applyGOMEMLIMIT runs.
func warnOnMissingGOMEMLIMIT(
	ctx context.Context,
	logger libLog.Logger,
	reader memoryLimitReader,
	gomemlimit string,
) {
	if logger == nil {
		return
	}

	if !shouldWarnOnGOMEMLIMIT(gomemlimit, reader) {
		return
	}

	limit, source, _ := reader()

	logger.Log(ctx, libLog.LevelWarn, fmt.Sprintf(
		"GOMEMLIMIT is not set but the process is running in a cgroup-capped "+
			"container (%d bytes detected from %s). Without GOMEMLIMIT the Go "+
			"runtime defaults its soft memory limit to math.MaxInt64 and can "+
			"grow the heap past the cgroup ceiling before GC reacts, risking "+
			"OOMKill. Set GOMEMLIMIT to ~%d%% of the pod memory limit via "+
			"downward API: env[].valueFrom.resourceFieldRef.resource=limits.memory.",
		limit, source, recommendedMemLimitHeadroomPct,
	))
}
