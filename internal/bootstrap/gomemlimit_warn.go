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

	if strings.TrimSpace(gomemlimit) != "" || reader == nil {
		return
	}

	limit, source, err := reader()
	if err != nil || limit <= 0 {
		return
	}

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
