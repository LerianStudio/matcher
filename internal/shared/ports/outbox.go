// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import "github.com/LerianStudio/lib-commons/v5/commons/outbox"

// OutboxRepository is a type alias for the canonical lib-commons/v5 outbox repository.
// All bounded contexts reference this alias so the canonical outbox internals
// remain transparent to callers.
type OutboxRepository = outbox.OutboxRepository
