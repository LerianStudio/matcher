// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package ports

import (
	"context"
	"errors"

	"github.com/LerianStudio/matcher/internal/exception/domain/services"
)

//go:generate mockgen -destination=mocks/external_connector_mock.go -package=mocks . ExternalConnector

// ErrConnectorNotConfigured indicates that the selected routing target has no connector configuration.
var ErrConnectorNotConfigured = errors.New("connector not configured for target")

// DispatchResult contains the result of an external dispatch operation.
type DispatchResult struct {
	Target            services.RoutingTarget
	ExternalReference string
	Acknowledged      bool
}

// ExternalConnector dispatches exceptions to external systems.
type ExternalConnector interface {
	// Dispatch sends the exception to an external system based on the routing decision.
	// The payload must be a JSON-encoded byte slice representing the exception data
	// formatted for the target system (e.g., JIRA issue payload, webhook event).
	Dispatch(
		ctx context.Context,
		exceptionID string,
		decision services.RoutingDecision,
		payload []byte,
	) (DispatchResult, error)
}
