// Package dto provides data transfer objects for discovery HTTP handlers.
package dto

// RefreshDiscoveryRequest is the request body for POST /v1/discovery/refresh.
// Currently empty but included for future extensibility.
type RefreshDiscoveryRequest struct{}

// TestConnectionRequest is the request body for POST /v1/discovery/connections/:connectionId/test.
// Currently empty -- connection ID comes from path param.
type TestConnectionRequest struct{}
