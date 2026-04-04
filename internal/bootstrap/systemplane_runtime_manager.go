// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	spports "github.com/LerianStudio/lib-commons/v4/commons/systemplane/ports"
	spregistry "github.com/LerianStudio/lib-commons/v4/commons/systemplane/registry"
	spservice "github.com/LerianStudio/lib-commons/v4/commons/systemplane/service"
)

type systemplaneRuntimeManager interface {
	spservice.Manager
	registry() spregistry.Registry
	store() spports.Store
	supervisor() spservice.Supervisor
}

type matcherSystemplaneRuntimeManager struct {
	spservice.Manager
	runtimeRegistry   spregistry.Registry
	runtimeStore      spports.Store
	runtimeSupervisor spservice.Supervisor
}

func newMatcherSystemplaneRuntimeManager(
	delegate spservice.Manager,
	reg spregistry.Registry,
	store spports.Store,
	supervisor spservice.Supervisor,
) spservice.Manager {
	if delegate == nil {
		return nil
	}

	if reg == nil || store == nil || supervisor == nil {
		return delegate
	}

	return &matcherSystemplaneRuntimeManager{
		Manager:           delegate,
		runtimeRegistry:   reg,
		runtimeStore:      store,
		runtimeSupervisor: supervisor,
	}
}

func (manager *matcherSystemplaneRuntimeManager) registry() spregistry.Registry {
	if manager == nil {
		return nil
	}

	return manager.runtimeRegistry
}

func (manager *matcherSystemplaneRuntimeManager) store() spports.Store {
	if manager == nil {
		return nil
	}

	return manager.runtimeStore
}

func (manager *matcherSystemplaneRuntimeManager) supervisor() spservice.Supervisor {
	if manager == nil {
		return nil
	}

	return manager.runtimeSupervisor
}

var _ systemplaneRuntimeManager = (*matcherSystemplaneRuntimeManager)(nil)
