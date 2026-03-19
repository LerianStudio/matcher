// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// reload returns a ReloadResult describing the current state.
//
// In seed mode (the normal runtime state after systemplane initialization),
// reload returns immediately — the systemplane supervisor owns all runtime
// configuration changes.
//
// Outside seed mode (the brief window between bootstrap and systemplane init),
// reload is a no-op since file-based config is not supported. The caller
// should use the systemplane API once it is initialized.
func (cm *ConfigManager) reload() (*ReloadResult, error) {
	if cm.InSeedMode() {
		return &ReloadResult{
			Version:    cm.version.Load(),
			ReloadedAt: cm.LastReloadAt(),
			Skipped:    true,
			Reason:     "superseded by systemplane",
		}, nil
	}

	// Non-seed-mode reload is a no-op. File-based configuration is not
	// supported — use the systemplane API for runtime changes.
	return &ReloadResult{
		Version:    cm.version.Load(),
		ReloadedAt: cm.LastReloadAt(),
		Skipped:    true,
		Reason:     "file reload not supported; use systemplane API",
	}, nil
}
