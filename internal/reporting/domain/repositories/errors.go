// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package repositories provides reporting persistence contracts.
package repositories

import "errors"

// ErrExportJobNotFound is returned when an export job is not found.
var ErrExportJobNotFound = errors.New("export job not found")
