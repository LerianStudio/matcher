// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import "github.com/LerianStudio/matcher/pkg/systemplane/domain"

func concatKeyDefs(groups ...[]domain.KeyDef) []domain.KeyDef {
	total := 0
	for _, group := range groups {
		total += len(group)
	}

	defs := make([]domain.KeyDef, 0, total)
	for _, group := range groups {
		defs = append(defs, group...)
	}

	return defs
}
