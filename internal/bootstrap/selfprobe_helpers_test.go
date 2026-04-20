// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

// Test-only helpers for the startup self-probe package-private state. Lives
// in a *_test.go file so it is NOT compiled into the production binary —
// per the Ring testing-anti-patterns skill, production files must not carry
// test-only symbols.
//
// Tests mutate the package-level `selfProbeOK` atomic via this helper so
// neighbouring tests (and cross-file tests in the same package) start from a
// known state. No locking is needed beyond the atomic itself: every caller
// runs serially under `//nolint:paralleltest` annotations on the test cases
// that touch global state.

// resetSelfProbeStateForTest clears the package-level self-probe flag so
// tests are order-independent.
func resetSelfProbeStateForTest() {
	selfProbeOK.Store(false)
}
