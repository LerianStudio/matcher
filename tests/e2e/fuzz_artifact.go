//go:build e2e

package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// File/directory permission constants.
const (
	fuzzDirPermissions  = 0750
	fuzzFilePermissions = 0640
	maxSanitizedNameLen = 50
)

// Sentinel errors for fuzz artifact operations.
var errUnrecognizedFormat = errors.New("unrecognized format")

// sanitizeFilename replaces non-alphanumeric characters with underscores and truncates to maxLen.
var sanitizeRe = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func sanitizeFilename(s string, maxLen int) string {
	clean := sanitizeRe.ReplaceAllString(s, "_")
	clean = strings.Trim(clean, "_")

	if len(clean) > maxLen {
		clean = clean[:maxLen]
	}

	return clean
}

// WriteFuzzArtifact writes the full run artifact to a JSON file.
// Returns the path written.
func WriteFuzzArtifact(artifact *FuzzRunArtifact, dir string) (string, error) {
	if err := os.MkdirAll(dir, fuzzDirPermissions); err != nil {
		return "", fmt.Errorf("create artifact dir %s: %w", dir, err)
	}

	ts := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("run_%s_%s.json", ts, artifact.RunID)
	path := filepath.Join(dir, filename)

	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal fuzz artifact: %w", err)
	}

	if err := os.WriteFile(path, data, fuzzFilePermissions); err != nil {
		return "", fmt.Errorf("write fuzz artifact %s: %w", path, err)
	}

	return path, nil
}

// WriteFailureArtifact writes a single failed scenario to the failures directory.
// Returns the path written.
func WriteFailureArtifact(scenario *FuzzScenario, dir string) (string, error) {
	failDir := filepath.Join(dir, "failures")

	if err := os.MkdirAll(failDir, fuzzDirPermissions); err != nil {
		return "", fmt.Errorf("create failures dir %s: %w", failDir, err)
	}

	sanitized := sanitizeFilename(scenario.AttackVector, maxSanitizedNameLen)
	filename := fmt.Sprintf("fail_%s_%s.json", scenario.ID, sanitized)
	path := filepath.Join(failDir, filename)

	data, err := json.MarshalIndent(scenario, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal failure artifact: %w", err)
	}

	if err := os.WriteFile(path, data, fuzzFilePermissions); err != nil {
		return "", fmt.Errorf("write failure artifact %s: %w", path, err)
	}

	return path, nil
}

// LoadFuzzArtifact reads a fuzz artifact from a JSON file (for replay mode).
func LoadFuzzArtifact(path string) (*FuzzRunArtifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fuzz artifact %s: %w", path, err)
	}

	var artifact FuzzRunArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, fmt.Errorf("unmarshal fuzz artifact %s: %w", path, err)
	}

	return &artifact, nil
}

// LoadFuzzScenarios loads scenario(s) from a JSON file.
// Handles FuzzRunArtifact (extracts scenarios), single FuzzScenario, or []FuzzScenario.
func LoadFuzzScenarios(path string) ([]FuzzScenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fuzz scenarios %s: %w", path, err)
	}

	// Try FuzzRunArtifact first (has "scenarios" field).
	var artifact FuzzRunArtifact
	if err := json.Unmarshal(data, &artifact); err == nil && len(artifact.Scenarios) > 0 {
		return artifact.Scenarios, nil
	}

	// Try []FuzzScenario.
	var scenarios []FuzzScenario
	if err := json.Unmarshal(data, &scenarios); err == nil && len(scenarios) > 0 {
		return scenarios, nil
	}

	// Try single FuzzScenario.
	var single FuzzScenario
	if err := json.Unmarshal(data, &single); err == nil && single.ID != "" {
		return []FuzzScenario{single}, nil
	}

	return nil, fmt.Errorf("unable to parse fuzz scenarios from %s: %w", path, errUnrecognizedFormat)
}

// LoadRegressionScenarios loads all .json files from the regressions directory.
func LoadRegressionScenarios(dir string) ([]FuzzScenario, error) {
	pattern := filepath.Join(dir, "regressions", "*.json")

	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob regression files %s: %w", pattern, err)
	}

	var all []FuzzScenario

	for _, f := range files {
		scenarios, err := LoadFuzzScenarios(f)
		if err != nil {
			return nil, fmt.Errorf("load regression file %s: %w", f, err)
		}

		all = append(all, scenarios...)
	}

	return all, nil
}
