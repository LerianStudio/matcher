// Package main provides a minimal static binary for container healthchecks.
//
// Distroless images have no shell, curl, or wget. This binary performs a
// simple HTTP GET against the application's /health endpoint and exits
// with code 0 on success or 1 on failure, which is exactly what Docker
// healthcheck expects.
//
// Usage (in docker-compose or Dockerfile HEALTHCHECK):
//
//	["CMD", "/health-probe"]
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	defaultHealthURL = "http://localhost:4018/health"
	timeout          = 5 * time.Second
)

func main() {
	url := defaultHealthURL
	if v := os.Getenv("HEALTH_PROBE_URL"); v != "" {
		url = v
	}

	os.Exit(probe(url))
}

func probe(url string) int {
	client := &http.Client{Timeout: timeout}

	// #nosec G704 -- health probe binary; URL is from env var validated by operator
	resp, err := client.Get(url) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "health probe failed: %v\n", err)
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health probe: status %d\n", resp.StatusCode)
		return 1
	}

	return 0
}
