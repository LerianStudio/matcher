//go:build chaos

package chaos

import (
	"fmt"
	"testing"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------
// PostgreSQL toxic injection
// --------------------------------------------------------------------------

// InjectPGLatency adds downstream latency to the PostgreSQL proxy.
// Every response from PostgreSQL is delayed by latencyMs milliseconds (+/- jitterMs).
func (h *ChaosHarness) InjectPGLatency(t *testing.T, latencyMs, jitterMs int) {
	t.Helper()

	_, err := h.PGProxy.AddToxic("pg_latency", "latency", "downstream", 1.0, toxiproxy.Attributes{
		"latency": latencyMs,
		"jitter":  jitterMs,
	})
	require.NoError(t, err, "inject PG latency toxic")

	t.Cleanup(func() {
		_ = h.PGProxy.RemoveToxic("pg_latency")
	})
}

// InjectPGResetPeer immediately resets the TCP connection to PostgreSQL.
// Simulates an abrupt network partition or PostgreSQL crash.
// timeout: bytes to allow through before resetting (0 = immediate reset).
func (h *ChaosHarness) InjectPGResetPeer(t *testing.T, timeout int) {
	t.Helper()

	_, err := h.PGProxy.AddToxic("pg_reset", "reset_peer", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeout,
	})
	require.NoError(t, err, "inject PG reset_peer toxic")

	t.Cleanup(func() {
		_ = h.PGProxy.RemoveToxic("pg_reset")
	})
}

// InjectPGTimeout creates a complete blackhole — data goes in, nothing comes back.
// Simulates a hung PostgreSQL or a firewall dropping packets.
// timeoutMs: time in ms before the connection is closed (0 = indefinite hang).
func (h *ChaosHarness) InjectPGTimeout(t *testing.T, timeoutMs int) {
	t.Helper()

	_, err := h.PGProxy.AddToxic("pg_timeout", "timeout", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeoutMs,
	})
	require.NoError(t, err, "inject PG timeout toxic")

	t.Cleanup(func() {
		_ = h.PGProxy.RemoveToxic("pg_timeout")
	})
}

// InjectPGBandwidth limits data throughput on the PostgreSQL connection.
// rateKB: maximum kilobytes per second.
func (h *ChaosHarness) InjectPGBandwidth(t *testing.T, rateKB int) {
	t.Helper()

	_, err := h.PGProxy.AddToxic("pg_bandwidth", "bandwidth", "downstream", 1.0, toxiproxy.Attributes{
		"rate": rateKB,
	})
	require.NoError(t, err, "inject PG bandwidth toxic")

	t.Cleanup(func() {
		_ = h.PGProxy.RemoveToxic("pg_bandwidth")
	})
}

// DisablePGProxy disables the PostgreSQL proxy entirely.
// All connections through it will be refused. Re-enable with EnablePGProxy.
func (h *ChaosHarness) DisablePGProxy(t *testing.T) {
	t.Helper()
	h.PGProxy.Enabled = false

	err := h.PGProxy.Save()
	require.NoError(t, err, "disable PG proxy")

	t.Cleanup(func() {
		h.PGProxy.Enabled = true
		_ = h.PGProxy.Save()
	})
}

// EnablePGProxy re-enables the PostgreSQL proxy.
func (h *ChaosHarness) EnablePGProxy(t *testing.T) {
	t.Helper()
	h.PGProxy.Enabled = true

	err := h.PGProxy.Save()
	require.NoError(t, err, "enable PG proxy")
}

// --------------------------------------------------------------------------
// Redis toxic injection
// --------------------------------------------------------------------------

// InjectRedisLatency adds downstream latency to the Redis proxy.
func (h *ChaosHarness) InjectRedisLatency(t *testing.T, latencyMs, jitterMs int) {
	t.Helper()

	_, err := h.RedisProxy.AddToxic("redis_latency", "latency", "downstream", 1.0, toxiproxy.Attributes{
		"latency": latencyMs,
		"jitter":  jitterMs,
	})
	require.NoError(t, err, "inject Redis latency toxic")

	t.Cleanup(func() {
		_ = h.RedisProxy.RemoveToxic("redis_latency")
	})
}

// InjectRedisTimeout creates a complete blackhole on the Redis connection.
// Simulates a hung Redis instance or network partition.
func (h *ChaosHarness) InjectRedisTimeout(t *testing.T, timeoutMs int) {
	t.Helper()

	_, err := h.RedisProxy.AddToxic("redis_timeout", "timeout", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeoutMs,
	})
	require.NoError(t, err, "inject Redis timeout toxic")

	t.Cleanup(func() {
		_ = h.RedisProxy.RemoveToxic("redis_timeout")
	})
}

// InjectRedisResetPeer resets TCP connections to Redis.
func (h *ChaosHarness) InjectRedisResetPeer(t *testing.T, timeout int) {
	t.Helper()

	_, err := h.RedisProxy.AddToxic("redis_reset", "reset_peer", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeout,
	})
	require.NoError(t, err, "inject Redis reset_peer toxic")

	t.Cleanup(func() {
		_ = h.RedisProxy.RemoveToxic("redis_reset")
	})
}

// DisableRedisProxy disables the Redis proxy entirely (connection refused).
func (h *ChaosHarness) DisableRedisProxy(t *testing.T) {
	t.Helper()
	h.RedisProxy.Enabled = false

	err := h.RedisProxy.Save()
	require.NoError(t, err, "disable Redis proxy")

	t.Cleanup(func() {
		h.RedisProxy.Enabled = true
		_ = h.RedisProxy.Save()
	})
}

// EnableRedisProxy re-enables the Redis proxy.
func (h *ChaosHarness) EnableRedisProxy(t *testing.T) {
	t.Helper()
	h.RedisProxy.Enabled = true

	err := h.RedisProxy.Save()
	require.NoError(t, err, "enable Redis proxy")
}

// --------------------------------------------------------------------------
// RabbitMQ toxic injection
// --------------------------------------------------------------------------

// InjectRabbitLatency adds downstream latency to the RabbitMQ proxy.
func (h *ChaosHarness) InjectRabbitLatency(t *testing.T, latencyMs, jitterMs int) {
	t.Helper()

	_, err := h.RabbitProxy.AddToxic("rabbit_latency", "latency", "downstream", 1.0, toxiproxy.Attributes{
		"latency": latencyMs,
		"jitter":  jitterMs,
	})
	require.NoError(t, err, "inject RabbitMQ latency toxic")

	t.Cleanup(func() {
		_ = h.RabbitProxy.RemoveToxic("rabbit_latency")
	})
}

// InjectRabbitTimeout creates a complete blackhole on the RabbitMQ connection.
func (h *ChaosHarness) InjectRabbitTimeout(t *testing.T, timeoutMs int) {
	t.Helper()

	_, err := h.RabbitProxy.AddToxic("rabbit_timeout", "timeout", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeoutMs,
	})
	require.NoError(t, err, "inject RabbitMQ timeout toxic")

	t.Cleanup(func() {
		_ = h.RabbitProxy.RemoveToxic("rabbit_timeout")
	})
}

// InjectRabbitResetPeer resets TCP connections to RabbitMQ.
func (h *ChaosHarness) InjectRabbitResetPeer(t *testing.T, timeout int) {
	t.Helper()

	_, err := h.RabbitProxy.AddToxic("rabbit_reset", "reset_peer", "downstream", 1.0, toxiproxy.Attributes{
		"timeout": timeout,
	})
	require.NoError(t, err, "inject RabbitMQ reset_peer toxic")

	t.Cleanup(func() {
		_ = h.RabbitProxy.RemoveToxic("rabbit_reset")
	})
}

// DisableRabbitProxy disables the RabbitMQ proxy entirely (connection refused).
func (h *ChaosHarness) DisableRabbitProxy(t *testing.T) {
	t.Helper()
	h.RabbitProxy.Enabled = false

	err := h.RabbitProxy.Save()
	require.NoError(t, err, "disable RabbitMQ proxy")

	t.Cleanup(func() {
		h.RabbitProxy.Enabled = true
		_ = h.RabbitProxy.Save()
	})
}

// EnableRabbitProxy re-enables the RabbitMQ proxy.
func (h *ChaosHarness) EnableRabbitProxy(t *testing.T) {
	t.Helper()
	h.RabbitProxy.Enabled = true

	err := h.RabbitProxy.Save()
	require.NoError(t, err, "enable RabbitMQ proxy")
}

// --------------------------------------------------------------------------
// Multi-service chaos
// --------------------------------------------------------------------------

// RemoveAllToxics removes all active toxics from all proxies.
// Called automatically via t.Cleanup, but can be invoked manually
// for mid-test recovery scenarios.
func (h *ChaosHarness) RemoveAllToxics(t *testing.T) {
	t.Helper()

	for _, proxy := range []*toxiproxy.Proxy{h.PGProxy, h.RedisProxy, h.RabbitProxy} {
		if proxy == nil {
			continue
		}

		toxics, err := proxy.Toxics()
		if err != nil {
			t.Logf("warning: failed to list toxics for %s: %v", proxy.Name, err)
			continue
		}

		for _, toxic := range toxics {
			if err := proxy.RemoveToxic(toxic.Name); err != nil {
				t.Logf("warning: failed to remove toxic %s from %s: %v",
					toxic.Name, proxy.Name, err)
			}
		}
	}
}

// EnableAllProxies re-enables all proxies (recovering from DisableXxxProxy calls).
func (h *ChaosHarness) EnableAllProxies(t *testing.T) {
	t.Helper()

	for _, proxy := range []*toxiproxy.Proxy{h.PGProxy, h.RedisProxy, h.RabbitProxy} {
		if proxy == nil {
			continue
		}

		proxy.Enabled = true
		if err := proxy.Save(); err != nil {
			t.Logf("warning: failed to re-enable proxy %s: %v", proxy.Name, err)
		}
	}
}

// IsolateService disables one proxy while keeping others active.
// Simulates a targeted infrastructure failure.
func (h *ChaosHarness) IsolateService(t *testing.T, service string) error {
	t.Helper()

	var proxy *toxiproxy.Proxy

	switch service {
	case "postgres":
		proxy = h.PGProxy
	case "redis":
		proxy = h.RedisProxy
	case "rabbitmq":
		proxy = h.RabbitProxy
	default:
		return fmt.Errorf("unknown service: %s (expected: postgres, redis, rabbitmq)", service)
	}

	if proxy == nil {
		return fmt.Errorf("%s proxy is nil", service)
	}

	proxy.Enabled = false

	if err := proxy.Save(); err != nil {
		return fmt.Errorf("isolate %s: %w", service, err)
	}

	t.Cleanup(func() {
		proxy.Enabled = true
		_ = proxy.Save()
	})

	return nil
}
