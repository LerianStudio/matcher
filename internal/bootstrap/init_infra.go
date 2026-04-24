// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// interface-only:skip-check-tests

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
)

func createPostgresConnection(cfg *Config, logger libLog.Logger) (*libPostgres.Client, error) {
	conn, err := libPostgres.New(libPostgres.Config{
		PrimaryDSN:         cfg.PrimaryDSN(),
		ReplicaDSN:         cfg.ReplicaDSN(),
		Logger:             logger,
		MaxOpenConnections: cfg.Postgres.MaxOpenConnections,
		MaxIdleConnections: cfg.Postgres.MaxIdleConnections,
	})
	if err != nil {
		return nil, fmt.Errorf("create postgres client: %w", err)
	}

	return conn, nil
}

func buildRedisConfig(cfg *Config, logger libLog.Logger) libRedis.Config {
	redisCfg := libRedis.Config{
		Auth: libRedis.Auth{
			StaticPassword: &libRedis.StaticPasswordAuth{
				Password: cfg.Redis.Password,
			},
		},
		Options: libRedis.ConnectionOptions{
			DB:           cfg.Redis.DB,
			Protocol:     cfg.Redis.Protocol,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConn,
			ReadTimeout:  cfg.RedisReadTimeout(),
			WriteTimeout: cfg.RedisWriteTimeout(),
			DialTimeout:  cfg.RedisDialTimeout(),
		},
		Logger: logger,
	}

	// Build TLS config if enabled
	if cfg.Redis.TLS {
		redisCfg.TLS = &libRedis.TLSConfig{
			CACertBase64: cfg.Redis.CACert,
		}
	}

	// Determine topology from config
	rawAddresses := strings.Split(cfg.Redis.Host, ",")
	addresses := make([]string, 0, len(rawAddresses))

	for _, addr := range rawAddresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			addresses = append(addresses, trimmed)
		}
	}

	switch {
	case cfg.Redis.MasterName != "":
		redisCfg.Topology = libRedis.Topology{
			Sentinel: &libRedis.SentinelTopology{
				Addresses:  addresses,
				MasterName: cfg.Redis.MasterName,
			},
		}
	case len(addresses) > 1:
		redisCfg.Topology = libRedis.Topology{
			Cluster: &libRedis.ClusterTopology{
				Addresses: addresses,
			},
		}
	default:
		addr := strings.TrimSpace(cfg.Redis.Host)
		if addr == "" && !IsProductionEnvironment(cfg.App.EnvName) {
			addr = "localhost:6379"
		}

		redisCfg.Topology = libRedis.Topology{
			Standalone: &libRedis.StandaloneTopology{
				Address: addr,
			},
		}
	}

	return redisCfg
}

func createRedisConnection(ctx context.Context, cfg *Config, logger libLog.Logger) (*libRedis.Client, error) {
	redisCfg := buildRedisConfig(cfg, logger)

	conn, err := libRedis.New(ctx, redisCfg)
	if err != nil {
		return nil, fmt.Errorf("create redis client: %w", err)
	}

	return conn, nil
}

func createRabbitMQConnection(cfg *Config, logger libLog.Logger) *libRabbitmq.RabbitMQConnection {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if cfg == nil {
		logger.Log(
			context.Background(),
			libLog.LevelError,
			"RabbitMQ connection configuration is nil; using empty defaults and disabling insecure health checks",
		)

		cfg = &Config{}
	}

	allowInsecureHealthCheck, denialReason := evaluateInsecureRabbitMQHealthCheckPolicy(cfg)
	if denialReason != "" {
		logger.Log(context.Background(), libLog.LevelWarn, denialReason)
	}

	if !allowInsecureHealthCheck && isInsecureHTTPHealthCheckURL(cfg.RabbitMQ.HealthURL) {
		logger.Log(
			context.Background(),
			libLog.LevelWarn,
			"RabbitMQ health URL uses HTTP while insecure checks are disabled; set RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK=true only for local/internal non-production environments",
		)
	}

	return &libRabbitmq.RabbitMQConnection{
		ConnectionStringSource:   cfg.RabbitMQDSN(),
		HealthCheckURL:           cfg.RabbitMQ.HealthURL,
		Host:                     cfg.RabbitMQ.Host,
		Port:                     cfg.RabbitMQ.Port,
		User:                     cfg.RabbitMQ.User,
		Pass:                     cfg.RabbitMQ.Password,
		Logger:                   logger,
		AllowInsecureHealthCheck: allowInsecureHealthCheck,
	}
}

func evaluateInsecureRabbitMQHealthCheckPolicy(cfg *Config) (bool, string) {
	if cfg == nil {
		return false, "RabbitMQ health check insecure HTTP is disabled because configuration is nil"
	}

	if !cfg.RabbitMQ.AllowInsecureHealthCheck {
		return false, ""
	}

	if IsProductionEnvironment(cfg.App.EnvName) {
		return false, "RabbitMQ health check insecure HTTP is disabled in production"
	}

	if !isInsecureHTTPHealthCheckURL(cfg.RabbitMQ.HealthURL) {
		return false, "RabbitMQ insecure health check requires an HTTP health URL"
	}

	if !isAllowedInsecureHealthCheckHost(cfg.RabbitMQ.HealthURL, cfg.RabbitMQ.Host) {
		return false, "RabbitMQ insecure health check is restricted to local/internal hosts"
	}

	return true, ""
}

func isInsecureHTTPHealthCheckURL(healthURL string) bool {
	parsed, err := url.Parse(healthURL)
	if err != nil {
		return false
	}

	return strings.EqualFold(parsed.Scheme, "http")
}

func isAllowedInsecureHealthCheckHost(healthURL, configuredRabbitHost string) bool {
	parsed, err := url.Parse(healthURL)
	if err != nil {
		return false
	}

	hostname := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if hostname == "" {
		return false
	}

	if hostname == "localhost" {
		return true
	}

	ip := net.ParseIP(hostname)
	if ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}

	configuredHost := strings.ToLower(strings.TrimSpace(configuredRabbitHost))
	if configuredHost != "" && !strings.Contains(hostname, ".") && hostname == configuredHost {
		return true
	}

	return strings.HasSuffix(hostname, ".local") ||
		strings.HasSuffix(hostname, ".internal") ||
		strings.HasSuffix(hostname, ".cluster.local")
}

// errRabbitMQConnectionNil is returned when attempting to open a channel on a nil connection.
var errRabbitMQConnectionNil = errors.New("rabbitmq connection or underlying AMQP connection is nil")

// openDedicatedChannel opens a new AMQP channel from the underlying *amqp.Connection.
// Each ConfirmablePublisher MUST own a dedicated channel because AMQP publisher confirms
// are channel-scoped. Sharing a channel between publishers corrupts delivery tag tracking.
func openDedicatedChannel(conn *libRabbitmq.RabbitMQConnection) (*amqp.Channel, error) {
	if conn == nil || conn.Connection == nil {
		return nil, errRabbitMQConnectionNil
	}

	ch, err := conn.Connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("open dedicated AMQP channel: %w", err)
	}

	return ch, nil
}
