//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rabbitMQStartupTimeout = 180 * time.Second
	rabbitMQStartAttempts  = 3
	rabbitMQRetryCount     = 12
	rabbitMQRetryDelay     = 2 * time.Second
)

var (
	rabbitMQContainerFactory = testcontainers.GenericContainer
	rabbitMQRetryWait        = waitWithContext
)

func isNilRabbitMQContainer(container any) bool {
	if container == nil {
		return true
	}

	rv := reflect.ValueOf(container)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func startRabbitMQContainer(ctx context.Context) (testcontainers.Container, error) {
	var lastErr error

	for attempt := 1; attempt <= rabbitMQStartAttempts; attempt++ {
		container, err := rabbitMQContainerFactory(
			ctx,
			testcontainers.GenericContainerRequest{
				ContainerRequest: rabbitMQContainerRequest(),
				Started:          true,
			},
		)
		if err == nil {
			if isNilRabbitMQContainer(container) {
				return nil, errors.New("rabbitmq container factory returned nil container")
			}

			return container, nil
		}

		if !isNilRabbitMQContainer(container) {
			if terminateErr := container.Terminate(ctx); terminateErr != nil {
				return nil, fmt.Errorf("cleanup failed after rabbitmq startup error: %w", errors.Join(err, terminateErr))
			}
		}

		lastErr = err

		if attempt == rabbitMQStartAttempts {
			break
		}

		if waitErr := rabbitMQRetryWait(ctx, time.Duration(attempt)*rabbitMQRetryDelay); waitErr != nil {
			return nil, fmt.Errorf("wait before retrying rabbitmq startup: %w", waitErr)
		}
	}

	return nil, fmt.Errorf("start rabbitmq after %d attempts: %w", rabbitMQStartAttempts, lastErr)
}

func rabbitMQContainerRequest() testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.1.3-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor: wait.ForLog("Server startup complete").
			WithStartupTimeout(rabbitMQStartupTimeout),
	}
}

type rabbitMQStartupContainer interface {
	Host(context.Context) (string, error)
	MappedPort(context.Context, string) (network.Port, error)
	Inspect(context.Context) (*container.InspectResponse, error)
}

func containerHostWithRetry(ctx context.Context, container rabbitMQStartupContainer) (string, error) {
	if isNilRabbitMQContainer(container) {
		return "", errors.New("container is nil")
	}

	var lastErr error

	for attempt := 1; attempt <= rabbitMQRetryCount; attempt++ {
		host, err := container.Host(ctx)
		if err == nil && host != "" {
			return host, nil
		}

		if err == nil {
			err = errors.New("empty container host")
		}

		lastErr = err

		if attempt == rabbitMQRetryCount {
			break
		}

		if waitErr := rabbitMQRetryWait(ctx, rabbitMQRetryDelay); waitErr != nil {
			return "", fmt.Errorf("wait before retrying rabbitmq host lookup: %w", waitErr)
		}
	}

	return "", fmt.Errorf("get rabbitmq host: %w", lastErr)
}

func mappedPortWithRetry(
	ctx context.Context,
	container rabbitMQStartupContainer,
	containerPort string,
) (string, error) {
	if isNilRabbitMQContainer(container) {
		return "", errors.New("container is nil")
	}

	var lastErr error

	for attempt := 1; attempt <= rabbitMQRetryCount; attempt++ {
		resolvedPort, err := resolveMappedPort(ctx, container, containerPort)
		if err == nil && resolvedPort != "" {
			return resolvedPort, nil
		}

		if err == nil {
			err = fmt.Errorf("empty mapped port for %s", containerPort)
		}

		lastErr = err

		if attempt == rabbitMQRetryCount {
			break
		}

		if waitErr := rabbitMQRetryWait(ctx, rabbitMQRetryDelay); waitErr != nil {
			return "", fmt.Errorf("wait before retrying mapped port lookup: %w", waitErr)
		}
	}

	return "", fmt.Errorf("get rabbitmq mapped port %s: %w", containerPort, lastErr)
}

func resolveMappedPort(
	ctx context.Context,
	c rabbitMQStartupContainer,
	containerPort string,
) (string, error) {
	mappedPort, err := c.MappedPort(ctx, containerPort)
	if err == nil {
		if mappedPort.Port() == "" {
			return "", fmt.Errorf("empty mapped port for %s", containerPort)
		}

		return mappedPort.Port(), nil
	}

	rawPort := containerPort
	if idx := strings.Index(containerPort, "/"); idx != -1 {
		rawPort = containerPort[:idx]
	}

	mappedPortNoProtocol, noProtocolErr := c.MappedPort(ctx, rawPort)
	if noProtocolErr == nil {
		if mappedPortNoProtocol.Port() == "" {
			return "", fmt.Errorf("empty mapped port for %s", containerPort)
		}

		return mappedPortNoProtocol.Port(), nil
	}

	inspect, inspectErr := c.Inspect(ctx)
	if inspectErr == nil {
		if inspect == nil {
			return "", fmt.Errorf("inspect returned nil container details for %s", containerPort)
		}

		if inspect.NetworkSettings != nil {
			for exposedPort, bindings := range inspect.NetworkSettings.Ports {
				if len(bindings) == 0 {
					continue
				}

				if exposedPort.Port() == rawPort && bindings[0].HostPort != "" {
					return bindings[0].HostPort, nil
				}
			}
		}

		if inspect.HostConfig != nil && string(inspect.HostConfig.NetworkMode) == "host" {
			return rawPort, nil
		}
	}

	return "", err
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
