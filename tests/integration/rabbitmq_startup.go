//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rabbitMQStartupTimeout = 180 * time.Second
	rabbitMQStartAttempts  = 3
	rabbitMQRetryCount     = 12
	rabbitMQRetryDelay     = 2 * time.Second
)

func startRabbitMQContainer(ctx context.Context) (testcontainers.Container, error) {
	var lastErr error

	for attempt := 1; attempt <= rabbitMQStartAttempts; attempt++ {
		container, err := testcontainers.GenericContainer(
			ctx,
			testcontainers.GenericContainerRequest{
				ContainerRequest: rabbitMQContainerRequest(),
				Started:          true,
			},
		)
		if err == nil {
			return container, nil
		}

		if container != nil {
			_ = container.Terminate(ctx)
		}

		lastErr = err

		if attempt == rabbitMQStartAttempts {
			break
		}

		if waitErr := waitWithContext(ctx, time.Duration(attempt)*rabbitMQRetryDelay); waitErr != nil {
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

func containerHostWithRetry(ctx context.Context, container testcontainers.Container) (string, error) {
	if container == nil {
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

		if waitErr := waitWithContext(ctx, rabbitMQRetryDelay); waitErr != nil {
			return "", fmt.Errorf("wait before retrying rabbitmq host lookup: %w", waitErr)
		}
	}

	return "", fmt.Errorf("get rabbitmq host: %w", lastErr)
}

func mappedPortWithRetry(
	ctx context.Context,
	container testcontainers.Container,
	containerPort string,
) (string, error) {
	if container == nil {
		return "", errors.New("container is nil")
	}

	var lastErr error

	for attempt := 1; attempt <= rabbitMQRetryCount; attempt++ {
		mappedPort, err := container.MappedPort(ctx, nat.Port(containerPort))
		if err == nil && mappedPort.Port() != "" {
			return mappedPort.Port(), nil
		}

		if err == nil {
			err = fmt.Errorf("empty mapped port for %s", containerPort)
		}

		lastErr = err

		if attempt == rabbitMQRetryCount {
			break
		}

		if waitErr := waitWithContext(ctx, rabbitMQRetryDelay); waitErr != nil {
			return "", fmt.Errorf("wait before retrying mapped port lookup: %w", waitErr)
		}
	}

	return "", fmt.Errorf("get rabbitmq mapped port %s: %w", containerPort, lastErr)
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
