// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fasthttp/fasthttpadaptor"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	streaming "github.com/LerianStudio/lib-streaming/v2"

	"github.com/LerianStudio/matcher/internal/shared/constants"
	streamingBootstrap "github.com/LerianStudio/matcher/internal/streaming/bootstrap"
)

// StreamingManifestRoutePath is the authenticated management-plane route for the streaming manifest.
const StreamingManifestRoutePath = "/system/matcher/streaming/manifest"

var (
	errMountStreamingManifestAppRequired     = errors.New("mount streaming manifest api: app is required")
	errMountStreamingManifestCatalogRequired = errors.New("mount streaming manifest api: catalog is required")
)

// MountStreamingManifestAPI mounts lib-streaming's manifest handler under the
// existing /system admin plane. The caller must invoke this only after
// MountSystemplaneAPI has installed the /system middleware chain; this function
// deliberately does not duplicate auth, tenant extraction, idempotency, or
// rate-limit wiring.
func MountStreamingManifestAPI(
	app *fiber.App,
	bundle streamingBootstrap.ProducerBundle,
	logger libLog.Logger,
) error {
	if app == nil {
		return errMountStreamingManifestAppRequired
	}

	if bundle.Catalog.Len() == 0 {
		return errMountStreamingManifestCatalogRequired
	}

	descriptor, err := streamingManifestPublisherDescriptor(bundle)
	if err != nil {
		return fmt.Errorf("build streaming manifest descriptor: %w", err)
	}

	handler, err := streaming.NewStreamingHandler(descriptor, bundle.Catalog)
	if err != nil {
		return fmt.Errorf("build streaming manifest handler: %w", err)
	}

	fasthttpHandler := fasthttpadaptor.NewFastHTTPHandler(handler)

	app.Get(StreamingManifestRoutePath, func(fiberCtx *fiber.Ctx) error {
		fasthttpHandler(fiberCtx.Context())

		return nil
	})

	if logger != nil {
		logger.Log(context.Background(), libLog.LevelInfo,
			"streaming manifest API mounted on "+StreamingManifestRoutePath)
	}

	return nil
}

func streamingManifestPublisherDescriptor(bundle streamingBootstrap.ProducerBundle) (streaming.PublisherDescriptor, error) {
	base := streaming.PublisherDescriptor{
		ServiceName:     constants.ApplicationName,
		SourceBase:      streamingManifestSourceBase(bundle),
		RoutePath:       StreamingManifestRoutePath,
		OutboxSupported: true,
		AppVersion:      resolveReadinessVersion(),
		LibVersion:      streamingManifestLibVersion(),
	}

	producer, ok := bundle.Emitter.(*streaming.Producer)
	if !ok || producer == nil {
		descriptor, err := streaming.NewPublisherDescriptor(base)
		if err != nil {
			return streaming.PublisherDescriptor{}, fmt.Errorf("build streaming publisher descriptor: %w", err)
		}

		return descriptor, nil
	}

	descriptor, err := producer.Descriptor(base)
	if err != nil {
		return streaming.PublisherDescriptor{}, fmt.Errorf("build streaming producer descriptor: %w", err)
	}

	return descriptor, nil
}

func streamingManifestSourceBase(bundle streamingBootstrap.ProducerBundle) string {
	if sourceBase := strings.TrimSpace(bundle.Config.CloudEventsSource); sourceBase != "" {
		return sourceBase
	}

	return constants.ApplicationName
}

func streamingManifestLibVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	const libStreamingModulePath = "github.com/LerianStudio/lib-streaming/v2"
	for _, dep := range info.Deps {
		if dep.Path == libStreamingModulePath {
			return dep.Version
		}
	}

	return "unknown"
}
