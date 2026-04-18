// Package storage provides object storage adapters for export files.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// S3Config contains configuration for S3-compatible storage.
// Works with AWS S3, MinIO, SeaweedFS, and other S3-compatible services.
type S3Config struct {
	Endpoint        string // For SeaweedFS: http://localhost:8333, for MinIO: http://localhost:9000
	Region          string // Default: us-east-1
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	UsePathStyle    bool // Required for SeaweedFS/MinIO
	DisableSSL      bool
	AllowInsecure   bool // Allows non-loopback HTTP endpoints for local development.
}

// DefaultSeaweedConfig returns a configuration suitable for local SeaweedFS development.
func DefaultSeaweedConfig(bucket string) S3Config {
	return S3Config{
		Endpoint:     "http://localhost:8333",
		Region:       "us-east-1",
		Bucket:       bucket,
		UsePathStyle: true,
		DisableSSL:   true,
	}
}

// S3Client provides S3-compatible object storage operations.
type S3Client struct {
	s3     *s3.Client
	bucket string
}

var (
	// ErrBucketRequired indicates bucket name is missing.
	ErrBucketRequired = errors.New("bucket name is required")
	// ErrInsecureEndpoint indicates a non-local object storage endpoint is using cleartext HTTP.
	ErrInsecureEndpoint = errors.New("object storage endpoint must use https unless explicitly local")
	// ErrKeyRequired indicates object key is missing.
	ErrKeyRequired = errors.New("object key is required")
	// ErrObjectNotFound indicates the object does not exist.
	ErrObjectNotFound = errors.New("object not found")
	// ErrObjectStorageEndpointMissingSchemeOrHost indicates endpoint is not a valid URL target.
	ErrObjectStorageEndpointMissingSchemeOrHost = errors.New("parse object storage endpoint: missing scheme or host")
	// ErrObjectStorageEndpointUnsupportedScheme indicates endpoint uses a non HTTP(S) scheme.
	ErrObjectStorageEndpointUnsupportedScheme = errors.New("parse object storage endpoint: unsupported scheme")
)

const defaultServerSideEncryption = "AES256"

// NewS3Client creates a new S3 client with the given configuration.
func NewS3Client(ctx context.Context, cfg S3Config) (*S3Client, error) {
	if cfg.Bucket == "" {
		return nil, ErrBucketRequired
	}

	if err := validateEndpointSecurity(cfg.Endpoint, cfg.AllowInsecure); err != nil {
		return nil, err
	}

	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){}

	if cfg.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.UsePathStyle {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, clientOpts...)

	return &S3Client{
		s3:     s3Client,
		bucket: cfg.Bucket,
	}, nil
}

func (client *S3Client) ensureReady() error {
	if client == nil || client.s3 == nil || client.bucket == "" {
		return sharedPorts.ErrObjectStorageUnavailable
	}

	return nil
}

func validateEndpointSecurity(endpoint string, allowInsecure bool) error {
	trimmed := strings.TrimSpace(endpoint)
	if trimmed == "" {
		return nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("parse object storage endpoint: %w", err)
	}

	if parsed.Scheme == "" || parsed.Host == "" {
		return ErrObjectStorageEndpointMissingSchemeOrHost
	}

	if strings.EqualFold(parsed.Scheme, "https") {
		return nil
	}

	if !strings.EqualFold(parsed.Scheme, "http") {
		return fmt.Errorf("%w: %s", ErrObjectStorageEndpointUnsupportedScheme, parsed.Scheme)
	}

	if allowInsecure {
		return nil
	}

	host := parsed.Hostname()
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return nil
	}

	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}

	return ErrInsecureEndpoint
}

// Upload stores content from a reader at the given key.
func (client *S3Client) Upload(
	ctx context.Context,
	key string,
	reader io.Reader,
	contentType string,
) (string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.upload")

	defer span.End()

	if key == "" {
		return "", ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return "", err
	}

	input := &s3.PutObjectInput{
		Bucket:               aws.String(client.bucket),
		Key:                  aws.String(key),
		Body:                 reader,
		ContentType:          aws.String(contentType),
		ServerSideEncryption: types.ServerSideEncryption(defaultServerSideEncryption),
	}

	if _, err := client.s3.PutObject(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to upload object", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to upload object %s: %v", key, err))

		return "", fmt.Errorf("uploading object: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("uploaded object %s to bucket %s", key, client.bucket))

	return key, nil
}

// UploadWithOptions stores content with configurable storage options.
func (client *S3Client) UploadWithOptions(
	ctx context.Context,
	key string,
	reader io.Reader,
	contentType string,
	opts ...sharedPorts.UploadOption,
) (string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.upload_with_options")

	defer span.End()

	if key == "" {
		return "", ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return "", err
	}

	options := &sharedPorts.UploadOptions{}
	for _, opt := range opts {
		opt(options)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(client.bucket),
		Key:         aws.String(key),
		Body:        reader,
		ContentType: aws.String(contentType),
	}

	if options.StorageClass != "" {
		input.StorageClass = types.StorageClass(options.StorageClass)
	}

	if options.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(options.ServerSideEncryption)
	} else {
		input.ServerSideEncryption = types.ServerSideEncryption(defaultServerSideEncryption)
	}

	if _, err := client.s3.PutObject(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to upload object with options", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to upload object %s: %v", key, err))

		return "", fmt.Errorf("uploading object with options: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("uploaded object %s to bucket %s (storage_class=%s)", key, client.bucket, options.StorageClass))

	return key, nil
}

// Download retrieves content from the given key.
func (client *S3Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.download")

	defer span.End()

	if key == "" {
		return nil, ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return nil, err
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(key),
	}

	result, err := client.s3.GetObject(ctx, input)
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotFound
		}

		libOpentelemetry.HandleSpanError(span, "failed to download object", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to download object %s: %v", key, err))

		return nil, fmt.Errorf("downloading object: %w", err)
	}

	return result.Body, nil
}

// Delete removes an object by key.
func (client *S3Client) Delete(ctx context.Context, key string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.delete")

	defer span.End()

	if key == "" {
		return ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return err
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(key),
	}

	if _, err := client.s3.DeleteObject(ctx, input); err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to delete object", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to delete object %s: %v", key, err))

		return fmt.Errorf("deleting object: %w", err)
	}

	logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("deleted object %s from bucket %s", key, client.bucket))

	return nil
}

// GeneratePresignedURL creates a time-limited download URL.
func (client *S3Client) GeneratePresignedURL(
	ctx context.Context,
	key string,
	expiry time.Duration,
) (string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.generate_presigned_url")

	defer span.End()

	if key == "" {
		return "", ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return "", err
	}

	presigner := s3.NewPresignClient(client.s3)

	input := &s3.GetObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(key),
	}

	result, err := presigner.PresignGetObject(ctx, input, s3.WithPresignExpires(expiry))
	if err != nil {
		libOpentelemetry.HandleSpanError(span, "failed to generate presigned url", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to generate presigned url for %s: %v", key, err))

		return "", fmt.Errorf("generating presigned url: %w", err)
	}

	return result.URL, nil
}

// Exists checks if an object exists at the given key.
func (client *S3Client) Exists(ctx context.Context, key string) (bool, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.exists")

	defer span.End()

	if key == "" {
		return false, ErrKeyRequired
	}

	if err := client.ensureReady(); err != nil {
		return false, err
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(key),
	}

	if _, err := client.s3.HeadObject(ctx, input); err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return false, nil
		}

		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}

		libOpentelemetry.HandleSpanError(span, "failed to check object existence", err)

		logger.Log(ctx, libLog.LevelError, fmt.Sprintf("failed to check existence of %s: %v", key, err))

		return false, fmt.Errorf("checking object existence: %w", err)
	}

	return true, nil
}

// Compile-time interface check.
var _ sharedPorts.ObjectStorageClient = (*S3Client)(nil)
