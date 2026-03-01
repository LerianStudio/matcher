// Package storage provides object storage adapters for export files.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	tms3 "github.com/LerianStudio/lib-commons/v3/commons/tenant-manager/s3"
	libCommons "github.com/LerianStudio/lib-uncommons/v2/uncommons"
	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libOpentelemetry "github.com/LerianStudio/lib-uncommons/v2/uncommons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/reporting/ports"
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

// getTenantPrefixedKey returns a tenant-prefixed object key using canonical lib-commons v3
// s3.GetObjectStorageKeyForTenant.
// In multi-tenant mode (tenantID in context): "{tenantID}/{key}"
// In single-tenant mode (no tenant in context): "{key}" unchanged
// Leading slashes are always stripped from the key for clean path construction.
func getTenantPrefixedKey(ctx context.Context, key string) string {
	return tms3.GetObjectStorageKeyForTenant(ctx, key)
}

var (
	// ErrBucketRequired indicates bucket name is missing.
	ErrBucketRequired = errors.New("bucket name is required")
	// ErrKeyRequired indicates object key is missing.
	ErrKeyRequired = errors.New("object key is required")
	// ErrObjectNotFound indicates the object does not exist.
	ErrObjectNotFound = errors.New("object not found")
)

// NewS3Client creates a new S3 client with the given configuration.
func NewS3Client(ctx context.Context, cfg S3Config) (*S3Client, error) {
	if cfg.Bucket == "" {
		return nil, ErrBucketRequired
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

// Upload stores content from a reader at the given key.
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
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

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(client.bucket),
		Key:         aws.String(prefixedKey),
		Body:        reader,
		ContentType: aws.String(contentType),
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
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
func (client *S3Client) UploadWithOptions(
	ctx context.Context,
	key string,
	reader io.Reader,
	contentType string,
	opts ...ports.UploadOption,
) (string, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.upload_with_options")

	defer span.End()

	if key == "" {
		return "", ErrKeyRequired
	}

	options := &ports.UploadOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(client.bucket),
		Key:         aws.String(prefixedKey),
		Body:        reader,
		ContentType: aws.String(contentType),
	}

	if options.StorageClass != "" {
		input.StorageClass = types.StorageClass(options.StorageClass)
	}

	if options.ServerSideEncryption != "" {
		input.ServerSideEncryption = types.ServerSideEncryption(options.ServerSideEncryption)
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
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
func (client *S3Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.download")

	defer span.End()

	if key == "" {
		return nil, ErrKeyRequired
	}

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	input := &s3.GetObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(prefixedKey),
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
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
func (client *S3Client) Delete(ctx context.Context, key string) error {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.delete")

	defer span.End()

	if key == "" {
		return ErrKeyRequired
	}

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(prefixedKey),
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
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
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

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	presigner := s3.NewPresignClient(client.s3)

	input := &s3.GetObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(prefixedKey),
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
// In multi-tenant mode, the key is automatically prefixed with the tenant ID.
func (client *S3Client) Exists(ctx context.Context, key string) (bool, error) {
	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)
	ctx, span := tracer.Start(ctx, "s3.exists")

	defer span.End()

	if key == "" {
		return false, ErrKeyRequired
	}

	// Apply tenant prefix for multi-tenant isolation
	prefixedKey := getTenantPrefixedKey(ctx, key)

	input := &s3.HeadObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(prefixedKey),
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
var _ ports.ObjectStorageClient = (*S3Client)(nil)
