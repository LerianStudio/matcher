//go:build unit

package ports

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

type objectStorageClientStub struct{}

func (objectStorageClientStub) Upload(_ context.Context, key string, _ io.Reader, _ string) (string, error) {
	return key, nil
}

func (objectStorageClientStub) UploadIfAbsent(_ context.Context, key string, _ io.Reader, _ string) (string, error) {
	return key, nil
}

func (objectStorageClientStub) UploadWithOptions(
	_ context.Context,
	key string,
	_ io.Reader,
	_ string,
	_ ...UploadOption,
) (string, error) {
	return key, nil
}

func (objectStorageClientStub) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (objectStorageClientStub) Delete(_ context.Context, _ string) error {
	return nil
}

func (objectStorageClientStub) GeneratePresignedURL(_ context.Context, key string, _ time.Duration) (string, error) {
	return key, nil
}

func (objectStorageClientStub) Exists(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func TestObjectStorageAliases(t *testing.T) {
	t.Parallel()

	var client ObjectStorageClient = objectStorageClientStub{}
	if client == nil {
		t.Fatal("expected object storage alias to be assignable")
	}

	options := UploadOptions{}
	WithStorageClass("STANDARD")(&options)
	WithServerSideEncryption("AES256")(&options)

	if options.StorageClass != "STANDARD" {
		t.Fatalf("expected storage class to be set, got %q", options.StorageClass)
	}

	if options.ServerSideEncryption != "AES256" {
		t.Fatalf("expected server-side encryption to be set, got %q", options.ServerSideEncryption)
	}
}
