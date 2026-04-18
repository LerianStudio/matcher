//go:build unit

package ports

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/storageopt"
)

func TestObjectStorageClientInterfaceExists(t *testing.T) {
	t.Parallel()

	var _ ObjectStorageClient = (*mockObjectStorageClient)(nil)
}

type mockObjectStorageClient struct{}

func (m *mockObjectStorageClient) Upload(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) UploadIfAbsent(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) UploadWithOptions(
	_ context.Context,
	_ string,
	_ io.Reader,
	_ string,
	_ ...storageopt.UploadOption,
) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) Download(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockObjectStorageClient) Delete(_ context.Context, _ string) error {
	return nil
}

func (m *mockObjectStorageClient) GeneratePresignedURL(_ context.Context, _ string, _ time.Duration) (string, error) {
	return "", nil
}

func (m *mockObjectStorageClient) Exists(_ context.Context, _ string) (bool, error) {
	return false, nil
}
