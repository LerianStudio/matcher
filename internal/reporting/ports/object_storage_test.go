//go:build unit

package ports

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	portsmocks "github.com/LerianStudio/matcher/internal/reporting/ports/mocks"
)

var (
	errTestUploadFailed            = errors.New("upload failed")
	errTestKeyCannotBeEmpty        = errors.New("key cannot be empty")
	errTestObjectNotFound          = errors.New("object not found")
	errTestPermissionDenied        = errors.New("permission denied")
	errTestExpiryCannotExceed7Days = errors.New("expiry cannot exceed 7 days")
	errTestAccessDenied            = errors.New("access denied")
)

func TestObjectStorageClient_InterfaceNotNil(t *testing.T) {
	t.Parallel()

	clientType := reflect.TypeOf((*ObjectStorageClient)(nil)).Elem()
	assert.NotNil(t, clientType)
}

func TestObjectStorageClient_IsInterface(t *testing.T) {
	t.Parallel()

	clientType := reflect.TypeOf((*ObjectStorageClient)(nil)).Elem()
	assert.Equal(t, reflect.Interface, clientType.Kind())
}

func TestObjectStorageClient_MethodCount(t *testing.T) {
	t.Parallel()

	clientType := reflect.TypeOf((*ObjectStorageClient)(nil)).Elem()

	const expectedMethodCount = 6

	actualCount := clientType.NumMethod()

	assert.Equal(t, expectedMethodCount, actualCount,
		"ObjectStorageClient should have exactly %d methods - found %d",
		expectedMethodCount, actualCount)
}

func TestObjectStorageClient_RequiredMethods(t *testing.T) {
	t.Parallel()

	clientType := reflect.TypeOf((*ObjectStorageClient)(nil)).Elem()

	requiredMethods := []string{
		"Upload",
		"UploadWithOptions",
		"Download",
		"Delete",
		"GeneratePresignedURL",
		"Exists",
	}

	for _, methodName := range requiredMethods {
		t.Run(methodName+"_exists", func(t *testing.T) {
			t.Parallel()

			_, exists := clientType.MethodByName(methodName)
			assert.True(t, exists, "method %s must exist in ObjectStorageClient", methodName)
		})
	}
}

func TestObjectStorageClient_InterfaceContract(t *testing.T) {
	t.Parallel()

	clientType := reflect.TypeOf((*ObjectStorageClient)(nil)).Elem()

	t.Run("Upload method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("Upload")
		assert.True(t, exists, "Upload method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 4, numIn, "Upload should accept context, key, reader, contentType")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Upload should return key and error")
	})

	t.Run("Download method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("Download")
		assert.True(t, exists, "Download method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Download should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Download should return ReadCloser and error")
	})

	t.Run("Delete method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("Delete")
		assert.True(t, exists, "Delete method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Delete should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 1, numOut, "Delete should return only error")
	})

	t.Run("GeneratePresignedURL method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("GeneratePresignedURL")
		assert.True(t, exists, "GeneratePresignedURL method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 3, numIn, "GeneratePresignedURL should accept context, key, expiry")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "GeneratePresignedURL should return URL and error")
	})

	t.Run("Exists method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("Exists")
		assert.True(t, exists, "Exists method must exist")

		numIn := method.Type.NumIn()
		assert.Equal(t, 2, numIn, "Exists should accept context and key")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "Exists should return bool and error")
	})

	t.Run("UploadWithOptions method signature", func(t *testing.T) {
		t.Parallel()

		method, exists := clientType.MethodByName("UploadWithOptions")
		assert.True(t, exists, "UploadWithOptions method must exist")

		// Variadic methods: NumIn includes ctx, key, reader, contentType, ...UploadOption = 5
		numIn := method.Type.NumIn()
		assert.Equal(t, 5, numIn, "UploadWithOptions should accept context, key, reader, contentType, ...UploadOption")

		assert.True(t, method.Type.IsVariadic(), "UploadWithOptions should be variadic")

		numOut := method.Type.NumOut()
		assert.Equal(t, 2, numOut, "UploadWithOptions should return key and error")
	})
}

func newObjectStorageMock(t *testing.T) *portsmocks.MockObjectStorageClient {
	t.Helper()

	ctrl := gomock.NewController(t)

	return portsmocks.NewMockObjectStorageClient(ctrl)
}

func TestMockObjectStorageClient_ImplementsInterface(t *testing.T) {
	t.Parallel()

	mock := newObjectStorageMock(t)

	var client ObjectStorageClient = mock
	assert.NotNil(t, client)
}

func TestMockObjectStorageClient_Upload(t *testing.T) {
	t.Parallel()

	t.Run("successful upload", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		content := bytes.NewReader([]byte("test content"))

		mock.EXPECT().
			Upload(gomock.Any(), "test-file.csv", gomock.Any(), "text/csv").
			Return("bucket/test-file.csv", nil)
		resultKey, err := mock.Upload(context.Background(), "test-file.csv", content, "text/csv")

		require.NoError(t, err)
		assert.Equal(t, "bucket/test-file.csv", resultKey)
	})

	t.Run("upload error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errTestUploadFailed
		mock := newObjectStorageMock(t)

		mock.EXPECT().
			Upload(gomock.Any(), "test-file.csv", gomock.Any(), "text/csv").
			Return("", expectedErr)
		_, err := mock.Upload(context.Background(), "test-file.csv", nil, "text/csv")

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("empty key", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			Upload(gomock.Any(), "", gomock.Any(), "text/csv").
			Return("", errTestKeyCannotBeEmpty)
		_, err := mock.Upload(context.Background(), "", nil, "text/csv")

		require.Error(t, err)
	})
}

func TestMockObjectStorageClient_Download(t *testing.T) {
	t.Parallel()

	t.Run("successful download", func(t *testing.T) {
		t.Parallel()

		expectedContent := []byte("downloaded content")
		mock := newObjectStorageMock(t)

		mock.EXPECT().
			Download(gomock.Any(), "test-file.csv").
			Return(io.NopCloser(bytes.NewReader(expectedContent)), nil)

		reader, err := mock.Download(context.Background(), "test-file.csv")

		require.NoError(t, err)

		defer reader.Close()

		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, expectedContent, content)
	})

	t.Run("download not found", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().Download(gomock.Any(), "nonexistent.csv").Return(nil, errTestObjectNotFound)
		_, err := mock.Download(context.Background(), "nonexistent.csv")

		require.Error(t, err)
	})
}

func TestMockObjectStorageClient_Delete(t *testing.T) {
	t.Parallel()

	t.Run("successful delete", func(t *testing.T) {
		t.Parallel()

		deletedKeys := make([]string, 0)
		mock := newObjectStorageMock(t)

		mock.EXPECT().
			Delete(gomock.Any(), "test-file.csv").
			DoAndReturn(func(context.Context, string) error {
				deletedKeys = append(deletedKeys, "test-file.csv")
				return nil
			})
		err := mock.Delete(context.Background(), "test-file.csv")

		require.NoError(t, err)
		assert.Contains(t, deletedKeys, "test-file.csv")
	})

	t.Run("delete error", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().Delete(gomock.Any(), "protected-file.csv").Return(errTestPermissionDenied)
		err := mock.Delete(context.Background(), "protected-file.csv")

		require.Error(t, err)
	})
}

func TestMockObjectStorageClient_GeneratePresignedURL(t *testing.T) {
	t.Parallel()

	t.Run("successful presigned URL generation", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().GeneratePresignedURL(gomock.Any(), "test-file.csv", 1*time.Hour).
			Return("https://bucket.s3.amazonaws.com/test-file.csv?expiry=1h0m0s", nil)
		url, err := mock.GeneratePresignedURL(context.Background(), "test-file.csv", 1*time.Hour)

		require.NoError(t, err)
		assert.Contains(t, url, "test-file.csv")
		assert.Contains(t, url, "1h0m0s")
	})

	t.Run("presigned URL error", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			GeneratePresignedURL(gomock.Any(), "nonexistent.csv", 1*time.Hour).
			Return("", errTestObjectNotFound)
		_, err := mock.GeneratePresignedURL(context.Background(), "nonexistent.csv", 1*time.Hour)

		require.Error(t, err)
	})

	t.Run("expiry duration validation", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			GeneratePresignedURL(gomock.Any(), "test.csv", 8*24*time.Hour).
			Return("", errTestExpiryCannotExceed7Days)
		_, err := mock.GeneratePresignedURL(context.Background(), "test.csv", 8*24*time.Hour)

		require.Error(t, err)
	})
}

func TestMockObjectStorageClient_Exists(t *testing.T) {
	t.Parallel()

	t.Run("object exists", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().Exists(gomock.Any(), "existing-file.csv").Return(true, nil)
		exists, err := mock.Exists(context.Background(), "existing-file.csv")

		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("object does not exist", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().Exists(gomock.Any(), "nonexistent-file.csv").Return(false, nil)
		exists, err := mock.Exists(context.Background(), "nonexistent-file.csv")

		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("exists check error", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().Exists(gomock.Any(), "protected-file.csv").Return(false, errTestAccessDenied)
		_, err := mock.Exists(context.Background(), "protected-file.csv")

		require.Error(t, err)
	})
}

func TestObjectStorageClient_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mock := newObjectStorageMock(t)

	mock.EXPECT().
		Upload(gomock.Any(), "test.csv", gomock.Any(), "text/csv").
		DoAndReturn(func(ctx context.Context, _ string, _ io.Reader, _ string) (string, error) {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			default:
				return "key", nil
			}
		})

	_, err := mock.Upload(ctx, "test.csv", nil, "text/csv")

	require.ErrorIs(t, err, context.Canceled)
}

func TestMockObjectStorageClient_UploadWithOptions(t *testing.T) {
	t.Parallel()

	t.Run("successful upload with storage class", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		content := bytes.NewReader([]byte("archive data"))

		mock.EXPECT().
			UploadWithOptions(gomock.Any(), "archives/audit_logs_2026_01.jsonl.gz", gomock.Any(), "application/gzip", gomock.Any()).
			Return("archives/audit_logs_2026_01.jsonl.gz", nil)

		resultKey, err := mock.UploadWithOptions(
			context.Background(),
			"archives/audit_logs_2026_01.jsonl.gz",
			content,
			"application/gzip",
			WithStorageClass("GLACIER"),
		)

		require.NoError(t, err)
		assert.Equal(t, "archives/audit_logs_2026_01.jsonl.gz", resultKey)
	})

	t.Run("upload with multiple options", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			UploadWithOptions(gomock.Any(), "key.gz", gomock.Any(), "application/gzip", gomock.Any(), gomock.Any()).
			Return("key.gz", nil)

		resultKey, err := mock.UploadWithOptions(
			context.Background(),
			"key.gz",
			nil,
			"application/gzip",
			WithStorageClass("DEEP_ARCHIVE"),
			WithServerSideEncryption("aws:kms"),
		)

		require.NoError(t, err)
		assert.Equal(t, "key.gz", resultKey)
	})

	t.Run("upload with no options", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			UploadWithOptions(gomock.Any(), "key.gz", gomock.Any(), "application/gzip").
			Return("key.gz", nil)

		resultKey, err := mock.UploadWithOptions(
			context.Background(),
			"key.gz",
			nil,
			"application/gzip",
		)

		require.NoError(t, err)
		assert.Equal(t, "key.gz", resultKey)
	})

	t.Run("upload error", func(t *testing.T) {
		t.Parallel()

		mock := newObjectStorageMock(t)

		mock.EXPECT().
			UploadWithOptions(gomock.Any(), "key.gz", gomock.Any(), "application/gzip", gomock.Any()).
			Return("", errTestUploadFailed)

		_, err := mock.UploadWithOptions(
			context.Background(),
			"key.gz",
			nil,
			"application/gzip",
			WithStorageClass("GLACIER"),
		)

		require.ErrorIs(t, err, errTestUploadFailed)
	})
}

func TestObjectStorageClient_ContentTypes(t *testing.T) {
	t.Parallel()

	contentTypes := []struct {
		format      string
		contentType string
	}{
		{format: "csv", contentType: "text/csv"},
		{format: "json", contentType: "application/json"},
		{format: "xml", contentType: "application/xml"},
		{format: "pdf", contentType: "application/pdf"},
	}

	for _, ct := range contentTypes {
		t.Run(ct.format, func(t *testing.T) {
			t.Parallel()

			var receivedContentType string

			mock := newObjectStorageMock(t)

			mock.EXPECT().
				Upload(gomock.Any(), "file."+ct.format, gomock.Any(), ct.contentType).
				DoAndReturn(func(_ context.Context, _ string, _ io.Reader, contentType string) (string, error) {
					receivedContentType = contentType
					return "key", nil
				})
			_, err := mock.Upload(context.Background(), "file."+ct.format, nil, ct.contentType)

			require.NoError(t, err)
			assert.Equal(t, ct.contentType, receivedContentType)
		})
	}
}
