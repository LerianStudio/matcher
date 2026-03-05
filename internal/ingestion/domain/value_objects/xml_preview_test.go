//go:build unit

package value_objects

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsXMLRecordElement(t *testing.T) {
	t.Parallel()

	assert.True(t, IsXMLRecordElement("transaction"))
	assert.True(t, IsXMLRecordElement("TRANSACTION"))
	assert.True(t, IsXMLRecordElement("entry"))
	assert.False(t, IsXMLRecordElement("header"))
}

func TestStripUTF8BOM(t *testing.T) {
	t.Parallel()

	t.Run("strips bom when present", func(t *testing.T) {
		t.Parallel()

		input := append([]byte{0xEF, 0xBB, 0xBF}, []byte("id,name\n1,test")...)
		reader, err := StripUTF8BOM(bytes.NewReader(input))
		require.NoError(t, err)

		out, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,test", string(out))
	})

	t.Run("keeps data when bom absent", func(t *testing.T) {
		t.Parallel()

		reader, err := StripUTF8BOM(strings.NewReader("id,name\n1,test"))
		require.NoError(t, err)

		out, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "id,name\n1,test", string(out))
	})
}
