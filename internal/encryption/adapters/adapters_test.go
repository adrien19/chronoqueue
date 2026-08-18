package adapters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalAdapterFetchKeys(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "abcdef0123456789")
	t.Setenv("ENCRYPTION_PREVIOUS_KEYS", `["0123456789abcdef","0123456789abcdefghijklmn"]`)

	keySet, err := NewLocalAdapter().FetchKeys()
	require.NoError(t, err)
	assert.Equal(t, []byte("abcdef0123456789"), keySet.CurrentKey)
	assert.Equal(t, [][]byte{
		[]byte("0123456789abcdef"),
		[]byte("0123456789abcdefghijklmn"),
	}, keySet.HistoricalKeys)
}

func TestLocalAdapterFetchKeysRejectsInvalidHistory(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "abcdef0123456789")
	t.Setenv("ENCRYPTION_PREVIOUS_KEYS", "not-json")

	_, err := NewLocalAdapter().FetchKeys()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse ENCRYPTION_PREVIOUS_KEYS")
}

func TestVaultKeySetSupportsKV2History(t *testing.T) {
	keySet, err := vaultKeySet(map[string]interface{}{
		"data": map[string]interface{}{
			"key":           "abcdef0123456789",
			"previous_keys": []interface{}{"0123456789abcdef"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("abcdef0123456789"), keySet.CurrentKey)
	assert.Equal(t, [][]byte{[]byte("0123456789abcdef")}, keySet.HistoricalKeys)
}

func TestVaultKeySetRejectsNonStringHistory(t *testing.T) {
	_, err := vaultKeySet(map[string]interface{}{
		"key":           "abcdef0123456789",
		"previous_keys": []interface{}{42},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "previous encryption key is not a string")
}
