package keymanager

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/internal/encryption/adapters"
	"github.com/adrien19/chronoqueue/pkg/log"
)

type rotatingKeyAdapter struct {
	sync.Mutex
	keySets []*adapters.KeySet
	index   int
}

func (a *rotatingKeyAdapter) FetchKeys() (*adapters.KeySet, error) {
	a.Lock()
	defer a.Unlock()
	keySet := a.keySets[a.index]
	if a.index < len(a.keySets)-1 {
		a.index++
	}
	return keySet, nil
}

func TestNewEncryptionKeyManagerWithConfig(t *testing.T) {
	logger := log.NewLogger()

	t.Run("disabled", func(t *testing.T) {
		manager, err := NewEncryptionKeyManagerWithConfig(logger, Config{})
		require.NoError(t, err)
		assert.False(t, manager.Enabled)
	})

	t.Run("local key", func(t *testing.T) {
		t.Setenv("ENCRYPTION_KEY", "0123456789abcdef")
		manager, err := NewEncryptionKeyManagerWithConfig(logger, Config{Enabled: true, SourceType: "LOCAL"})
		require.NoError(t, err)
		assert.True(t, manager.Enabled)
	})

	t.Run("unsupported source", func(t *testing.T) {
		manager, err := NewEncryptionKeyManagerWithConfig(logger, Config{Enabled: true, SourceType: "FILE"})
		require.Error(t, err)
		assert.Nil(t, manager)
	})
}

func TestEncryptionKeyManager_RefreshesUnknownKeyAndUsesHistoricalKeys(t *testing.T) {
	oldKey := []byte("0123456789abcdef")
	newKey := []byte("abcdef0123456789")
	adapter := &rotatingKeyAdapter{keySets: []*adapters.KeySet{
		{CurrentKey: oldKey},
		{CurrentKey: newKey, HistoricalKeys: [][]byte{oldKey}},
	}}
	manager := &EncryptionKeyManager{Enabled: true, adapter: adapter, logger: log.NewLogger()}
	require.NoError(t, manager.refreshKey())

	oldID, current, err := manager.GetCurrentEncryptionKey()
	require.NoError(t, err)
	assert.Equal(t, oldKey, current)

	newID := encryptionKeyID(newKey)
	keys, err := manager.GetDecryptionKeys(newID)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, newKey, keys[0])

	currentID, current, err := manager.GetCurrentEncryptionKey()
	require.NoError(t, err)
	assert.Equal(t, newID, currentID)
	assert.Equal(t, newKey, current)

	keys, err = manager.GetDecryptionKeys(oldID)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, oldKey, keys[0])
}

func TestEncryptionKeyManager_RefreshRemovesOmittedKeys(t *testing.T) {
	oldKey := []byte("0123456789abcdef")
	newKey := []byte("abcdef0123456789")
	adapter := &rotatingKeyAdapter{keySets: []*adapters.KeySet{
		{CurrentKey: oldKey},
		{CurrentKey: newKey},
	}}
	manager := &EncryptionKeyManager{Enabled: true, adapter: adapter, logger: log.NewLogger()}
	require.NoError(t, manager.refreshKey())
	require.NoError(t, manager.refreshKey())

	keys, err := manager.GetDecryptionKeys(encryptionKeyID(oldKey))
	require.ErrorContains(t, err, "is not available")
	assert.Nil(t, keys)
}

func TestEncryptionKeyManager_RejectsInvalidHistoricalKey(t *testing.T) {
	manager := &EncryptionKeyManager{
		Enabled: true,
		adapter: &rotatingKeyAdapter{keySets: []*adapters.KeySet{{
			CurrentKey:     []byte("0123456789abcdef"),
			HistoricalKeys: [][]byte{[]byte("too-short")},
		}}},
		logger: log.NewLogger(),
	}

	err := manager.refreshKey()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid encryption key size")
}
