package keymanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/pkg/log"
)

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
