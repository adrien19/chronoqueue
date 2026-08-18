package encryption

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestDecryptPayload_RejectsInvalidNonceLength(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "0123456789abcdef")
	t.Setenv("ENCRYPTION_PREVIOUS_KEYS", "")
	manager, err := keymanager.NewEncryptionKeyManagerWithConfig(log.NewLogger(), keymanager.Config{Enabled: true, SourceType: "LOCAL"})
	require.NoError(t, err)

	ciphertext := base64.StdEncoding.EncodeToString(make([]byte, 16))
	nonce := base64.StdEncoding.EncodeToString([]byte{1})

	_, err = DecryptPayload(ciphertext, nonce, "", manager)
	require.ErrorContains(t, err, "invalid nonce length")
}
