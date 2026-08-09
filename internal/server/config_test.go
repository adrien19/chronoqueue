package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticationDefaults(t *testing.T) {
	t.Setenv("AUTH_ENABLED", "")
	t.Setenv("API_KEYS", "")

	assert.False(t, DefaultConfig().AuthEnabled)
	assert.True(t, ProductionConfig().AuthEnabled)
}

func TestValidateAuthentication(t *testing.T) {
	config := DefaultConfig()
	config.AuthEnabled = true

	err := config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no API keys configured")

	config.APIKeys = []string{"secret"}
	assert.NoError(t, config.Validate())

	config.APIKeys = []string{""}
	err = config.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be empty")
}
