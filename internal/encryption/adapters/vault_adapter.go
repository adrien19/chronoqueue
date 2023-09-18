package adapters

import (
	"errors"
	"os"

	vaultapi "github.com/hashicorp/vault/api"
)

type VaultAdapter struct {
	authMethod    string
	vaultEndpoint string
	secretPath    string
}

func NewVaultAdapter() *VaultAdapter {
	return &VaultAdapter{
		authMethod:    os.Getenv("VAULT_AUTH_METHOD"),
		vaultEndpoint: os.Getenv("VAULT_ENDPOINT"),
		secretPath:    os.Getenv("VAULT_SECRET_PATH"),
	}
}

func (v *VaultAdapter) FetchKey() ([]byte, error) {
	config := vaultapi.DefaultConfig()
	config.Address = v.vaultEndpoint

	client, err := vaultapi.NewClient(config)
	if err != nil {
		return nil, err
	}

	switch v.authMethod {
	case "TOKEN":
		client.SetToken(os.Getenv("VAULT_TOKEN"))
	case "APPROLE":
		roleID := os.Getenv("VAULT_APPROLE_ROLE_ID")
		secretID := os.Getenv("VAULT_APPROLE_SECRET_ID")
		// Use AppRole to log in
		data := map[string]interface{}{
			"role_id":   roleID,
			"secret_id": secretID,
		}
		resp, err := client.Logical().Write("auth/approle/login", data)
		if err != nil {
			return nil, err
		}
		client.SetToken(resp.Auth.ClientToken)
	default:
		return nil, errors.New("unsupported auth method")
	}

	// Fetch the key from Vault
	secret, err := client.Logical().Read(v.secretPath)
	if err != nil {
		return nil, err
	}

	if secret == nil || secret.Data == nil {
		return nil, errors.New("no secret data returned")
	}

	key, ok := secret.Data["key"].(string)
	if !ok {
		return nil, errors.New("encryption key is not present or not a string")
	}

	return []byte(key), nil
}

// Environment Variables:

// VAULT_ENDPOINT: The endpoint where Vault is running.
// VAULT_AUTH_METHOD: The authentication method ("TOKEN" or "APPROLE").
// VAULT_TOKEN: If using token authentication, this is the token.
// VAULT_APPROLE_ROLE_ID and VAULT_APPROLE_SECRET_ID: If using AppRole authentication, these are the role ID and secret ID respectively.
// VAULT_SECRET_PATH: The path in Vault where the encryption key is stored.
