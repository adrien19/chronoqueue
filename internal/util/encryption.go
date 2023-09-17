package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

// Encryption utility function
func EncryptPayload(payload []byte) (string, string, error) {
	// TODO: Fetch the encryption key from your key management setup
	key := []byte("your-encryption-key-here") // This is just a placeholder.

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", "", err
	}

	// Generate a new nonce for each encryption
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}

	ciphertext := aesgcm.Seal(nil, nonce, payload, nil)

	// Convert encryptedPayload and nonce both to base64
	base64Ciphertext := base64.StdEncoding.EncodeToString(ciphertext)
	base64Nonce := base64.StdEncoding.EncodeToString(nonce)

	return base64Ciphertext, base64Nonce, nil
}

// Decryption utility function
func DecryptPayload(base64Ciphertext string, base64Nonce string) ([]byte, error) {
	// TODO: Fetch the encryption key from your key management setup
	key := []byte("your-encryption-key-here") // This is just a placeholder.

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Decode the base64 strings back to []byte
	ciphertext, err := base64.StdEncoding.DecodeString(base64Ciphertext)
	if err != nil {
		return nil, errors.New("failed to decode encryptedPayload from base64")
	}

	nonce, err := base64.StdEncoding.DecodeString(base64Nonce)
	if err != nil {
		return nil, errors.New("failed to decode nonce from base64")
	}

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
