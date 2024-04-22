package util

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateID() (string, error) {
	id := make([]byte, 32)
	_, err := rand.Read(id)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(id), nil
}
