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

func FilterEmptyStrings(s []string) []string {
	j := 0
	for _, val := range s {
		if val != "" {
			s[j] = val
			j++
		}
	}
	return s[:j]
}
