package adapters

import (
	"errors"
	"os"
)

type LocalAdapter struct {
	envVarName string
}

func NewLocalAdapter() *LocalAdapter {
	return &LocalAdapter{
		envVarName: "ENCRYPTION_KEY",
	}
}

func (l *LocalAdapter) FetchKey() ([]byte, error) {
	key := os.Getenv(l.envVarName)
	if key == "" {
		return nil, errors.New("encryption key not set in environment variable")
	}
	return []byte(key), nil
}
