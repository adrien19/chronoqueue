package adapters

import (
	"encoding/json"
	"errors"
	"fmt"
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

func (l *LocalAdapter) FetchKeys() (*KeySet, error) {
	key := os.Getenv(l.envVarName)
	if key == "" {
		return nil, errors.New("encryption key not set in environment variable")
	}

	keySet := &KeySet{CurrentKey: []byte(key)}
	history := os.Getenv("ENCRYPTION_PREVIOUS_KEYS")
	if history == "" {
		return keySet, nil
	}

	var previousKeys []string
	if err := json.Unmarshal([]byte(history), &previousKeys); err != nil {
		return nil, fmt.Errorf("parse ENCRYPTION_PREVIOUS_KEYS: %w", err)
	}
	if previousKeys == nil {
		return nil, errors.New("ENCRYPTION_PREVIOUS_KEYS must be a JSON array")
	}
	for _, previousKey := range previousKeys {
		keySet.HistoricalKeys = append(keySet.HistoricalKeys, []byte(previousKey))
	}

	return keySet, nil
}
