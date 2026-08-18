package keymanager

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/adrien19/chronoqueue/internal/encryption/adapters"
	"github.com/adrien19/chronoqueue/pkg/log"
)

const defaultRefreshDuration = 1 * time.Hour

type KeyAdapter interface {
	FetchKeys() (*adapters.KeySet, error)
}

type Config struct {
	Enabled    bool
	SourceType string
}

type EncryptionKeyManager struct {
	Enabled      bool
	adapter      KeyAdapter
	refreshDelay time.Duration
	refreshMu    sync.Mutex
	cache        struct {
		sync.RWMutex
		currentID string
		keys      map[string][]byte
	}
	logger *log.Logger
}

func NewEncryptionKeyManager(logger *log.Logger) (*EncryptionKeyManager, error) {
	return NewEncryptionKeyManagerWithConfig(logger, Config{
		Enabled:    envString("ENABLE_ENCRYPTION", "false") == "true",
		SourceType: os.Getenv("ENCRYPTION_KEY_SOURCE_TYPE"),
	})
}

func NewEncryptionKeyManagerWithConfig(logger *log.Logger, config Config) (*EncryptionKeyManager, error) {
	logger.InfoWithFields("Checking encryption configuration", "enabled", config.Enabled)

	if !config.Enabled {
		logger.Info("Encryption is DISABLED - returning disabled key manager")
		return &EncryptionKeyManager{
			Enabled:      false,
			adapter:      nil,
			refreshDelay: 0,
			cache: struct {
				sync.RWMutex
				currentID string
				keys      map[string][]byte
			}{},
			logger: nil,
		}, nil
	}

	logger.Info("Encryption is ENABLED - initializing key manager")

	manager := &EncryptionKeyManager{}
	var adapter KeyAdapter

	switch config.SourceType {
	case "LOCAL":
		adapter = adapters.NewLocalAdapter()
	case "VAULT":
		adapter = adapters.NewVaultAdapter()
	default:
		return nil, errors.New("unsupported key source type")
	}
	manager.adapter = adapter
	manager.Enabled = true
	manager.logger = logger

	// Get refresh duration from env or use default
	refreshDurationStr := os.Getenv("KEY_REFRESH_DURATION_IN_MINUTES")
	if refreshDurationStr != "" {
		durationInMinutes, err := strconv.Atoi(refreshDurationStr)
		if err != nil {
			// Log the error and use default duration
			manager.logger.WarnWithFields("No KEY_REFRESH_DURATION_IN_MINUTES, using default - 1 hour.", "error", err)
			manager.refreshDelay = defaultRefreshDuration
		} else {
			manager.refreshDelay = time.Duration(durationInMinutes) * time.Minute
		}
	} else {
		manager.refreshDelay = defaultRefreshDuration
	}

	err := manager.refreshKey()
	if err != nil {
		return nil, err
	}
	// Start the background routine to refresh the key
	go manager.keyRefresher()

	manager.logger.InfoWithFields(
		"Encryption key manager initialized successfully",
		"enabled", manager.Enabled,
		"sourceType", config.SourceType,
		"refreshDelayMinutes", manager.refreshDelay.Minutes(),
	)
	return manager, nil
}

func (m *EncryptionKeyManager) GetEncryptionKey() ([]byte, error) {
	_, key, err := m.GetCurrentEncryptionKey()
	return key, err
}

func (m *EncryptionKeyManager) GetCurrentEncryptionKey() (string, []byte, error) {
	m.cache.RLock()
	defer m.cache.RUnlock()

	key, ok := m.cache.keys[m.cache.currentID]
	if !ok {
		return "", nil, errors.New("current encryption key is not available")
	}
	return m.cache.currentID, append([]byte(nil), key...), nil
}

func (m *EncryptionKeyManager) GetDecryptionKeys(keyID string) ([][]byte, error) {
	keys := m.cachedDecryptionKeys(keyID)
	if len(keys) > 0 || keyID == "" {
		return keys, nil
	}

	if err := m.refreshKey(); err != nil {
		return nil, fmt.Errorf("refresh encryption keys: %w", err)
	}
	keys = m.cachedDecryptionKeys(keyID)
	if len(keys) == 0 {
		return nil, fmt.Errorf("encryption key %q is not available", keyID)
	}
	return keys, nil
}

func (m *EncryptionKeyManager) cachedDecryptionKeys(keyID string) [][]byte {
	m.cache.RLock()
	defer m.cache.RUnlock()

	if keyID != "" {
		key, ok := m.cache.keys[keyID]
		if !ok {
			return nil
		}
		return [][]byte{append([]byte(nil), key...)}
	}

	keys := make([][]byte, 0, len(m.cache.keys))
	if current, ok := m.cache.keys[m.cache.currentID]; ok {
		keys = append(keys, append([]byte(nil), current...))
	}
	for id, key := range m.cache.keys {
		if id != m.cache.currentID {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	return keys
}

func (m *EncryptionKeyManager) refreshKey() error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()

	keySet, err := m.adapter.FetchKeys()
	if err != nil {
		return err
	}
	if keySet == nil {
		return errors.New("encryption key source returned no key set")
	}

	keys := make(map[string][]byte, len(keySet.HistoricalKeys)+1)
	allKeys := append([][]byte{keySet.CurrentKey}, keySet.HistoricalKeys...)
	for _, key := range allKeys {
		keySize := len(key)
		if keySize != 16 && keySize != 24 && keySize != 32 {
			return fmt.Errorf("invalid encryption key size: %d bytes", keySize)
		}
		id := encryptionKeyID(key)
		keys[id] = append([]byte(nil), key...)
	}
	currentID := encryptionKeyID(keySet.CurrentKey)

	m.cache.Lock()
	m.cache.currentID = currentID
	m.cache.keys = keys
	m.cache.Unlock()

	return nil
}

func encryptionKeyID(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:])
}

func (m *EncryptionKeyManager) keyRefresher() {
	ticker := time.NewTicker(m.refreshDelay)
	for range ticker.C {
		err := m.refreshKey()
		if err != nil {
			m.logger.WarnWithFields("Error refreshing encryption key", "error", err)
		}
	}
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
