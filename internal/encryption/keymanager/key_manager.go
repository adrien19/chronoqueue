package keymanager

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/adrien19/chronoqueue/internal/encryption/adapters"
	"github.com/adrien19/chronoqueue/pkg/log"
)

const defaultRefreshDuration = 1 * time.Hour

type KeyAdapter interface {
	FetchKey() ([]byte, error)
}

type EncryptionKeyManager struct {
	Enabled      bool
	adapter      KeyAdapter
	refreshDelay time.Duration
	cache        struct {
		sync.RWMutex
		key []byte
	}
	logger *log.Logger
}

func NewEncryptionKeyManager(logger *log.Logger) (*EncryptionKeyManager, error) {
	encryptionEnabled := envString("ENABLE_ENCRYPTION", "false")
	if encryptionEnabled != "true" {
		return &EncryptionKeyManager{
			Enabled:      false,
			adapter:      nil,
			refreshDelay: 0,
			cache: struct {
				sync.RWMutex
				key []byte
			}{},
			logger: nil,
		}, nil
	}

	manager := &EncryptionKeyManager{}
	var adapter KeyAdapter
	sourceType := os.Getenv("ENCRYPTION_KEY_SOURCE_TYPE")

	switch sourceType {
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

	return manager, nil
}

func (m *EncryptionKeyManager) GetEncryptionKey() ([]byte, error) {
	m.cache.RLock()
	defer m.cache.RUnlock()

	return m.cache.key, nil
}

func (m *EncryptionKeyManager) refreshKey() error {
	key, err := m.adapter.FetchKey()
	if err != nil {
		return err
	}
	keySize := len(key)
	if keySize != 16 && keySize != 24 && keySize != 32 {
		m.logger.FatalWithFields("Invalid encryption key size", "bytes", keySize)
	}

	m.cache.Lock()
	m.cache.key = key
	m.cache.Unlock()

	return nil
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
