package llm

import "time"

// ProviderType represents the LLM provider
type ProviderType string

const (
	ProviderMock      ProviderType = "mock"
	ProviderOllama    ProviderType = "ollama"
	ProviderOpenAI    ProviderType = "openai"
	ProviderAnthropic ProviderType = "anthropic"
)

// LLMConfig holds the complete LLM configuration
type LLMConfig struct {
	DefaultProvider   ProviderType    `yaml:"default_provider" json:"default_provider"`
	Providers         ProvidersConfig `yaml:"providers" json:"providers"`
	FallbackProviders []ProviderType  `yaml:"fallback_providers" json:"fallback_providers"`
	Streaming         bool            `yaml:"streaming" json:"streaming"`
	CacheEnabled      bool            `yaml:"cache_enabled" json:"cache_enabled"`
	CacheTTL          time.Duration   `yaml:"cache_ttl" json:"cache_ttl"`
}

// ProvidersConfig holds configurations for all providers
type ProvidersConfig struct {
	Ollama    *OllamaConfig    `yaml:"ollama,omitempty" json:"ollama,omitempty"`
	OpenAI    *OpenAIConfig    `yaml:"openai,omitempty" json:"openai,omitempty"`
	Anthropic *AnthropicConfig `yaml:"anthropic,omitempty" json:"anthropic,omitempty"`
}

// OllamaConfig holds Ollama-specific configuration
type OllamaConfig struct {
	BaseURL     string            `yaml:"base_url" json:"base_url"`
	Models      map[string]string `yaml:"models" json:"models"` // agent type -> model name
	Timeout     time.Duration     `yaml:"timeout" json:"timeout"`
	MaxRetries  int               `yaml:"max_retries" json:"max_retries"`
	Temperature float32           `yaml:"temperature" json:"temperature"`
}

// OpenAIConfig holds OpenAI-specific configuration
type OpenAIConfig struct {
	APIKeyEnv   string            `yaml:"api_key_env" json:"api_key_env"`
	Models      map[string]string `yaml:"models" json:"models"`
	Timeout     time.Duration     `yaml:"timeout" json:"timeout"`
	MaxRetries  int               `yaml:"max_retries" json:"max_retries"`
	Temperature float32           `yaml:"temperature" json:"temperature"`
}

// AnthropicConfig holds Anthropic-specific configuration
type AnthropicConfig struct {
	APIKeyEnv   string            `yaml:"api_key_env" json:"api_key_env"`
	Models      map[string]string `yaml:"models" json:"models"`
	Timeout     time.Duration     `yaml:"timeout" json:"timeout"`
	MaxRetries  int               `yaml:"max_retries" json:"max_retries"`
	Temperature float32           `yaml:"temperature" json:"temperature"`
}

// DefaultConfig returns a default LLM configuration with mock provider
func DefaultConfig() *LLMConfig {
	return &LLMConfig{
		DefaultProvider:   ProviderMock,
		Providers:         ProvidersConfig{},
		FallbackProviders: []ProviderType{},
		Streaming:         false,
		CacheEnabled:      false,
		CacheTTL:          time.Hour,
	}
}

// DefaultOllamaConfig returns a default Ollama configuration
func DefaultOllamaConfig() *OllamaConfig {
	return &OllamaConfig{
		BaseURL: "http://localhost:11434",
		Models: map[string]string{
			"coordinator":    "llama3.2:3b",
			"web-search":     "llama3.2:3b",
			"code-analyzer":  "qwen2.5-coder:7b",
			"data-processor": "llama3.2:3b",
			"aggregator":     "llama3.1:8b",
		},
		Timeout:     120 * time.Second,
		MaxRetries:  3,
		Temperature: 0.7,
	}
}
