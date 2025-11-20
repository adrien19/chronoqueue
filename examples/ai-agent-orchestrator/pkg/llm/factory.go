package llm

import "fmt"

// NewLLMClient creates an LLM client based on the provided configuration
func NewLLMClient(config *LLMConfig, verbose bool) (LLMClient, error) {
if config == nil {
// Default to mock client
if verbose {
fmt.Println("[LLM Factory] No config provided, using mock client")
}
return NewMockLLMClient(verbose), nil
}

if verbose {
fmt.Printf("[LLM Factory] Creating %s client\n", config.DefaultProvider)
}

switch config.DefaultProvider {
case ProviderMock:
return NewMockLLMClient(verbose), nil

case ProviderOllama:
if config.Providers.Ollama == nil {
return nil, fmt.Errorf("ollama provider selected but no configuration provided")
}
return NewOllamaClient(config.Providers.Ollama, verbose)

case ProviderOpenAI:
if config.Providers.OpenAI == nil {
return nil, fmt.Errorf("openai provider selected but no configuration provided")
}
return nil, fmt.Errorf("openai client not yet implemented")

case ProviderAnthropic:
if config.Providers.Anthropic == nil {
return nil, fmt.Errorf("anthropic provider selected but no configuration provided")
}
return nil, fmt.Errorf("anthropic client not yet implemented")

default:
return nil, fmt.Errorf("unsupported provider: %s", config.DefaultProvider)
}
}

// NewLLMClientWithFallback creates an LLM client with fallback support
func NewLLMClientWithFallback(config *LLMConfig, verbose bool) (LLMClient, error) {
// Try primary provider
client, err := NewLLMClient(config, verbose)
if err == nil {
return client, nil
}

if verbose {
fmt.Printf("[LLM Factory] Primary provider failed: %v\n", err)
}

// Try fallback providers
for _, fallbackProvider := range config.FallbackProviders {
if verbose {
fmt.Printf("[LLM Factory] Trying fallback provider: %s\n", fallbackProvider)
}

fallbackConfig := &LLMConfig{
DefaultProvider: fallbackProvider,
Providers:       config.Providers,
}

client, err := NewLLMClient(fallbackConfig, verbose)
if err == nil {
if verbose {
fmt.Printf("[LLM Factory] Fallback provider %s succeeded\n", fallbackProvider)
}
return client, nil
}

if verbose {
fmt.Printf("[LLM Factory] Fallback provider %s failed: %v\n", fallbackProvider, err)
}
}

// All providers failed, return mock as last resort
if verbose {
fmt.Println("[LLM Factory] All providers failed, falling back to mock client")
}
return NewMockLLMClient(verbose), nil
}
