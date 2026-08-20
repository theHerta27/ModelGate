package provider

import (
	"fmt"
	"net/http"

	"github.com/theHerta27/ModelGate/internal/config"
)

func NewFromConfig(cfg config.Config) (Provider, error) {
	client := &http.Client{Timeout: cfg.RequestTimeout}

	switch cfg.Provider {
	case config.ProviderMock:
		return NewMockProvider(), nil
	case config.ProviderDeepSeek:
		return NewDeepSeekProvider(cfg.DeepSeekBaseURL, cfg.DeepSeekAPIKey, client)
	case config.ProviderOpenAICompatible:
		return NewOpenAICompatibleProvider(
			"openai-compatible",
			cfg.OpenAIBaseURL,
			cfg.OpenAIAPIKey,
			client,
		)
	default:
		return nil, fmt.Errorf("unsupported provider %q", cfg.Provider)
	}
}
