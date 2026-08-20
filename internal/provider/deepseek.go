package provider

import "net/http"

// DeepSeekProvider has its own concrete type for configuration clarity while
// reusing the protocol behavior of the OpenAI-compatible adapter.
type DeepSeekProvider struct {
	*OpenAICompatibleProvider
}

func NewDeepSeekProvider(
	baseURL string,
	apiKey string,
	client *http.Client,
) (*DeepSeekProvider, error) {
	compatible, err := NewOpenAICompatibleProvider("deepseek", baseURL, apiKey, client)
	if err != nil {
		return nil, err
	}
	return &DeepSeekProvider{OpenAICompatibleProvider: compatible}, nil
}
