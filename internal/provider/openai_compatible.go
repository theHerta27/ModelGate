package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxUpstreamResponseBytes = 4 << 20

type OpenAICompatibleProvider struct {
	name     string
	endpoint string
	apiKey   string
	client   *http.Client
}

func NewOpenAICompatibleProvider(
	name string,
	baseURL string,
	apiKey string,
	client *http.Client,
) (*OpenAICompatibleProvider, error) {
	name = strings.TrimSpace(name)
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	apiKey = strings.TrimSpace(apiKey)

	if name == "" {
		return nil, fmt.Errorf("provider name is required")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("provider base URL must be an absolute URL")
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return nil, fmt.Errorf("provider base URL must use http or https")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("provider API key is required")
	}
	if client == nil {
		return nil, fmt.Errorf("HTTP client is required")
	}

	return &OpenAICompatibleProvider{
		name:     name,
		endpoint: baseURL + "/chat/completions",
		apiKey:   apiKey,
		client:   client,
	}, nil
}

func (p *OpenAICompatibleProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", p.name, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", p.name, err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call %s provider: %w", p.name, err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", p.name, err)
	}
	if len(responseBody) > maxUpstreamResponseBytes {
		return nil, fmt.Errorf("%s response exceeds %d bytes", p.name, maxUpstreamResponseBytes)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("%s provider returned HTTP %d", p.name, resp.StatusCode)
	}

	var chatResponse ChatResponse
	if err := json.Unmarshal(responseBody, &chatResponse); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", p.name, err)
	}
	if chatResponse.ID == "" || len(chatResponse.Choices) == 0 {
		return nil, fmt.Errorf("%s provider returned an incomplete response", p.name)
	}

	return &chatResponse, nil
}

func (p *OpenAICompatibleProvider) ChatStream(context.Context, *ChatRequest) (Stream, error) {
	return nil, ErrStreamingNotSupported
}
