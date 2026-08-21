package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
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
	resp, err := p.doRequest(ctx, req, "application/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseBytes+1))
	if err != nil {
		return nil, &UpstreamError{
			Provider: p.name, Operation: "read response", Kind: ErrorKindNetwork,
			Retryable: retryableTransportError(err), Cause: err,
		}
	}
	if len(responseBody) > maxUpstreamResponseBytes {
		return nil, &UpstreamError{
			Provider: p.name, Operation: "read response", Kind: ErrorKindInvalidResponse,
			Cause: fmt.Errorf("response exceeds %d bytes", maxUpstreamResponseBytes),
		}
	}

	var chatResponse ChatResponse
	if err := json.Unmarshal(responseBody, &chatResponse); err != nil {
		return nil, &UpstreamError{
			Provider: p.name, Operation: "decode response", Kind: ErrorKindInvalidResponse,
			Cause: err,
		}
	}
	if chatResponse.ID == "" || len(chatResponse.Choices) == 0 {
		return nil, &UpstreamError{
			Provider: p.name, Operation: "validate response", Kind: ErrorKindInvalidResponse,
			Cause: fmt.Errorf("provider returned an incomplete response"),
		}
	}
	chatResponse.Provider = p.name

	return &chatResponse, nil
}

func (p *OpenAICompatibleProvider) ChatStream(
	ctx context.Context,
	req *ChatRequest,
) (Stream, error) {
	streamRequest := *req
	streamRequest.Stream = true

	resp, err := p.doRequest(ctx, &streamRequest, "text/event-stream")
	if err != nil {
		return nil, err
	}

	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		_ = resp.Body.Close()
		return nil, &UpstreamError{
			Provider: p.name, Operation: "validate stream response", Kind: ErrorKindInvalidResponse,
			Cause: fmt.Errorf("provider returned a non-SSE response"),
		}
	}

	return newSSEStream(ctx, resp.Body), nil
}

func (p *OpenAICompatibleProvider) doRequest(
	ctx context.Context,
	req *ChatRequest,
	accept string,
) (*http.Response, error) {
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
	httpReq.Header.Set("Accept", accept)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, &UpstreamError{
			Provider: p.name, Operation: "request", Kind: ErrorKindNetwork,
			Retryable: retryableTransportError(err), Cause: err,
		}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = resp.Body.Close()
		return nil, &UpstreamError{
			Provider: p.name, Operation: "request", Kind: ErrorKindHTTPStatus,
			StatusCode: resp.StatusCode,
			Retryable:  resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable,
		}
	}
	return resp, nil
}
