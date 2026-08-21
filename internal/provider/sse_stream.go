package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
)

const maxSSEEventBytes = 1 << 20

type sseStream struct {
	ctx     context.Context
	body    io.ReadCloser
	scanner *bufio.Scanner
	once    sync.Once
	err     error
	done    bool
}

func newSSEStream(ctx context.Context, body io.ReadCloser) *sseStream {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), maxSSEEventBytes)
	return &sseStream{ctx: ctx, body: body, scanner: scanner}
}

func (s *sseStream) Recv() (*ChatStreamChunk, error) {
	if s.done {
		return nil, io.EOF
	}

	dataLines := make([]string, 0, 1)
	eventBytes := 0

	for {
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		default:
		}

		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				if s.ctx.Err() != nil {
					return nil, s.ctx.Err()
				}
				return nil, fmt.Errorf("read SSE stream: %w", err)
			}
			if len(dataLines) > 0 {
				return s.decode(dataLines)
			}
			s.done = true
			return nil, io.EOF
		}

		line := strings.TrimSuffix(s.scanner.Text(), "\r")
		eventBytes += len(line) + 1
		if eventBytes > maxSSEEventBytes {
			return nil, fmt.Errorf("SSE event exceeds %d bytes", maxSSEEventBytes)
		}
		if line == "" {
			if len(dataLines) == 0 {
				eventBytes = 0
				continue
			}
			return s.decode(dataLines)
		}
		if strings.HasPrefix(line, ":") {
			continue
		}

		field, value, found := strings.Cut(line, ":")
		if !found || field != "data" {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		dataLines = append(dataLines, value)
	}
}

func (s *sseStream) decode(dataLines []string) (*ChatStreamChunk, error) {
	chunk, err := decodeSSEData(dataLines)
	if err == io.EOF {
		s.done = true
	}
	return chunk, err
}

func (s *sseStream) Close() error {
	s.once.Do(func() {
		s.err = s.body.Close()
	})
	return s.err
}

func decodeSSEData(dataLines []string) (*ChatStreamChunk, error) {
	data := strings.Join(dataLines, "\n")
	if strings.TrimSpace(data) == "[DONE]" {
		return nil, io.EOF
	}

	var chunk ChatStreamChunk
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return nil, fmt.Errorf("decode SSE chunk: %w", err)
	}
	if chunk.ID == "" || chunk.Object != "chat.completion.chunk" {
		return nil, fmt.Errorf("provider returned an incomplete SSE chunk")
	}
	return &chunk, nil
}
