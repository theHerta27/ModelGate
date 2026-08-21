package provider

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSSEStreamIgnoresMetadataAndParsesData(t *testing.T) {
	body := &trackingReadCloser{Reader: strings.NewReader(
		": keep-alive\n" +
			"event: message\n" +
			"id: 1\n" +
			"data: {\"id\":\"chunk-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"mock\",\"choices\":[]}\n\n" +
			"data: [DONE]\n\n",
	)}
	stream := newSSEStream(context.Background(), body)

	chunk, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if chunk.ID != "chunk-1" {
		t.Fatalf("chunk ID = %q", chunk.ID)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Recv() error = %v, want io.EOF", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() after DONE error = %v, want stable io.EOF", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if body.closeCalls != 1 {
		t.Fatalf("body Close calls = %d, want 1", body.closeCalls)
	}
}

func TestSSEStreamRejectsMalformedChunk(t *testing.T) {
	stream := newSSEStream(
		context.Background(),
		&trackingReadCloser{Reader: strings.NewReader("data: not-json\n\n")},
	)

	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "decode SSE chunk") {
		t.Fatalf("Recv() error = %v, want decode error", err)
	}
}

func TestSSEStreamRejectsOversizedEvent(t *testing.T) {
	line := "data: " + strings.Repeat("x", maxSSEEventBytes+1) + "\n\n"
	stream := newSSEStream(
		context.Background(),
		&trackingReadCloser{Reader: strings.NewReader(line)},
	)

	if _, err := stream.Recv(); err == nil {
		t.Fatal("Recv() error = nil, want oversized event error")
	}
}

func TestSSEStreamRejectsOversizedMetadataEvent(t *testing.T) {
	lineCount := maxSSEEventBytes/len("event: x\n") + 1
	stream := newSSEStream(
		context.Background(),
		&trackingReadCloser{Reader: strings.NewReader(strings.Repeat("event: x\n", lineCount))},
	)

	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "SSE event exceeds") {
		t.Fatalf("Recv() error = %v, want event size error", err)
	}
}

func TestSSEStreamHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newSSEStream(ctx, &trackingReadCloser{Reader: strings.NewReader("")})

	if _, err := stream.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv() error = %v, want context.Canceled", err)
	}
}

type trackingReadCloser struct {
	io.Reader
	closeCalls int
}

func (r *trackingReadCloser) Close() error {
	r.closeCalls++
	return nil
}
