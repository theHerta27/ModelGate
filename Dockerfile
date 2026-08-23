# syntax=docker/dockerfile:1

ARG GO_VERSION=1.26.4
ARG ALPINE_VERSION=3.23

FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS build-base
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

FROM build-base AS test
CMD ["go", "test", "./..."]

FROM build-base AS build
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/modelgate ./cmd/server

FROM alpine:${ALPINE_VERSION} AS runtime
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 modelgate \
    && adduser -S -D -H -u 10001 -G modelgate modelgate
COPY --from=build /out/modelgate /usr/local/bin/modelgate
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=6 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["modelgate"]
