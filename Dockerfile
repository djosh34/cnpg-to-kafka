# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.1-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" -o /out/cnpg-to-kafka ./cmd/collector

FROM scratch
COPY --from=build /out/cnpg-to-kafka /cnpg-to-kafka
ENTRYPOINT ["/cnpg-to-kafka"]
