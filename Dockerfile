# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends patch \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
ENV GOTOOLCHAIN=local GOMAXPROCS=2
COPY . .
RUN ./scripts/prepare-go.sh
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -p 1 -mod=vendor \
    -trimpath -ldflags="-s -w" -o /out/cnpg-to-kafka ./cmd/collector

FROM scratch
COPY --from=build /out/cnpg-to-kafka /cnpg-to-kafka
ENTRYPOINT ["/cnpg-to-kafka"]
CMD ["--config=/etc/cnpg-to-kafka/config.yaml"]
