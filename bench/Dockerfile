# syntax=docker/dockerfile:1
#
# The harness, as a binary. The context is the root of the repository (bench has a replace
# directive on the module above it), which docker-compose.yml sets.

FROM golang:1.27 AS build
WORKDIR /src

# The modules first, so that a change of the code does not download them again.
COPY go.mod go.sum ./
COPY bench/go.mod bench/go.sum ./bench/
WORKDIR /src/bench
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /src
COPY . .
WORKDIR /src/bench
ARG KVUTIL_COMMIT=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/ehsanranjbar/kvutil/bench/internal/version.KvutilCommit=${KVUTIL_COMMIT}" \
      -o /out/kvbench ./cmd/kvbench

FROM debian:trixie-slim
COPY --from=build /out/kvbench /usr/local/bin/kvbench
ENV KVB_DATA_DIR=/data
VOLUME /data
ENTRYPOINT ["kvbench"]
CMD ["list"]
