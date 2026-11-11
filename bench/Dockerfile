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
# The distances of the vector index are summed with the vector instructions of the processor (the
# package simd of Go), as pgvector's are with the compiler's; without the experiment they are
# summed in plain Go. Only the vector workload is affected. Pass GOEXPERIMENT= to build without.
ARG GOEXPERIMENT=simd
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOEXPERIMENT=${GOEXPERIMENT} go build -trimpath \
      -ldflags "-s -w -X github.com/ehsanranjbar/kvutil/bench/internal/version.KvutilCommit=${KVUTIL_COMMIT}" \
      -o /out/kvbench ./cmd/kvbench

FROM debian:trixie-slim
COPY --from=build /out/kvbench /usr/local/bin/kvbench
ENV KVB_DATA_DIR=/data
VOLUME /data
ENTRYPOINT ["kvbench"]
CMD ["list"]
