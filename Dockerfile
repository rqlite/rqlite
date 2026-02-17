FROM golang:alpine AS builder

ARG VERSION="unknown"
ARG COMMIT="unknown"
ARG BRANCH="unknown"
ARG DATE="unknown"

RUN apk add --no-cache \
    curl \
    gcc \
    gettext \
    git \
    icu-dev \
    jq \
    linux-headers \
    make \
    musl-dev \
    pkgconf \
    zlib-dev \
    zip

COPY . /app

# Build rqlite.
WORKDIR /app
ENV CGO_ENABLED=1
RUN go build -ldflags=" \
    -w -s -X github.com/rqlite/rqlite/v10/cmd.CompilerCommand=musl-gcc \
    -X github.com/rqlite/rqlite/v10/cmd.Version=${VERSION} \
    -X github.com/rqlite/rqlite/v10/cmd.Branch=${BRANCH} \
    -X github.com/rqlite/rqlite/v10/cmd.Commit=${COMMIT} \
    -X github.com/rqlite/rqlite/v10/cmd.Buildtime=${DATE}" ./cmd/rqlited/. && \
    go build -ldflags="-w -s" ./cmd/rqlite/.

# Build the extensions, start by creating the extensions directory.
WORKDIR /extensions
WORKDIR /app

RUN sh tools/build-sqlean.sh /extensions

RUN sh tools/build-sqlite-vec.sh /extensions

RUN sh tools/build-sqliteai.sh /extensions "${TARGETOS:-}" "${TARGETARCH:-}"

RUN sh tools/build-rqlite-ext.sh /extensions

#######################################################################
# Phase 2: Create the final image.
FROM alpine:latest

RUN apk add --no-cache icu-libs

# Create the user and group (Alpine syntax).
# Using 1000 is standard convention for non-root users.
RUN addgroup -g 1000 rqlite && \
    adduser -u 1000 -G rqlite -D rqlite

# Copy the init script and rqlite binaries.
COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

# Bake in the extensions.
COPY --from=builder /extensions /opt/extensions/

# Create the directory and fix ownership permissions.
# This ensures the 'rqlite' user can write to this directory.
RUN mkdir -p /rqlite/file && \
    chown -R rqlite:rqlite /rqlite/file

VOLUME /rqlite/file

EXPOSE 4001 4002

# Switch to the non-root user.
USER rqlite

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
