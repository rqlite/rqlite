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
    -w -s -X github.com/rqlite/rqlite/v9/cmd.CompilerCommand=musl-gcc \
    -X github.com/rqlite/rqlite/v9/cmd.Version=${VERSION} \
    -X github.com/rqlite/rqlite/v9/cmd.Branch=${BRANCH} \
    -X github.com/rqlite/rqlite/v9/cmd.Commit=${COMMIT} \
    -X github.com/rqlite/rqlite/v9/cmd.Buildtime=${DATE}" ./cmd/rqlited/. && \
    go build -ldflags="-w -s" ./cmd/rqlite/.

# Build the extensions, start by creating the extensions directory.
WORKDIR /extensions
WORKDIR /app

RUN set -e; \
    sqlean_url=$(curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | jq -r .tarball_url); \
    echo "Downloading sqlean from: $sqlean_url"; \
    curl -L "$sqlean_url" -o sqlean.tar.gz
RUN tar xvfz sqlean.tar.gz && \
    cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && zip -j /extensions/sqlean.zip dist/sqlean.so

RUN set -e; \
    sqlitevec_url=$(curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | jq -r .tarball_url); \
    echo "Downloading sqlite-vec from: $sqlitevec_url"; \
    curl -L "$sqlitevec_url" -o sqlite-vec.tar.gz
RUN tar xvfz sqlite-vec.tar.gz && \
    echo location >> ~/.curlrc && \
    cd asg017* && sh scripts/vendor.sh && echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c && make loadable && zip -j /extensions/sqlite-vec.zip dist/vec0.so

RUN set -euo pipefail; \
    \
    echo "TARGETOS=${TARGETOS:-<unset>} TARGETARCH=${TARGETARCH:-<unset>}"; \
    \
    if [ "${TARGETOS:-}" = "linux" ] && [ "${TARGETARCH:-}" = "386" ]; then \
        echo "Skipping sqliteai-vector entirely on ${TARGETOS}/${TARGETARCH}"; \
        exit 0; \
    fi; \
    \
    echo "[1/7] Fetch latest release metadata from GitHub"; \
    meta_json="$(mktemp)"; \
    if ! curl -fsSL "https://api.github.com/repos/sqliteai/sqlite-vector/releases/latest" -o "$meta_json"; then \
        echo "FAIL [1/7] curl GitHub API (releases/latest)"; \
        exit 1; \
    fi; \
    \
    echo "[2/7] Extract tarball_url via jq"; \
    sqliteai_vector="$(jq -er '.tarball_url' "$meta_json")" || { \
        echo "FAIL [2/7] jq parse .tarball_url (missing/invalid JSON)"; \
        echo "---- begin GitHub API response ----"; \
        head -c 2000 "$meta_json" || true; \
        echo; \
        echo "---- end GitHub API response ----"; \
        exit 1; \
    }; \
    rm -f "$meta_json"; \
    echo "tarball_url=$sqliteai_vector"; \
    \
    echo "[3/7] Download tarball"; \
    if ! curl -fL --retry 3 --retry-delay 2 "$sqliteai_vector" -o sqliteai-vector.tar.gz; then \
        echo "FAIL [3/7] curl tarball"; \
        exit 1; \
    fi; \
    \
    echo "[4/7] Extract tarball"; \
    if ! tar xvfz sqliteai-vector.tar.gz; then \
        echo "FAIL [4/7] tar extract"; \
        exit 1; \
    fi; \
    rm -f sqliteai-vector.tar.gz; \
    \
    echo "[5/7] Enter extracted directory"; \
    dir="$(ls -d sqliteai* 2>/dev/null | head -n 1)"; \
    if [ -z "$dir" ]; then \
        echo "FAIL [5/7] could not find extracted directory matching 'sqliteai*'"; \
        ls -la; \
        exit 1; \
    fi; \
    cd "$dir"; \
    \
    echo "[6/7] Build (make)"; \
    if ! make; then \
        echo "FAIL [6/7] make"; \
        exit 1; \
    fi; \
    \
    echo "[7/7] Package dist/vector.so into /extensions/sqliteai-vector.zip"; \
    if [ ! -f dist/vector.so ]; then \
        echo "FAIL [7/7] expected output file dist/vector.so not found"; \
        ls -la dist || true; \
        exit 1; \
    fi; \
    mkdir -p /extensions; \
    if ! zip -j /extensions/sqliteai-vector.zip dist/vector.so; then \
        echo "FAIL [7/7] zip"; \
        exit 1; \
    fi

RUN git clone https://github.com/rqlite/rqlite-sqlite-ext.git
RUN cd rqlite-sqlite-ext/misc && make && zip /extensions/misc.zip *.so
RUN cd rqlite-sqlite-ext/icu && gcc -fPIC -shared icu.c -I .. `pkg-config --libs --cflags icu-uc icu-io` -o icu.so && zip /extensions/icu.zip icu.so

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
