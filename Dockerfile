FROM golang:alpine as builder

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
    -w -s -X github.com/rqlite/rqlite/v8/cmd.CompilerCommand=musl-gcc \
    -X github.com/rqlite/rqlite/v8/cmd.Version=${VERSION} \
    -X github.com/rqlite/rqlite/v8/cmd.Branch=${BRANCH} \
    -X github.com/rqlite/rqlite/v8/cmd.Commit=${COMMIT} \
    -X github.com/rqlite/rqlite/v8/cmd.Buildtime=${DATE}" ./cmd/rqlited/. && \
    go build -ldflags="-w -s" ./cmd/rqlite/.

# Build the extensions, start by creating the extensions directory.
WORKDIR /extensions
WORKDIR /app

RUN curl -L `curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlean.tar.gz && \
    tar xvfz sqlean.tar.gz && \
    cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && zip -j /extensions/sqlean.zip dist/sqlean.so

RUN curl -L `curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlite-vec.tar.gz && \
    tar xvfz sqlite-vec.tar.gz && \
    cd asg017* && sh scripts/vendor.sh && echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c && make loadable && zip -j /extensions/sqlite-vec.zip dist/vec0.so

RUN git clone https://github.com/rqlite/rqlite-sqlite-ext.git
RUN cd rqlite-sqlite-ext/misc && make && zip /extensions/misc.zip *.so
RUN cd rqlite-sqlite-ext/icu && gcc -fPIC -shared icu.c -I .. `pkg-config --libs --cflags icu-uc icu-io` -o icu.so && zip /extensions/icu.zip icu.so

#######################################################################
# Phase 2: Create the final image.
FROM alpine:latest

RUN apk add --no-cache icu-libs

# Copy the init script and rqlite binaries.
COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

# Bake in the extensions.
COPY --from=builder /extensions /opt/extensions/

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file
EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
