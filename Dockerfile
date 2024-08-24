FROM golang:alpine as builder

ARG VERSION="unknown"
ARG COMMIT="unknown"
ARG BRANCH="unknown"
ARG DATE="unknown"

RUN apk add --no-cache zip gcc musl-dev curl make git gettext pkgconf vim icu-dev zlib-dev

COPY . /app

# Build rqlite.
WORKDIR /app
RUN CGO_ENABLED=1 go build -ldflags="-w -s -X github.com/rqlite/rqlite/v8/cmd.CompilerCommand=musl-gcc -X github.com/rqlite/rqlite/v8/cmd.Version=${VERSION} -X github.com/rqlite/rqlite/v8/cmd.Branch=${BRANCH} -X github.com/rqlite/rqlite/v8/cmd.Commit=${COMMIT} -X github.com/rqlite/rqlite/v8/cmd.Buildtime=${DATE}" ./cmd/rqlited/.
RUN CGO_ENABLED=1 go build -ldflags="-w -s" ./cmd/rqlite/.

# Build the extensions.
WORKDIR /app
RUN mkdir -p /extensions/icu && \
    gcc -fPIC -shared extensions/src/icu/icu.c -I extensions/src/ `pkg-config --libs --cflags icu-uc icu-io` -o /extensions/icu/icu.so && \
    cd /extensions/icu && zip icu.zip icu.so && rm icu.so

WORKDIR /extensions
RUN mkdir -p /extensions/sqlean && \
    curl -L `curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlean.tar.gz && \
    tar xvfz sqlean.tar.gz && \
    cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && cp dist/sqlean.so /extensions/sqlean && \
    cd /extensions/sqlean && zip sqlean.zip sqlean.so && rm sqlean.so

WORKDIR /extensions
RUN mkdir -p /extensions/sqlite-vec && \
    curl -L `curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlite-vec.tar.gz && \
    tar xvfz sqlite-vec.tar.gz && \
    cd asg017* && sh scripts/vendor.sh && echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c && make loadable && cp dist/* /extensions/sqlite-vec/ && \
    cd /extensions/sqlite-vec && zip sqlite-vec.zip *.so && rm *.so

WORKDIR /app
RUN mkdir -p /extensions/misc && \
    cd extensions/src/misc/ && make && cp *.so /extensions/misc && \
    cd /extensions/misc && zip misc.zip *.so && rm *.so

#######################################################################
# Phase 2: Create the final image.
FROM alpine:latest

RUN apk add --no-cache icu-libs

# Copy the init script and rqlite binaries.
COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

# Make all required directories.
RUN mkdir -p /opt/extensions/icu && \
    mkdir -p /opt/extensions/sqlean && \
    mkdir -p /opt/extensions/sqlite-vec && \
    mkdir -p /opt/extensions/misc && \
    mkdir -p /rqlite/file

# Bake in the extensions.
COPY --from=builder /extensions/icu/* /opt/extensions/icu
COPY --from=builder /extensions/sqlean/* /opt/extensions/sqlean
COPY --from=builder /extensions/sqlite-vec/* /opt/extensions/sqlite-vec
COPY --from=builder /extensions/misc/* /opt/extensions/misc

VOLUME /rqlite/file
EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["run"]
