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
    libxml2 \
    libxml2-dev \
    make \
    minizip-dev \
    musl-dev \
    pkgconf \
    proj \
    proj-dev \
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

# Some extensions require that SQLite be available, so build it first.
RUN curl https://www.sqlite.org/2024/sqlite-autoconf-3460100.tar.gz -o sqlite.tar.gz && \
    tar xvfz sqlite.tar.gz && \
    cd sqlite-autoconf-3460100 && \
    CPPFLAGS='-DATOMIC_INTRINSICS=1 -DDEFAULT_AUTOVACUUM -DDEFAULT_CACHE_SIZE=-2000 -DDEFAULT_FILE_FORMAT=4 -DDEFAULT_JOURNAL_SIZE_LIMIT=-1 -DDEFAULT_MMAP_SIZE=0 -DDEFAULT_PAGE_SIZE=4096 -DDEFAULT_PCACHE_INITSZ=20 -DDEFAULT_RECURSIVE_TRIGGERS -DDEFAULT_SECTOR_SIZE=4096 -DDEFAULT_SYNCHRONOUS=2 -DDEFAULT_WAL_AUTOCHECKPOINT=1000 -DDEFAULT_WAL_SYNCHRONOUS=1 -DDEFAULT_WORKER_THREADS=0 -DDIRECT_OVERFLOW_READ -DENABLE_DBSTAT_VTAB -DENABLE_FTS3 -DENABLE_FTS3_PARENTHESIS -DENABLE_FTS5 -DENABLE_GEOPOLY -DENABLE_RTREE -DENABLE_UPDATE_DELETE_LIMIT -DMALLOC_SOFT_LIMIT=1024 -DMAX_ATTACHED=10 -DMAX_COLUMN=2000 -DMAX_COMPOUND_SELECT=500 -DMAX_DEFAULT_PAGE_SIZE=8192 -DMAX_EXPR_DEPTH=1000 -DMAX_FUNCTION_ARG=127 -DMAX_LENGTH=1000000000 -DMAX_LIKE_PATTERN_LENGTH=50000 -DMAX_MMAP_SIZE=0x7fff0000 -DMAX_PAGE_COUNT=0xfffffffe -DMAX_PAGE_SIZE=65536 -DMAX_SQL_LENGTH=1000000000 -DMAX_TRIGGER_DEPTH=1000 -DMAX_VARIABLE_NUMBER=32766 -DMAX_VDBE_OP=250000000 -DMAX_WORKER_THREADS=8 -DMUTEX_PTHREADS -DOMIT_DEPRECATED -DOMIT_SHARED_CACHE -DSYSTEM_MALLOC -DTEMP_STORE=1 -DTHREADSAFE=1' ./configure --disable-tcl --enable-shared --disable-static && \
    make && make install

RUN curl -L `curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlean.tar.gz && \
    tar xvfz sqlean.tar.gz && \
    cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && zip -j /extensions/sqlean.zip dist/sqlean.so

RUN curl -L `curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlite-vec.tar.gz && \
    tar xvfz sqlite-vec.tar.gz && \
    cd asg017* && sh scripts/vendor.sh && echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c && make loadable && zip -j /extensions/sqlite-vec.zip dist/vec0.so

RUN git clone https://github.com/rqlite/rqlite-sqlite-ext.git
RUN cd rqlite-sqlite-ext/misc && make && zip /extensions/misc.zip *.so
RUN cd rqlite-sqlite-ext/icu && gcc -fPIC -shared icu.c `pkg-config --libs --cflags icu-uc icu-io` -o icu.so && zip /extensions/icu.zip icu.so

RUN curl https://www.gaia-gis.it/gaia-sins/libspatialite-sources/libspatialite-5.1.0.tar.gz -o libspatialite.tar.gz && \
    tar xvfz libspatialite.tar.gz && \
    cd libspatialite-5.1.0 && \
    ./configure --disable-freexl --disable-geos && \
    make && zip -j /extensions/spatialite.zip .libs/libspatialite.so

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
