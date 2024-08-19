FROM golang:alpine as builder

ARG VERSION
ARG COMMIT
ARG BRANCH
ARG DATE

RUN apk add --no-cache gcc musl-dev curl make git gettext pkgconf vim icu-dev zlib-dev

COPY . /app

WORKDIR /app
RUN CGO_ENABLED=1 go build -a -ldflags="-w -s -X github.com/rqlite/rqlite/v8/cmd.CompilerCommand=musl-gcc -X github.com/rqlite/rqlite/v8/cmd.Version=${VERSION} -X github.com/rqlite/rqlite/v8/cmd.Branch=${BRANCH} -X github.com/rqlite/rqlite/v8/cmd.Commit=${COMMIT} -X github.com/rqlite/rqlite/v8/cmd.Buildtime=${DATE}" ./cmd/rqlited/.
RUN CGO_ENABLED=1 go build -a -ldflags="-w -s" ./cmd/rqlite/.

WORKDIR /app
RUN mkdir -p /extensions/icu
RUN gcc -fPIC -shared extensions/src/icu/icu.c -I extensions/src/ `pkg-config --libs --cflags icu-uc icu-io` -o /extensions/icu/icu.so

WORKDIR /extensions
RUN mkdir -p /extensions/sqlean
RUN curl -L `curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlean.tar.gz
RUN tar xvfz sqlean.tar.gz
RUN cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && cp dist/* /extensions/sqlean

WORKDIR /extensions
RUN mkdir -p /extensions/sqlite-vec
RUN curl -L `curl -s https://api.github.com/repos/asg017/sqlite-vec/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlite-vec.tar.gz
RUN tar xvfz sqlite-vec.tar.gz
RUN cd asg017* && sh scripts/vendor.sh && echo "#include <sys/types.h>" | cat - sqlite-vec.c > temp && mv temp sqlite-vec.c && make loadable && cp dist/* /extensions/sqlite-vec/

WORKDIR /app
RUN mkdir -p /extensions/misc
RUN cd extensions/src/misc/ && make && cp *.so /extensions/misc

FROM alpine:latest

RUN apk add --no-cache icu-libs

COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

RUN mkdir -p /opt/extensions
COPY --from=builder /extensions/* /opt/extensions

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file

EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["rqlite"]
