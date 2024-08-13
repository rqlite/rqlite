FROM golang:alpine as builder

ARG VERSION
ARG COMMIT
ARG BRANCH
ARG DATE

RUN apk add --no-cache gcc musl-dev curl make

COPY . /app
WORKDIR /app
RUN CGO_ENABLED=1 go build -a -tags -ldflags="-w -s -X github.com/rqlite/rqlite/v8/cmd.CompilerCommand=musl-gcc -X github.com/rqlite/rqlite/v8/cmd.Version=${VERSION} -X github.com/rqlite/rqlite/v8/cmd.Branch=${BRANCH} -X github.com/rqlite/rqlite/v8/cmd.Commit=${COMMIT} -X github.com/rqlite/rqlite/v8/cmd.Buildtime=${DATE}" ./cmd/rqlited/.
RUN CGO_ENABLED=1 go build -a -tags -ldflags="-w -s" ./cmd/rqlite/.

RUN mkdir -p /extensions/sqlean
WORKDIR /extensions
RUN curl -L `curl -s https://api.github.com/repos/nalgeon/sqlean/releases/latest | grep "tarball_url" | cut -d '"' -f 4` -o sqlean.tar.gz
RUN tar xvfz sqlean.tar.gz
RUN cd nalgeon* && make prepare-dist download-sqlite download-external compile-linux && cp dist/* /extensions/sqlean

FROM alpine:latest

COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

RUN mkdir -p /opt/extensions/sqlean
COPY --from=builder /extensions/sqlean/* /opt/extensions/sqlean

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file

EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["rqlite"]
