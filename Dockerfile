FROM golang:1.10-stretch
ADD . /go/src/github.com/rqlite/rqlite
WORKDIR /go/src/github.com/rqlite/rqlite
RUN go get ./...
RUN go install -ldflags="-X main.commit=$(git rev-parse HEAD) -X main.branch=$(git rev-parse --abbrev-ref HEAD)" ./cmd/...

FROM frolvlad/alpine-glibc:alpine-3.8
COPY --from=0 /go/bin/rqlite* /usr/local/bin/

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file
EXPOSE 4001 4002

COPY docker-entrypoint.sh /bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["rqlited", "-http-addr", "0.0.0.0:4001", "-raft-addr", "0.0.0.0:4002", "/rqlite/file/data"]
