FROM golang:alpine as builder

COPY . /app

WORKDIR /app

RUN CGO_ENABLED=1 go build -a -tags sqlite_omit_load_extension -ldflags="-w -s" ./cmd/rqlited/.
RUN CGO_ENABLED=1 go build -a -tags sqlite_omit_load_extension -ldflags="-w -s" ./cmd/rqlite/.

FROM alpine:latest

COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file

EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]

