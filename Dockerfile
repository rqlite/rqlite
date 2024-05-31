FROM golang:alpine as builder

ARG VERSION
ARG COMMIT
ARG BRANCH
ARG DATE

RUN apk add --no-cache gcc musl-dev

COPY . /app

WORKDIR /app

RUN CGO_ENABLED=1 go build -a -tags sqlite_omit_load_extension -ldflags="-w -s -X github.com/rqlite/rqlite/v8/cmd.CompilerCommand=musl-gcc -X github.com/rqlite/rqlite/v8/cmd.Version=${VERSION} -X github.com/rqlite/rqlite/v8/cmd.Branch=${BRANCH} -X github.com/rqlite/rqlite/v8/cmd.Commit=${COMMIT} -X github.com/rqlite/rqlite/v8/cmd.Buildtime=${DATE}" ./cmd/rqlited/.
RUN CGO_ENABLED=1 go build -a -tags sqlite_omit_load_extension -ldflags="-w -s" ./cmd/rqlite/.

FROM alpine:latest

COPY --from=builder /app/docker-entrypoint.sh /bin
COPY --from=builder /app/rqlited /bin
COPY --from=builder /app/rqlite /bin

RUN mkdir -p /rqlite/file
VOLUME /rqlite/file

EXPOSE 4001 4001

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["rqlite"]
