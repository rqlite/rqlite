FROM golang:alpine as builder

RUN apk add --no-cache gcc musl-dev

COPY . /app

WORKDIR /app

RUN CGO_ENABLED=1 go build -a -tags sqlite_omit_load_extension -ldflags="-w -s" -X $LINKER_PKG_PATH.CompilerCommand=musl-gcc -X $LINKER_PKG_PATH.Version=$VERSION -X $LINKER_PKG_PATH.Branch=$BRANCH -X $LINKER_PKG_PATH.Commit=$COMMIT -X $LINKER_PKG_PATH.Buildtime=`date +%Y-%m-%dT%T%z` ./cmd/rqlited/.
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
