# 
ARG DOCKER_TAG
ENV APP_VERSION=$DOCKER_TAG

FROM alpine:latest
MAINTAINER Philip O'Toole <philip.otoole@yahoo.com>

RUN apk update &&     apk --no-cache add curl tar &&     curl -L https://github.com/rqlite/rqlite/releases/download/$APP_VERSION/rqlite-$APP_VERSION-linux-amd64-musl.tar.gz -o rqlite-$APP_VERSION-linux-amd64-musl.tar.gz &&     tar xvfz rqlite-$APP_VERSION-linux-amd64-musl.tar.gz &&     cp rqlite-$APP_VERSION-linux-amd64-musl/rqlited /bin &&     cp rqlite-$APP_VERSION-linux-amd64-musl/rqlite /bin &&     rm -fr rqlite-$APP_VERSION-linux-amd64-musl rqlite-$APP_VERSION-linux-amd64-musl.tar.gz &&     apk del curl tar

RUN mkdir -p /rqlite/file

VOLUME /rqlite/file

EXPOSE 4001 4002

COPY docker-entrypoint.sh /bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["rqlite"]

