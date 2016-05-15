FROM iotdsa/base:ubuntu
MAINTAINER Kenneth Endfinger <k.endfinger@dglogik.com>

WORKDIR /app
VOLUME ["/data"]

ADD pubspec.* /app/
RUN pub get
ADD . /app
RUN pub get --offline

EXPOSE 8080
WORKDIR /data

CMD ["/usr/bin/dart", "/app/bin/broker.dart", "--docker"]
