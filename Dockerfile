FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY gradle gradle
COPY gradlew build.gradle.kts settings.gradle.kts gradle.properties ./
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon -q > /dev/null || true
COPY src src
RUN ./gradlew installDist --no-daemon -x test

FROM eclipse-temurin:21-jre-alpine
# Litestream ships the SQLite WAL to object storage continuously (see README).
ARG LITESTREAM_VERSION=0.3.13
RUN apk add --no-cache su-exec ca-certificates \
 && wget -qO- "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-v${LITESTREAM_VERSION}-linux-amd64.tar.gz" \
    | tar -xz -C /usr/local/bin litestream \
 && addgroup -S relay && adduser -S -G relay -H relay
WORKDIR /app
COPY --from=build /app/build/install/yantagram-relay ./
COPY litestream.yml /etc/litestream.yml
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 8080 9091
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["bin/yantagram-relay"]
