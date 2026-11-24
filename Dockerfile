FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY gradle gradle
COPY gradlew build.gradle.kts settings.gradle.kts gradle.properties ./
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon -q > /dev/null || true
COPY src src
RUN ./gradlew installDist --no-daemon -x test

# glibc (Ubuntu), not Alpine: the secp256k1 JNI library the bridge signs with is
# built against glibc and fails to load on musl.
FROM eclipse-temurin:21-jre
# Litestream ships the SQLite WAL to object storage continuously (see README).
ARG LITESTREAM_VERSION=0.3.13
# Set by BuildKit: amd64 on Fly, arm64 on Apple silicon.
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget \
 && rm -rf /var/lib/apt/lists/* \
 && wget -qO- "https://github.com/benbjohnson/litestream/releases/download/v${LITESTREAM_VERSION}/litestream-v${LITESTREAM_VERSION}-linux-${TARGETARCH:-amd64}.tar.gz" \
    | tar -xz -C /usr/local/bin litestream \
 && groupadd --system relay && useradd --system --gid relay --no-create-home relay
WORKDIR /app
COPY --from=build /app/build/install/yantagram-relay ./
COPY litestream.yml /etc/litestream.yml
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 8080 9091
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["bin/yantagram-relay"]
