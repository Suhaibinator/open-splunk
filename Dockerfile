ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_PRODUCT_VERSION
ARG OPEN_SPLUNK_IMAGE_CREATED
ARG OPEN_SPLUNK_SOURCE_DATE_EPOCH
ARG SOURCE_DATE_EPOCH=${OPEN_SPLUNK_SOURCE_DATE_EPOCH}

FROM --platform=${BUILDPLATFORM} node:26.7.0-alpine3.24@sha256:aadf416b2cdce311a8811ba3f0608a61b77dbf997500e2eafe781b51f6a0b019 AS ui
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_PRODUCT_VERSION
ARG OPEN_SPLUNK_SOURCE_DATE_EPOCH
ARG SOURCE_DATE_EPOCH
WORKDIR /workspace
COPY package.json package-lock.json ./
COPY third_party ./third_party
COPY scripts/check-go-common.mjs ./scripts/check-go-common.mjs
RUN node scripts/check-go-common.mjs && npm ci --include=dev
COPY app ./app
COPY README.md AGENTS.md CLAUDE.md ./
COPY docs ./docs
COPY configs/examples ./configs/examples
COPY deploy ./deploy
COPY gen/go/README.md ./gen/go/README.md
COPY gen/ts ./gen/ts
COPY integration/README.md ./integration/README.md
COPY internal/hec/testdata/compatibility/README.md ./internal/hec/testdata/compatibility/README.md
COPY internal/spl/completion_catalog.json ./internal/spl/completion_catalog.json
COPY lib ./lib
COPY migrations/README.md ./migrations/README.md
COPY public ./public
COPY scripts/README.md scripts/build-ui.mjs scripts/build-ui-output.mjs scripts/build-help.mjs ./scripts/
COPY next-env.d.ts next.config.ts tsconfig.json ./
RUN env \
      CI=1 \
      LANG=C \
      LC_ALL=C \
      NEXT_TELEMETRY_DISABLED=1 \
      NODE_ENV=production \
      NODE_OPTIONS= \
      OPEN_SPLUNK_API_BASE_URL= \
      OPEN_SPLUNK_DATA_MODE=backend \
      OPEN_SPLUNK_PRODUCT_VERSION="${OPEN_SPLUNK_PRODUCT_VERSION}" \
      OPEN_SPLUNK_SOURCE_REVISION="${OPEN_SPLUNK_SOURCE_REVISION}" \
      SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
      TZ=UTC \
      npm run build

# Match the metadata database's major version on each target architecture.
FROM --platform=${TARGETPLATFORM} postgres:18-alpine@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873 AS postgres-tools
RUN set -eu; \
    mkdir -p /postgres-root/usr/local/bin; \
    cp /usr/local/bin/pg_dump /usr/local/bin/pg_restore /postgres-root/usr/local/bin/; \
    ldd /usr/local/bin/pg_dump /usr/local/bin/pg_restore | \
      awk '$2 == "=>" && substr($3, 1, 1) == "/" {print $3} substr($1, 1, 1) == "/" {print $1}' | \
      sort -u > /postgres-libraries; \
    while IFS= read -r library; do \
      mkdir -p "/postgres-root$(dirname "${library}")"; \
      cp -L "${library}" "/postgres-root${library}"; \
    done < /postgres-libraries; \
    pg_dump --version | grep 'PostgreSQL) 18'; \
    pg_restore --version | grep 'PostgreSQL) 18'

FROM --platform=${BUILDPLATFORM} golang:1.27.2-alpine3.23@sha256:9e45f0eb4a63ed37ad6e604950407558b7c738e34e015ae77a5d4ad369cde079 AS binaries
ARG TARGETOS
ARG TARGETARCH
ARG OPEN_SPLUNK_EXPECTED_TARGETOS
ARG OPEN_SPLUNK_EXPECTED_TARGETARCH
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_PRODUCT_VERSION
ARG OPEN_SPLUNK_IMAGE_CREATED
ARG OPEN_SPLUNK_SOURCE_DATE_EPOCH
ARG SOURCE_DATE_EPOCH
ENV CGO_ENABLED=0 \
    GO111MODULE=on \
    GOAMD64=v1 \
    GOARM64=v8.0 \
    GOCACHEPROG= \
    GODEBUG= \
    GOENV=off \
    GOEXPERIMENT= \
    GOFIPS140=off \
    GOFLAGS= \
    GOTOOLCHAIN=local \
    GOWORK=off
WORKDIR /workspace
COPY go.mod go.sum ./
COPY third_party/go-common ./third_party/go-common
RUN go mod download all && go mod verify
COPY . ./
COPY --from=postgres-tools /postgres-root /postgres-tools
COPY --from=ui /workspace/out ./out
COPY --from=ui /workspace/.next/BUILD_ID ./.next/BUILD_ID
RUN set -eu; \
    test -n "${OPEN_SPLUNK_IMAGE_CREATED}"; \
    case "${OPEN_SPLUNK_SOURCE_REVISION}" in *[!0-9a-f]*) exit 1 ;; esac; \
    revision_length="${#OPEN_SPLUNK_SOURCE_REVISION}"; \
    test "${revision_length}" -eq 40 || test "${revision_length}" -eq 64; \
    case "${OPEN_SPLUNK_SOURCE_DATE_EPOCH}" in ''|*[!0-9]*) exit 1 ;; esac; \
    case "${SOURCE_DATE_EPOCH}" in ''|*[!0-9]*) exit 1 ;; esac; \
    test "${SOURCE_DATE_EPOCH}" = "${OPEN_SPLUNK_SOURCE_DATE_EPOCH}"; \
    if [ -n "${OPEN_SPLUNK_EXPECTED_TARGETOS:-}" ]; then test "${TARGETOS}" = "${OPEN_SPLUNK_EXPECTED_TARGETOS}"; fi; \
    if [ -n "${OPEN_SPLUNK_EXPECTED_TARGETARCH:-}" ]; then test "${TARGETARCH}" = "${OPEN_SPLUNK_EXPECTED_TARGETARCH}"; fi; \
    test "${TARGETOS}" = linux; \
    test "${TARGETARCH}" = amd64 || test "${TARGETARCH}" = arm64
RUN go run ./cmd/open-splunk-manifest \
      -source-revision "${OPEN_SPLUNK_SOURCE_REVISION}" \
      -product-version "${OPEN_SPLUNK_PRODUCT_VERSION}"
RUN set -eu; \
    mkdir -p /artifacts; \
    build_information_package=github.com/Suhaibinator/open-splunk/internal/buildinfo; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-X ${build_information_package}.sourceRevision=${OPEN_SPLUNK_SOURCE_REVISION} -X ${build_information_package}.productVersion=${OPEN_SPLUNK_PRODUCT_VERSION}" \
      -o /artifacts/open-splunk-server \
      ./cmd/open-splunk-server; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-X ${build_information_package}.sourceRevision=${OPEN_SPLUNK_SOURCE_REVISION} -X ${build_information_package}.productVersion=${OPEN_SPLUNK_PRODUCT_VERSION}" \
      -o /artifacts/open-splunk-collector \
      ./cmd/open-splunk-collector; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-X ${build_information_package}.sourceRevision=${OPEN_SPLUNK_SOURCE_REVISION} -X ${build_information_package}.productVersion=${OPEN_SPLUNK_PRODUCT_VERSION}" \
      -o /artifacts/open-splunk-loggen \
      ./cmd/open-splunk-loggen; \
    chmod 0555 /artifacts/open-splunk-server /artifacts/open-splunk-collector /artifacts/open-splunk-loggen
RUN set -eu; \
    if [ "${TARGETOS}" = "$(go env GOHOSTOS)" ] && \
       [ "${TARGETARCH}" = "$(go env GOHOSTARCH)" ]; then \
      expected_identity="$(printf 'source_revision=%s' "${OPEN_SPLUNK_SOURCE_REVISION}"; if [ -n "${OPEN_SPLUNK_PRODUCT_VERSION}" ]; then printf '\nproduct_version=%s' "${OPEN_SPLUNK_PRODUCT_VERSION}"; fi)"; \
      server_identity="$(/artifacts/open-splunk-server -verify-embedded-release | sed -n '/^source_revision=/p; /^product_version=/p')"; \
      collector_identity="$(/artifacts/open-splunk-collector version)"; \
      loggen_identity="$(/artifacts/open-splunk-loggen -version)"; \
      test "${server_identity}" = "${expected_identity}"; \
      test "${collector_identity}" = "${expected_identity}"; \
      test "${loggen_identity}" = "${expected_identity}"; \
    fi
# Materialize every scratch-root entry before COPY so BuildKit never synthesizes
# parent directories with wall-clock timestamps in the published layers.
RUN set -eu; \
    install -d -o 0 -g 0 -m 0555 \
      /image-rootfs/server/etc \
      /image-rootfs/server/etc/ssl/certs \
      /image-rootfs/server/usr \
      /image-rootfs/server/usr/local \
      /image-rootfs/server/usr/local/bin \
      /image-rootfs/collector/etc \
      /image-rootfs/collector/etc/ssl/certs \
      /image-rootfs/collector/usr \
      /image-rootfs/collector/usr/local \
      /image-rootfs/collector/usr/local/bin; \
    install -d -o 0 -g 0 -m 0755 \
      /image-rootfs/server/var \
      /image-rootfs/server/var/lib \
      /image-rootfs/server/var/lib/open-splunk \
      /image-rootfs/server/var/lib/open-splunk/state \
      /image-rootfs/server/var/lib/open-splunk/exports \
      /image-rootfs/server/var/lib/open-splunk/recovery \
      /image-rootfs/server/var/lib/open-splunk/lock \
      /image-rootfs/collector/var \
      /image-rootfs/collector/var/lib; \
    install -d -o 65532 -g 65532 -m 0700 \
      /image-rootfs/server/var/lib/open-splunk/state/private \
      /image-rootfs/server/var/lib/open-splunk/exports/private \
      /image-rootfs/server/var/lib/open-splunk/recovery/private \
      /image-rootfs/server/var/lib/open-splunk/lock/private \
      /image-rootfs/collector/var/lib/open-splunk-collector; \
    install -o 0 -g 0 -m 0444 \
      /etc/ssl/certs/ca-certificates.crt \
      /image-rootfs/server/etc/ssl/certs/; \
    install -o 0 -g 0 -m 0444 \
      /etc/ssl/certs/ca-certificates.crt \
      /image-rootfs/collector/etc/ssl/certs/; \
    install -o 0 -g 0 -m 0444 \
      oci/rootfs/etc/passwd \
      oci/rootfs/etc/group \
      /image-rootfs/server/etc/; \
    install -o 0 -g 0 -m 0444 \
      oci/rootfs/etc/passwd \
      oci/rootfs/etc/group \
      /image-rootfs/collector/etc/; \
    install -o 0 -g 0 -m 0555 \
      /artifacts/open-splunk-server \
      /image-rootfs/server/usr/local/bin/open-splunk-server; \
    install -o 0 -g 0 -m 0555 \
      /artifacts/open-splunk-collector \
      /image-rootfs/collector/usr/local/bin/open-splunk-collector; \
    cp -a /postgres-tools/. /image-rootfs/server/; \
    if [ "${TARGETARCH}" = "$(go env GOHOSTARCH)" ]; then \
      chroot /image-rootfs/server /usr/local/bin/pg_dump --version | grep 'PostgreSQL) 18'; \
      chroot /image-rootfs/server /usr/local/bin/pg_restore --version | grep 'PostgreSQL) 18'; \
    fi; \
    find /image-rootfs -exec touch -h -d "@${SOURCE_DATE_EPOCH}" {} +

FROM scratch AS artifacts
COPY --from=binaries /artifacts/ /

FROM scratch AS server
ENV PATH=/usr/local/bin
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_PRODUCT_VERSION
ARG OPEN_SPLUNK_IMAGE_CREATED
LABEL org.opencontainers.image.title="Open Splunk Server"
LABEL org.opencontainers.image.description="Open Splunk API, search, and ingestion server"
LABEL org.opencontainers.image.source="https://github.com/Suhaibinator/open-splunk"
LABEL org.opencontainers.image.revision="${OPEN_SPLUNK_SOURCE_REVISION}"
LABEL org.opencontainers.image.version="${OPEN_SPLUNK_PRODUCT_VERSION}"
LABEL org.opencontainers.image.created="${OPEN_SPLUNK_IMAGE_CREATED}"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=binaries /image-rootfs/server/ /
USER 65532:65532
WORKDIR /var/lib/open-splunk/state/private
EXPOSE 8080 4317
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=12 \
  CMD ["/usr/local/bin/open-splunk-server", "healthcheck", "-url", "http://127.0.0.1:8080/readyz"]
ENTRYPOINT ["/usr/local/bin/open-splunk-server"]

FROM scratch AS collector
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_PRODUCT_VERSION
ARG OPEN_SPLUNK_IMAGE_CREATED
LABEL org.opencontainers.image.title="Open Splunk Collector"
LABEL org.opencontainers.image.description="Durable Open Splunk file collector"
LABEL org.opencontainers.image.source="https://github.com/Suhaibinator/open-splunk"
LABEL org.opencontainers.image.revision="${OPEN_SPLUNK_SOURCE_REVISION}"
LABEL org.opencontainers.image.version="${OPEN_SPLUNK_PRODUCT_VERSION}"
LABEL org.opencontainers.image.created="${OPEN_SPLUNK_IMAGE_CREATED}"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=binaries /image-rootfs/collector/ /
USER 65532:65532
WORKDIR /var/lib/open-splunk-collector
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/open-splunk-collector"]
CMD ["run", "-config", "/etc/open-splunk/collector.yaml"]
