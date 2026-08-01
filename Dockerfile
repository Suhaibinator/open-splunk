ARG OPEN_SPLUNK_APPLICATION_VERSION
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_IMAGE_CREATED
ARG OPEN_SPLUNK_SOURCE_DATE_EPOCH
ARG SOURCE_DATE_EPOCH=${OPEN_SPLUNK_SOURCE_DATE_EPOCH}

FROM --platform=${BUILDPLATFORM} node:24.18.0-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d AS ui
ARG OPEN_SPLUNK_APPLICATION_VERSION
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_SOURCE_DATE_EPOCH
ARG SOURCE_DATE_EPOCH
WORKDIR /workspace
COPY package.json package-lock.json ./
RUN npm ci --include=dev
COPY app ./app
COPY gen/ts ./gen/ts
COPY internal/spl/completion_catalog.json ./internal/spl/completion_catalog.json
COPY lib ./lib
COPY public ./public
COPY scripts/build-ui.mjs scripts/build-ui-output.mjs ./scripts/
COPY next-env.d.ts next.config.ts tsconfig.json ./
RUN env \
      CI=1 \
      LANG=C \
      LC_ALL=C \
      NEXT_TELEMETRY_DISABLED=1 \
      NODE_ENV=production \
      NODE_OPTIONS= \
      OPEN_SPLUNK_API_BASE_URL= \
      OPEN_SPLUNK_APPLICATION_VERSION="${OPEN_SPLUNK_APPLICATION_VERSION}" \
      OPEN_SPLUNK_DATA_MODE=backend \
      OPEN_SPLUNK_SOURCE_REVISION="${OPEN_SPLUNK_SOURCE_REVISION}" \
      SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
      TZ=UTC \
      npm run build

FROM --platform=${BUILDPLATFORM} golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651 AS binaries
ARG TARGETOS
ARG TARGETARCH
ARG OPEN_SPLUNK_EXPECTED_TARGETOS
ARG OPEN_SPLUNK_EXPECTED_TARGETARCH
ARG OPEN_SPLUNK_APPLICATION_VERSION
ARG OPEN_SPLUNK_SOURCE_REVISION
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
RUN go mod download all && go mod verify
COPY . ./
COPY --from=ui /workspace/out ./out
COPY --from=ui /workspace/.next/BUILD_ID ./.next/BUILD_ID
RUN set -eu; \
    test -n "${OPEN_SPLUNK_APPLICATION_VERSION}"; \
    test -n "${OPEN_SPLUNK_IMAGE_CREATED}"; \
    case "${OPEN_SPLUNK_SOURCE_REVISION}" in *[!0-9a-f]*) exit 1 ;; esac; \
    revision_length="${#OPEN_SPLUNK_SOURCE_REVISION}"; \
    test "${revision_length}" -eq 40 || test "${revision_length}" -eq 64; \
    case "${OPEN_SPLUNK_SOURCE_DATE_EPOCH}" in ''|*[!0-9]*) exit 1 ;; esac; \
    case "${SOURCE_DATE_EPOCH}" in ''|*[!0-9]*) exit 1 ;; esac; \
    test "${SOURCE_DATE_EPOCH}" = "${OPEN_SPLUNK_SOURCE_DATE_EPOCH}"; \
    test "${TARGETOS}" = "${OPEN_SPLUNK_EXPECTED_TARGETOS}"; \
    test "${TARGETARCH}" = "${OPEN_SPLUNK_EXPECTED_TARGETARCH}"; \
    test "${TARGETOS}" = linux; \
    test "${TARGETARCH}" = amd64 || test "${TARGETARCH}" = arm64
RUN go run ./cmd/open-splunk-manifest \
      -application-version "${OPEN_SPLUNK_APPLICATION_VERSION}" \
      -source-revision "${OPEN_SPLUNK_SOURCE_REVISION}"
RUN set -eu; \
    mkdir -p /artifacts; \
    build_information_package=github.com/Suhaibinator/open-splunk/internal/buildinfo; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-X ${build_information_package}.applicationVersion=${OPEN_SPLUNK_APPLICATION_VERSION} -X ${build_information_package}.sourceRevision=${OPEN_SPLUNK_SOURCE_REVISION}" \
      -o /artifacts/open-splunk-server \
      ./cmd/open-splunk-server; \
    GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "-X ${build_information_package}.applicationVersion=${OPEN_SPLUNK_APPLICATION_VERSION} -X ${build_information_package}.sourceRevision=${OPEN_SPLUNK_SOURCE_REVISION}" \
      -o /artifacts/open-splunk-collector \
      ./cmd/open-splunk-collector; \
    chmod 0555 /artifacts/open-splunk-server /artifacts/open-splunk-collector
RUN set -eu; \
    if [ "${TARGETOS}" = "$(go env GOHOSTOS)" ] && \
       [ "${TARGETARCH}" = "$(go env GOHOSTARCH)" ]; then \
      expected="$(printf 'application_version=%s\nsource_revision=%s' \
        "${OPEN_SPLUNK_APPLICATION_VERSION}" \
        "${OPEN_SPLUNK_SOURCE_REVISION}")"; \
      server_identity="$(/artifacts/open-splunk-server -verify-embedded-release | sed -n '1,2p')"; \
      collector_identity="$(/artifacts/open-splunk-collector version)"; \
      test "${server_identity}" = "${expected}"; \
      test "${collector_identity}" = "${expected}"; \
    fi
# Materialize every scratch-root entry before COPY so BuildKit never synthesizes
# parent directories with wall-clock timestamps in the published layers.
RUN set -eu; \
    install -d -o 0 -g 0 -m 0555 \
      /image-rootfs/server/etc \
      /image-rootfs/server/usr \
      /image-rootfs/server/usr/local \
      /image-rootfs/server/usr/local/bin \
      /image-rootfs/collector/etc \
      /image-rootfs/collector/usr \
      /image-rootfs/collector/usr/local \
      /image-rootfs/collector/usr/local/bin; \
    install -d -o 0 -g 0 -m 0755 \
      /image-rootfs/server/var \
      /image-rootfs/server/var/lib \
      /image-rootfs/server/var/lib/open-splunk \
      /image-rootfs/server/var/lib/open-splunk/state \
      /image-rootfs/server/var/lib/open-splunk/exports \
      /image-rootfs/collector/var \
      /image-rootfs/collector/var/lib; \
    install -d -o 65532 -g 65532 -m 0700 \
      /image-rootfs/server/var/lib/open-splunk/state/private \
      /image-rootfs/server/var/lib/open-splunk/exports/private \
      /image-rootfs/collector/var/lib/open-splunk-collector; \
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
    find /image-rootfs -exec touch -h -d "@${SOURCE_DATE_EPOCH}" {} +

FROM scratch AS server
ARG OPEN_SPLUNK_APPLICATION_VERSION
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_IMAGE_CREATED
LABEL org.opencontainers.image.title="Open Splunk Server"
LABEL org.opencontainers.image.description="Open Splunk API, search, and ingestion server"
LABEL org.opencontainers.image.source="https://github.com/Suhaibinator/open-splunk"
LABEL org.opencontainers.image.version="${OPEN_SPLUNK_APPLICATION_VERSION}"
LABEL org.opencontainers.image.revision="${OPEN_SPLUNK_SOURCE_REVISION}"
LABEL org.opencontainers.image.created="${OPEN_SPLUNK_IMAGE_CREATED}"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=binaries /image-rootfs/server/ /
USER 65532:65532
WORKDIR /var/lib/open-splunk/state/private
EXPOSE 8080 4317
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=12 \
  CMD ["/usr/local/bin/open-splunk-server", "healthcheck", "-url", "https://127.0.0.1:8080/readyz", "-ca-cert", "/run/open-splunk/tls/ca.crt", "-server-name", "open-splunk-server"]
ENTRYPOINT ["/usr/local/bin/open-splunk-server"]

FROM scratch AS collector
ARG OPEN_SPLUNK_APPLICATION_VERSION
ARG OPEN_SPLUNK_SOURCE_REVISION
ARG OPEN_SPLUNK_IMAGE_CREATED
LABEL org.opencontainers.image.title="Open Splunk Collector"
LABEL org.opencontainers.image.description="Durable Open Splunk file collector"
LABEL org.opencontainers.image.source="https://github.com/Suhaibinator/open-splunk"
LABEL org.opencontainers.image.version="${OPEN_SPLUNK_APPLICATION_VERSION}"
LABEL org.opencontainers.image.revision="${OPEN_SPLUNK_SOURCE_REVISION}"
LABEL org.opencontainers.image.created="${OPEN_SPLUNK_IMAGE_CREATED}"
LABEL org.opencontainers.image.licenses="MIT"
COPY --from=binaries /image-rootfs/collector/ /
USER 65532:65532
WORKDIR /var/lib/open-splunk-collector
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/open-splunk-collector"]
CMD ["run", "-config", "/etc/open-splunk/collector.yaml"]
