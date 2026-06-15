# Multi-purpose runtime image. The binary is built by CI (see the publish
# workflow) and copied in here; source is never compiled inside Docker.
#
# Build context must contain the pre-built binary at ./bin/quepters.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    addgroup -S quepters && adduser -S quepters -G quepters

WORKDIR /app

COPY bin/quepters /usr/local/bin/quepters

USER quepters

# HTTP server (/metrics and /health) default port.
EXPOSE 9090

ENTRYPOINT ["quepters"]
CMD ["--config", "/etc/quepters/config.yaml"]
