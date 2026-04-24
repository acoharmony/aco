# © 2025 HarmonyCares
# 4icli Container Image
# Lightweight container for running 4Innovation CLI tool

FROM docker.io/ubuntu:22.04

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco \
      aco.type="cli" \
      aco.source="docker.io/ubuntu:22.04" \
      aco.build-date="${BUILD_DATE}" \
      aco.vendor="HarmonyCares" \
      aco.description="4Innovation DataHub CLI tool"

# Install minimal dependencies. `coreutils` brings sha256sum used by the
# rotate-persist wrapper; ca-certificates is for HTTPS to the 4i API.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    coreutils \
    && rm -rf /var/lib/apt/lists/*

# Add Zscaler CA certificates to system trust store
# This fixes "unable to get local issuer certificate" error
# when connecting through Zscaler SSL inspection proxy
COPY zscaler-root-ca.crt /usr/local/share/ca-certificates/
COPY zscaler-root.crt /usr/local/share/ca-certificates/

# Update CA certificates to include Zscaler certs
RUN update-ca-certificates

# Create care user with matching UID/GID
RUN groupadd -g 1002 care && \
    useradd -u 1002 -g 1002 -m -s /bin/sh care

# Real 4icli binary lives at 4icli-bin; the `4icli` name resolves to a
# shim that persists rotated/reconfigured creds back to $BRONZE/config.txt
# so they survive container restart (the entrypoint reseeds from there).
# Every caller — Python wrapper or manual `docker exec` — transparently
# hits the shim with no special routing.
COPY 4icli /usr/local/bin/4icli-bin
COPY 4icli-with-persist /usr/local/bin/4icli
RUN chmod +x /usr/local/bin/4icli-bin /usr/local/bin/4icli

# Entrypoint copies $BRONZE/config.txt into the XDG config path on every start
COPY entrypoint.sh /usr/local/bin/4icli-entrypoint
RUN chmod +x /usr/local/bin/4icli-entrypoint

# Create working directory with proper ownership
RUN mkdir -p /workspace/bronze && \
    chown -R care:care /workspace

# Set up PATH for care user
ENV PATH="/usr/local/bin:${PATH}"

# Configure Node.js (embedded in 4icli binary) to use system CA certificates
# This ensures the 4icli Node.js binary uses the updated CA bundle
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# Switch to care user
USER care

# Create home directory symlinks for 4icli
RUN mkdir -p /home/care/.4icli

WORKDIR /workspace/bronze

# config.txt is generated at container start by the entrypoint (from env vars)
# Storage backend will be mounted at runtime

ENTRYPOINT ["/usr/local/bin/4icli-entrypoint"]
CMD ["/bin/sh"]
