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

# Add Zscaler CA certificates to system trust store. Registering them
# in ca-certificates.conf (not just dropping into /usr/local/share/...)
# ensures they survive any later regeneration of the bundle and are
# appended to /etc/ssl/certs/ca-certificates.crt — which is what the
# 4icli embedded Node reads via NODE_EXTRA_CA_CERTS.
COPY zscaler-root-ca.crt /usr/local/share/ca-certificates/
COPY zscaler-root.crt /usr/local/share/ca-certificates/

RUN update-ca-certificates --fresh && \
    openssl crl2pkcs7 -nocrl -certfile /etc/ssl/certs/ca-certificates.crt \
        | openssl pkcs7 -print_certs -noout \
        | grep -q 'CN = Zscaler Root CA' && \
    openssl crl2pkcs7 -nocrl -certfile /etc/ssl/certs/ca-certificates.crt \
        | openssl pkcs7 -print_certs -noout \
        | grep -q 'CN = Zscaler Intermediate Root CA'

# Create care user with matching UID/GID
RUN groupadd -g 1002 care && \
    useradd -u 1002 -g 1002 -m -s /bin/sh care

# Install the real 4icli binary. No persistence shim: the runtime
# container is a read-only credential consumer, so nothing inside ever
# calls `4icli configure` or `4icli rotate`. Bootstrap (after portal
# rotation) is handled out-of-band by deploy/images/4icli/bootstrap.sh.
COPY 4icli /usr/local/bin/4icli
RUN chmod +x /usr/local/bin/4icli

# Entrypoint seeds $HOME/.config/4icli/config.txt from $BRONZE/config.txt
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
