# © 2026 HarmonyCares
# ACOMS CLI Container Image
# Lightweight container for running the ACOMS vendor CLI tool.

FROM docker.io/ubuntu:22.04

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco \
      aco.type="cli" \
      aco.source="docker.io/ubuntu:22.04" \
      aco.build-date="${BUILD_DATE}" \
      aco.vendor="HarmonyCares" \
      aco.description="ACOMS CLI tool"

RUN apt-get update && apt-get install -y \
    ca-certificates \
    coreutils \
    file \
    findutils \
    openssl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Reuse the same corporate CA bundle wiring as the 4icli image.
COPY 4icli/zscaler-root-ca.crt /usr/local/share/ca-certificates/
COPY 4icli/zscaler-root.crt /usr/local/share/ca-certificates/

RUN update-ca-certificates --fresh && \
    openssl crl2pkcs7 -nocrl -certfile /etc/ssl/certs/ca-certificates.crt \
        | openssl pkcs7 -print_certs -noout \
        | grep -q 'CN = Zscaler Root CA' && \
    openssl crl2pkcs7 -nocrl -certfile /etc/ssl/certs/ca-certificates.crt \
        | openssl pkcs7 -print_certs -noout \
        | grep -q 'CN = Zscaler Intermediate Root CA'

RUN groupadd -g 1002 care && \
    useradd -u 1002 -g 1002 -m -s /bin/sh care

# The vendor zip is intentionally not tracked in git. Put exactly one zip in
# deploy/images/acoms/src/ before building. The install step extracts the zip,
# finds the first file that looks like the ACOMS CLI, marks it executable, and
# exposes it as /usr/local/bin/acoms for stable docker exec usage.
COPY acoms/src/ /tmp/acoms-src/
RUN set -eux; \
    zip_count="$(find /tmp/acoms-src -maxdepth 1 -type f -iname '*.zip' | wc -l)"; \
    if [ "$zip_count" -ne 1 ]; then \
        echo "expected exactly one ACOMS CLI zip in deploy/images/acoms/src/; found $zip_count" >&2; \
        exit 1; \
    fi; \
    zip_file="$(find /tmp/acoms-src -maxdepth 1 -type f -iname '*.zip' | head -n 1)"; \
    mkdir -p /opt/acoms; \
    unzip -q "$zip_file" -d /opt/acoms; \
    candidate="$(find /opt/acoms -type f \
        \( -iname 'acoms' -o -iname 'acoms-cli' -o -iname 'acomscli' -o -iname 'acoms*' -o -iname 'acoms*.sh' \) \
        | sort | head -n 1)"; \
    if [ -z "$candidate" ]; then \
        echo "ACOMS zip did not contain an executable named like acoms/acoms-cli/acomscli" >&2; \
        echo "Archive contents:" >&2; \
        find /opt/acoms -maxdepth 4 -type f -printf '  %p\n' >&2; \
        exit 1; \
    fi; \
    chmod +x "$candidate"; \
    ln -s "$candidate" /usr/local/bin/acoms; \
    rm -rf /tmp/acoms-src

COPY acoms/entrypoint.sh /usr/local/bin/acoms-entrypoint
RUN chmod +x /usr/local/bin/acoms-entrypoint

RUN mkdir -p \
        /workspace/bronze \
        /home/care/.config/acoms \
        /home/care/.config/acoms-cli \
        /home/care/.acoms && \
    chown -R care:care /workspace /home/care/.config /home/care/.acoms /opt/acoms

ENV PATH="/usr/local/bin:${PATH}"
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt
ENV ACOMS_HOME=/home/care/.acoms

USER care
WORKDIR /workspace/bronze

ENTRYPOINT ["/usr/local/bin/acoms-entrypoint"]
CMD ["/bin/sh"]
