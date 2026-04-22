FROM alpine:latest

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco

RUN apk add --no-cache \
    openssl \
    bash

WORKDIR /ca

COPY generate-ca.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/generate-ca.sh

ENTRYPOINT ["/usr/local/bin/generate-ca.sh"]
