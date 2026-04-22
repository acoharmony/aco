#!/bin/bash
# Generate internal CA and certificates for ACO Harmony
# NO external dependencies, fully self-signed

set -e

CA_DIR="${CA_DIR:-/ca}"
DAYS="${DAYS:-3650}"  # 10 years
DOMAIN="${DOMAIN:-acoharmony.local}"

cd "$CA_DIR"

echo "==================================================================="
echo "ACO Harmony Internal Certificate Authority"
echo "==================================================================="
echo ""

# Create CA directory structure
mkdir -p ca/{certs,private,csr} traefik/{certs,private}

# Generate CA private key
if [ ! -f ca/private/ca-key.pem ]; then
    echo "Generating CA private key..."
    openssl genrsa -out ca/private/ca-key.pem 4096
    chmod 400 ca/private/ca-key.pem
fi

# Generate CA certificate
if [ ! -f ca/certs/ca-cert.pem ]; then
    echo "Generating CA certificate..."
    openssl req -new -x509 -days "$DAYS" \
        -key ca/private/ca-key.pem \
        -out ca/certs/ca-cert.pem \
        -subj "/C=US/ST=State/L=City/O=ACO Harmony/OU=Infrastructure/CN=ACO Harmony Root CA"
fi

echo ""
echo "[SUCCESS] CA certificate created"
echo ""

# Generate Traefik certificate
if [ ! -f traefik/private/traefik-key.pem ]; then
    echo "Generating Traefik private key..."
    openssl genrsa -out traefik/private/traefik-key.pem 2048
    chmod 400 traefik/private/traefik-key.pem
fi

if [ ! -f traefik/certs/traefik-cert.pem ]; then
    echo "Generating Traefik certificate..."

    # Create config for SAN
    cat > traefik/openssl.cnf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = State
L = City
O = ACO Harmony
OU = Infrastructure
CN = ${DOMAIN}

[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${DOMAIN}
DNS.2 = *.${DOMAIN}
DNS.3 = localhost
DNS.4 = *.localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

    # Generate CSR
    openssl req -new \
        -key traefik/private/traefik-key.pem \
        -out traefik/csr/traefik.csr \
        -config traefik/openssl.cnf

    # Sign with CA
    openssl x509 -req -days "$DAYS" \
        -in traefik/csr/traefik.csr \
        -CA ca/certs/ca-cert.pem \
        -CAkey ca/private/ca-key.pem \
        -CAcreateserial \
        -out traefik/certs/traefik-cert.pem \
        -extensions v3_req \
        -extfile traefik/openssl.cnf
fi

echo ""
echo "[SUCCESS] Traefik certificate created"
echo ""

# Generate PostgreSQL certificates
mkdir -p postgres/{certs,private}

if [ ! -f postgres/private/server-key.pem ]; then
    echo "Generating PostgreSQL server key..."
    openssl genrsa -out postgres/private/server-key.pem 2048
    chmod 400 postgres/private/server-key.pem

    # Generate CSR
    openssl req -new \
        -key postgres/private/server-key.pem \
        -out postgres/server.csr \
        -subj "/C=US/ST=State/L=City/O=ACO Harmony/OU=Database/CN=postgres"

    # Sign with CA
    openssl x509 -req -days "$DAYS" \
        -in postgres/server.csr \
        -CA ca/certs/ca-cert.pem \
        -CAkey ca/private/ca-key.pem \
        -CAcreateserial \
        -out postgres/certs/server-cert.pem

    # PostgreSQL needs specific permissions
    cp ca/certs/ca-cert.pem postgres/certs/root.crt
    chmod 600 postgres/private/server-key.pem
fi

echo ""
echo "[SUCCESS] PostgreSQL certificates created"
echo ""

# Display certificate info
echo "==================================================================="
echo "Certificate Summary"
echo "==================================================================="
echo ""
echo "CA Certificate:"
openssl x509 -in ca/certs/ca-cert.pem -noout -subject -dates
echo ""
echo "Traefik Certificate:"
openssl x509 -in traefik/certs/traefik-cert.pem -noout -subject -dates
echo ""
echo "PostgreSQL Certificate:"
openssl x509 -in postgres/certs/server-cert.pem -noout -subject -dates
echo ""
echo "==================================================================="
echo "Certificates generated successfully!"
echo "==================================================================="
echo ""
echo "Certificate locations:"
echo "  CA:        /ca/ca/certs/ca-cert.pem"
echo "  Traefik:   /ca/traefik/certs/traefik-cert.pem"
echo "  PostgreSQL: /ca/postgres/certs/server-cert.pem"
echo ""
echo "Add CA to trusted roots on your system:"
echo "  Linux:   sudo cp /ca/ca/certs/ca-cert.pem /usr/local/share/ca-certificates/acoharmony.crt && sudo update-ca-certificates"
echo "  macOS:   sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain /ca/ca/certs/ca-cert.pem"
echo ""

# Keep container running in case we need to regenerate
tail -f /dev/null
