# ACO Harmony Auth Operations

This deployment has two vendor CLIs with public-IP-bound credentials:

- `4icli` for 4Innovation DataHub
- `acoms` for ACO-MS DataHub

The goal is to keep long-running containers boring: they read persisted,
verified config from `/opt/s3/data/workspace/auth`, and setup commands handle
rotation out-of-band.

## Daily Check

```bash
aco auth doctor
```

The doctor compares:

- host public IPs observed from multiple providers
- container public IPs observed from multiple providers
- locally recorded portal-registered public IPs
- persisted config path, mode, owner, mtime, and fingerprint
- live vendor auth smoke test

Under Zscaler, different public-IP providers may report different egress IPs
from the same host/container. For example, one provider may show the direct ISP
egress while another shows the Zscaler proxy egress. `aco auth doctor` reports
all observed IPs and treats the registered IP as matching if it appears in that
observed set.

Diagnoses include `OK`, `IP_MISMATCH`, `BAD_SECRET`, `MISSING_CONFIG`,
`TLS_OR_ZSCALER`, and `CONTAINER_DOWN`.

## Record Portal IPs

When the vendor portal says a token is registered to a public IP, record that
non-secret metadata locally:

```bash
aco auth register-ip 4icli --ip <portal-registered-public-ip>
aco auth register-ip acoms --ip <portal-registered-public-ip>
```

If you omit `--ip`, the command records the current host IP only when there is
a single clear result. When Zscaler/direct routing exposes more than one public
IP, pass the portal value explicitly so the local registry mirrors the vendor
record instead of guessing.

The registry lives at:

```bash
/opt/s3/data/workspace/auth/registry.yaml
```

It stores entity IDs, registered IPs, labels, timestamps, and non-secret
fingerprints. It does not store API secrets.

## Rotate Credentials

For 4Innovation:

```bash
aco 4icli setup
docker compose -f deploy/docker-compose.yml restart 4icli
aco auth doctor --service 4icli
```

For ACO-MS:

```bash
aco acoms setup
docker compose -f deploy/docker-compose.yml restart acoms
aco auth doctor --service acoms
```

Both setup commands verify credentials in a throwaway container before replacing
the persisted runtime config.

## Harden Local Files

```bash
aco auth harden
```

This creates the auth directory layout and tightens permissions for `deploy/.env`,
persisted configs, manifests, certs, and the registry.

## Zscaler Certificate Updates

Place the current Zscaler root/intermediate bundle here:

```bash
/opt/s3/data/workspace/auth/certs/zscaler-bundle.pem
```

When present, the containers append it to the system CA bundle at startup and
use the combined bundle via `NODE_EXTRA_CA_CERTS`, `SSL_CERT_FILE`, and
`REQUESTS_CA_BUNDLE`.
