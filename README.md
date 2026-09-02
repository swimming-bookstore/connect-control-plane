# Connect Control Plane

Multi-tenant control plane. Servers install an agent elsewhere; the agent dials this process. No agent ships here.

Alice and Bob can both be customers. Their agents never see each other.

Postgres via sqlx. Shared TOML (`--config` / `CONNECT_CONFIG` / `/etc/connect/connect.toml`) or `DATABASE_URL`.

```
connect-control-plane --config /etc/connect/connect.toml serve
# or CONNECT_CONFIG=/etc/connect/connect.toml
```

Laptops log in through **[connect-gateway](https://github.com/swimming-bookstore/connect-gateway)** (device / API key / OIDC). The plane still only sees `Hello { token, pub }`. Boxes still use `--token` from `agent add`.

```
# gateway (auth only — not App)
connect-gateway --config /etc/connect/connect.toml

# laptop
connect-client login --gateway http://127.0.0.1:8787 --coord 127.0.0.1:4433 --tls-ca ca.pem
# prints ABCD-EFGH

# operator
connect-control-plane login list
connect-control-plane login approve ABCD-EFGH --tenant acme --name alice

# CI
connect-control-plane agent key --tenant acme --name ci
connect-client login --gateway http://127.0.0.1:8787 --coord 127.0.0.1:4433 --api-key ck_…

# OIDC (bind subject once per issuer, then /v1/oidc)
connect-control-plane oidc bind --issuer https://accounts.google.com --subject sub --tenant acme --name alice
connect-control-plane oidc bind --issuer https://dex.example --subject github:123 --tenant acme --name alice
# gateway: --oidc issuer=userinfo (repeat). POST { access_token, issuer? }
```

`:4433` is the plane. Gateway is a separate HTTP listen. TLS is required on the plane.

## Box ACL (within a tenant)

Default: a client sees **every box** in its tenant. After the first grant, that client is **restricted** to listed boxes. Presence and App both honor this. Gateway still only mints tokens.

```
connect-control-plane acl grant --tenant acme --client alice --box box-1
connect-control-plane acl grant --tenant acme --client alice --box box-2
connect-control-plane acl list --tenant acme --client alice
connect-control-plane acl revoke --tenant acme --client alice --box box-2
connect-control-plane acl open --tenant acme --client alice   # unrestricted again
```

Boxes still see every client that may use them (and other boxes). Clients never see clients.

```sh
./scripts/dev-certs.sh .
cargo run -- tenant add acme
cargo run -- agent add --tenant acme --name box-1
# token is logged once; it is not saved in plaintext
cargo run -- serve --tls-cert cert.pem --tls-key key.pem
```

`ca.pem` is the demo CA. Agents pass `--tls-ca ca.pem`. Production uses a real certificate.

Tenant and agent names: `[a-z][a-z0-9-]{0,63}`.

## Agent contract

TLS gRPC to the plane. First message:

```
Hello { token, pub }   # pub = 32 bytes
```

Then `Welcome`, then `Delta`. Clients see boxes; boxes see clients and other boxes. `App` is **client ↔ box** or **box ↔ box** on a **channel**. Clients never see or message each other. The plane overwrites `src` and stamps `channel`.

See `PROTOCOL.md`.

## Demo agent

Replicas are **stateless** (open sockets only) and share `DATABASE_URL`. **Alice** and **Bob** are clients (they never see each other). **box-1** dials plane A; **box-2** dials plane B. Clients talk to boxes, and boxes talk to each other, even across replicas.

```sh
# replica A
PLANE_ID=a cargo run -- serve --bind 127.0.0.1:4433 --tls-cert cert.pem --tls-key key.pem

# replica B
PLANE_ID=b cargo run -- serve --bind 127.0.0.1:4434 --tls-cert cert.pem --tls-key key.pem

# once
cargo run -- agent add --tenant acme --name box-1
cargo run -- agent add --tenant acme --name box-2
cargo run -- agent add --tenant acme --name alice --client
cargo run -- agent add --tenant acme --name bob --client

# box-1 / alice → A
cargo run --bin demo-agent -- --coord 127.0.0.1:4433 --tls-ca ca.pem --token TOKEN_BOX1 --key box-1.key
cargo run --bin demo-agent -- --coord 127.0.0.1:4433 --tls-ca ca.pem --token TOKEN_ALICE --key alice.key

# box-2 / bob → B
cargo run --bin demo-agent -- --coord 127.0.0.1:4434 --tls-ca ca.pem --token TOKEN_BOX2 --key box-2.key
cargo run --bin demo-agent -- --coord 127.0.0.1:4434 --tls-ca ca.pem --token TOKEN_BOB --key bob.key
```

CLI: type `@box-2 hello` from alice. `--once` joins, prints `Welcome`, and exits.

Web UI (two replicas + alice/bob/box-1/box-2): pick **To**, type a message, **Send**.

```sh
cargo run --features demo-web --bin demo-web
# http://127.0.0.1:3055
```

Screencast: `docs/demo.mp4` (regenerate with `./scripts/record-demo.sh`).

## What this is not

Not a data plane for video or TUN. Replicas share Postgres (`sessions` + `app_queue`); each process only holds its open streams. Revoke blocks reconnects; a live stream drops when the agent disconnects.
