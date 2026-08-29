# Connect Control Plane

Multi-tenant control plane. Servers install an agent elsewhere; the agent dials this process. No agent ships here.

Alice and Bob can both be customers. Their agents never see each other.

Postgres via sqlx. Set `DATABASE_URL` (e.g. `postgres://user:pass@localhost/plane`).

```
connect-control-plane tenant add acme
connect-control-plane tenant add acme --id 3d8e0c2a-1b4f-4a9d-9c6e-2f7b1a0d5e44
connect-control-plane agent add --tenant acme --name box-1
connect-control-plane agent add --tenant acme --name alice --client
connect-control-plane serve --tls-cert cert.pem --tls-key key.pem
```

`:4433` is the only public listen. TLS is required.

## Setup

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
