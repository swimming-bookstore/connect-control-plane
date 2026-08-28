# Database

Postgres via sqlx. Schema is created on connect. Set `DATABASE_URL`.

Tables: **tenants**, **agents**, **sessions** (who is online, which replica), **app_queue** (cross-replica `App`). Open gRPC sockets stay on the process that accepted them.

Names (`tenants.name`, `agents.name`) must match `[a-z][a-z0-9-]{0,63}`.

```
tenants 1 ──< agents
```

## tenants

Customer isolation root. An agent belongs to exactly one tenant. Peers and `App` traffic never cross this boundary.

| Column | Type | Notes |
| --- | --- | --- |
| `tenant_id` | `UUID` | Primary key; v4 unless passed to `tenant add --id` |
| `name` | `TEXT` | `NOT NULL`, label only (not unique) |

## agents

One row per issued agent. The plaintext token is shown once at create time and is **not** stored. Auth hashes the presented token (SHA-256) and looks up `token_hash`.

| Column | Type | Notes |
| --- | --- | --- |
| `agent_id` | `UUID` | Primary key; v4 |
| `tenant_id` | `UUID` | `NOT NULL`, FK → `tenants(tenant_id)` |
| `name` | `TEXT` | `NOT NULL` |
| `kind` | `TEXT` | `box` (agent) or `client`; default `box` |
| `pubkey` | `BYTEA` | 32-byte identity; `NULL` until first successful `Hello` |
| `token_hash` | `BYTEA` | SHA-256 of the token; unique |
| `revoked` | `BOOLEAN` | `NOT NULL DEFAULT FALSE` |

### Indexes

| Name / constraint | On | Notes |
| --- | --- | --- |
| `tenants_pkey` | `tenants(tenant_id)` | Primary key |
| `agents_pkey` | `agents(agent_id)` | Primary key |
| `agents_token_hash_key` | `agents(token_hash)` | Unique |
| `agents_tenant_id_name_key` | `agents(tenant_id, name)` | Unique per tenant |
| `agents_tenant_id_fkey` | `agents(tenant_id)` | References `tenants(tenant_id)` |

## sessions

| Column | Type | Notes |
| --- | --- | --- |
| `agent_id` | `UUID` | PK, FK → `agents(agent_id)` |
| `tenant_id` | `UUID` | FK → `tenants(tenant_id)` |
| `name` | `TEXT` | |
| `kind` | `TEXT` | |
| `pubkey` | `BYTEA` | |
| `plane_id` | `UUID` | replica that owns the stream (`PLANE_ID` parsed or v5) |
| `last_seen` | `TIMESTAMPTZ` | heartbeat; stale rows reaped |

## app_queue

| Column | Type | Notes |
| --- | --- | --- |
| `msg_id` | `UUID` | PK |
| `tenant_id` | `UUID` | |
| `dst_agent_id` | `UUID` | |
| `src` / `dst` / `channel` | `TEXT` | |
| `data` | `BYTEA` | |

### Auth and bind

- `auth`: lookup by `token_hash`. Missing row or `revoked` → no agent.
- `bind_pub`: first hello binds `pubkey`; later hellos must present the same 32 bytes.
- `revoke`: sets `revoked`. New sessions fail; a live stream ends when that agent disconnects.
