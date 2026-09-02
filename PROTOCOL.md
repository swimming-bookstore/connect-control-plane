# Connect Control Plane protocol

Agents speak gRPC over TLS. One RPC. Alice's tenant never shares a map or an `App` with Bob's.

```
rpc Session(stream ClientMsg) returns (stream ServerMsg);
```

First message is `Hello`. Token is per-agent, SHA-256 looked up in Postgres. `pub` is 32 bytes, bound on first success, required forever after. The plane overwrites `App.src`. Duplicate live identity is rejected (no session steal).

Laptops do not need a pre-issued token. They call `DeviceStart` / `DevicePoll` (Grok-style user code). An operator runs `login approve USER-CODE --tenant T --name alice`, which issues a **client** token. Boxes still use `agent add` tokens.

Presence is a snapshot of online counterparts on `Welcome`, then join/leave deltas. Clients see boxes. Boxes see clients and other boxes. Clients do not see each other.

| msg | who | why |
|---|---|---|
| Hello | agent | token, pub |
| Welcome | plane | agent_id, tenant, name, online peers |
| Delta | plane | upsert / remove in this tenant |
| App | both | opaque L7 pipe, client↔box or box↔box, one **channel** per pair |
| DeviceStart | laptop | begin login, get user_code |
| DevicePoll | laptop | pending / done(+token) / denied / expired |

## Channels

`App.channel` is a circuit between two ids in the same tenant (client↔box or box↔box). Alice→box and Bob→box are different channels. Client→client is dropped. Box→box is allowed so one machine can ask another for status.

- Empty `channel` + `dst`: plane creates or reuses the pair's channel and stamps it.
- Set `channel`: delivered only to the other member. A third agent using that id is dropped.
- `src` is overwritten. `dst` is set to the other member on delivery.
- Agent disconnect drops its channels.

Box still has one gRPC session; demux by `channel`, not by name.

## Auth blast radius

A stolen token impersonates **that agent**, not the tenant and not the plane. Wrong `pub` after bind looks like a bad token. Revoke blocks new sessions.

## Scale

Replicas are **stateless** aside from open gRPC sockets. Postgres holds tenants, tokens, `sessions` (who is online, which replica), and `app_queue`. Same `DATABASE_URL` on every replica. `PLANE_ID` identifies this process. Local `App` is delivered in-process; otherwise the row is queued and the owner replica drains it. Messages capped at 256 KiB.

Do not put Firefox video on this process in production — keep `App` for control-sized payloads.

## TLS

Required. Token never hits the wire in cleartext if clients verify the cert.
