use std::time::Duration;

use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const PUB_LEN: usize = 32;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    url: String,
}

#[derive(Debug, Clone)]
pub struct Tenant {
    pub tenant_id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Box,
    Client,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Box => "box",
            Kind::Client => "client",
        }
    }

    fn parse(s: &str) -> Self {
        if s == "client" {
            Kind::Client
        } else {
            Kind::Box
        }
    }
}

#[derive(Debug, Clone)]
pub struct Agent {
    pub agent_id: Uuid,
    pub tenant_id: Uuid,
    pub tenant: String,
    pub name: String,
    pub kind: Kind,
    pub pubkey: Option<[u8; PUB_LEN]>,
    pub revoked: bool,
}

pub struct Issued {
    pub agent: Agent,
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct LiveAgent {
    pub agent_id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub kind: Kind,
    pub pubkey: Vec<u8>,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let opts: PgConnectOptions = url
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid DATABASE_URL"))?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(opts)
            .await
            .map_err(|_| anyhow::anyhow!("postgres connect failed"))?;
        let s = Self {
            pool,
            url: url.to_string(),
        };
        s.migrate()
            .await
            .map_err(|_| anyhow::anyhow!("postgres migrate failed"))?;
        Ok(s)
    }

    async fn migrate(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(872634)")
            .execute(&mut *tx)
            .await?;
        let result = self.migrate_inner(&mut tx).await;
        tx.commit().await?;
        result
    }

    async fn migrate_inner(&self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
        let ty: Option<String> = sqlx::query_scalar(
            "SELECT data_type FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'tenants' AND column_name = 'tenant_id'",
        )
        .fetch_optional(&mut **tx)
        .await?;
        if matches!(ty.as_deref(), Some("text") | Some("character varying")) {
            sqlx::query("DROP TABLE IF EXISTS app_queue, sessions, agents, tenants CASCADE")
                .execute(&mut **tx)
                .await?;
        }
        let plane_ty: Option<String> = sqlx::query_scalar(
            "SELECT data_type FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'sessions' AND column_name = 'plane_id'",
        )
        .fetch_optional(&mut **tx)
        .await?;
        if matches!(plane_ty.as_deref(), Some("text") | Some("character varying")) {
            sqlx::query("DROP TABLE IF EXISTS app_queue, sessions, agents, tenants CASCADE")
                .execute(&mut **tx)
                .await?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS tenants (
                tenant_id UUID PRIMARY KEY,
                name TEXT NOT NULL
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS agents (
                agent_id UUID PRIMARY KEY,
                tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
                name TEXT NOT NULL,
                pubkey BYTEA,
                token_hash BYTEA NOT NULL UNIQUE,
                revoked BOOLEAN NOT NULL DEFAULT FALSE,
                kind TEXT NOT NULL DEFAULT 'box',
                UNIQUE (tenant_id, name)
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sessions (
                agent_id UUID PRIMARY KEY REFERENCES agents(agent_id),
                tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                pubkey BYTEA NOT NULL,
                plane_id UUID NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS app_queue (
                msg_id UUID PRIMARY KEY,
                tenant_id UUID NOT NULL,
                dst_agent_id UUID NOT NULL,
                src TEXT NOT NULL,
                dst TEXT NOT NULL,
                channel TEXT NOT NULL,
                data BYTEA NOT NULL
            )",
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub fn db_url(&self) -> &str {
        &self.url
    }

    pub async fn session_up(
        &self,
        agent_id: Uuid,
        tenant_id: Uuid,
        name: &str,
        kind: Kind,
        pubkey: &[u8],
        plane_id: Uuid,
    ) -> Result<bool> {
        let n = sqlx::query(
            "INSERT INTO sessions (agent_id, tenant_id, name, kind, pubkey, plane_id)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (agent_id) DO NOTHING",
        )
        .bind(agent_id)
        .bind(tenant_id)
        .bind(name)
        .bind(kind.as_str())
        .bind(pubkey)
        .bind(plane_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 1 {
            let _ = sqlx::query("SELECT pg_notify('connect_presence', $1)")
                .bind(format!("u\t{tenant_id}\t{agent_id}\t{plane_id}"))
                .execute(&self.pool)
                .await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn session_down(
        &self,
        agent_id: Uuid,
        tenant_id: Uuid,
        plane_id: Uuid,
        kind: Kind,
    ) -> Result<()> {
        let n = sqlx::query(
            "DELETE FROM sessions WHERE agent_id = $1 AND plane_id = $2",
        )
        .bind(agent_id)
        .bind(plane_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 1 {
            let _ = sqlx::query("SELECT pg_notify('connect_presence', $1)")
                .bind(format!(
                    "d\t{tenant_id}\t{agent_id}\t{plane_id}\t{}",
                    kind.as_str()
                ))
                .execute(&self.pool)
                .await;
        }
        Ok(())
    }

    pub async fn clear_plane(&self, plane_id: Uuid) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "DELETE FROM sessions WHERE plane_id = $1 RETURNING tenant_id, agent_id, kind",
        )
        .bind(plane_id)
        .fetch_all(&self.pool)
        .await?;
        let gone: Vec<(String, String)> = rows
            .iter()
            .map(|r| {
                let tid: Uuid = r.get("tenant_id");
                let aid: Uuid = r.get("agent_id");
                (tid.to_string(), aid.to_string())
            })
            .collect();
        for r in &rows {
            let tid: Uuid = r.get("tenant_id");
            let aid: Uuid = r.get("agent_id");
            let kind: String = r.get("kind");
            let _ = sqlx::query("SELECT pg_notify('connect_presence', $1)")
                .bind(format!("d\t{tid}\t{aid}\t{plane_id}\t{kind}"))
                .execute(&self.pool)
                .await;
        }
        Ok(gone)
    }

    pub async fn heartbeat(&self, plane_id: Uuid) -> Result<()> {
        sqlx::query("UPDATE sessions SET last_seen = now() WHERE plane_id = $1")
            .bind(plane_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn reap_stale(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "DELETE FROM sessions WHERE last_seen < now() - interval '20 seconds'
             RETURNING tenant_id, agent_id, plane_id, kind",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut gone = Vec::new();
        for r in &rows {
            let tid: Uuid = r.get("tenant_id");
            let aid: Uuid = r.get("agent_id");
            let pid: Uuid = r.get("plane_id");
            let kind: String = r.get("kind");
            let aid_s = aid.to_string();
            let _ = sqlx::query("SELECT pg_notify('connect_presence', $1)")
                .bind(format!("d\t{tid}\t{aid_s}\t{pid}\t{kind}"))
                .execute(&self.pool)
                .await;
            gone.push((tid.to_string(), aid_s));
        }
        Ok(gone)
    }

    pub async fn list_sessions(&self, tenant_id: Uuid) -> Result<Vec<LiveAgent>> {
        let rows = sqlx::query(
            "SELECT agent_id, tenant_id, name, kind, pubkey FROM sessions WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_live).collect())
    }

    pub async fn get_session(&self, agent_id: Uuid) -> Result<Option<LiveAgent>> {
        let row = sqlx::query(
            "SELECT agent_id, tenant_id, name, kind, pubkey FROM sessions WHERE agent_id = $1",
        )
        .bind(agent_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_live))
    }

    pub async fn enqueue_app(&self, tenant_id: Uuid, app: &crate::pb::App) -> Result<()> {
        let Ok(dst) = Uuid::parse_str(&app.dst) else {
            return Ok(());
        };
        let msg_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO app_queue (msg_id, tenant_id, dst_agent_id, src, dst, channel, data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(msg_id)
        .bind(tenant_id)
        .bind(dst)
        .bind(&app.src)
        .bind(&app.dst)
        .bind(&app.channel)
        .bind(&app.data)
        .execute(&self.pool)
        .await?;
        let _ = sqlx::query("SELECT pg_notify('connect_app', $1)")
            .bind(msg_id.to_string())
            .execute(&self.pool)
            .await;
        Ok(())
    }

    pub async fn take_queued(&self, plane_id: Uuid) -> Result<Vec<(Uuid, crate::pb::App)>> {
        let rows = sqlx::query(
            "DELETE FROM app_queue q
             USING sessions s
             WHERE q.dst_agent_id = s.agent_id AND s.plane_id = $1
             RETURNING q.tenant_id, q.src, q.dst, q.channel, q.data",
        )
        .bind(plane_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<Uuid, _>("tenant_id"),
                    crate::pb::App {
                        src: r.get("src"),
                        dst: r.get("dst"),
                        channel: r.get("channel"),
                        data: r.get("data"),
                    },
                )
            })
            .collect())
    }

    pub async fn create_tenant(&self, name: &str, id: Option<&str>) -> Result<Tenant> {
        let name = slug(name, "tenant")?;
        let chosen = match id {
            Some(s) => Some(
                Uuid::parse_str(s.trim())
                    .map_err(|_| anyhow::anyhow!("tenant id must be a uuid"))?,
            ),
            None => None,
        };
        for _ in 0..8 {
            let tenant_id = chosen.unwrap_or_else(Uuid::new_v4);
            let res = sqlx::query("INSERT INTO tenants (tenant_id, name) VALUES ($1, $2)")
                .bind(tenant_id)
                .bind(&name)
                .execute(&self.pool)
                .await;
            match res {
                Ok(_) => return Ok(Tenant { tenant_id, name }),
                Err(e) if is_unique(&e) && chosen.is_some() => {
                    bail!("tenant id {tenant_id} already exists")
                }
                Err(e) if is_unique(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        bail!("could not allocate tenant id")
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>> {
        let rows = sqlx::query("SELECT tenant_id, name FROM tenants ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| Tenant {
                tenant_id: r.get("tenant_id"),
                name: r.get("name"),
            })
            .collect())
    }

    pub async fn create_agent(&self, tenant: &str, name: &str, kind: Kind) -> Result<Issued> {
        let tenant = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        let agent_id = Uuid::new_v4();
        let token = b64(&rand(32)?);
        let token_hash = Sha256::digest(token.as_bytes()).to_vec();

        let res = sqlx::query(
            "INSERT INTO agents (agent_id, tenant_id, name, token_hash, kind)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(agent_id)
        .bind(&tenant.tenant_id)
        .bind(&name)
        .bind(&token_hash)
        .bind(kind.as_str())
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(Issued {
                agent: Agent {
                    agent_id,
                    tenant_id: tenant.tenant_id,
                    tenant: tenant.name,
                    name,
                    kind,
                    pubkey: None,
                    revoked: false,
                },
                token,
            }),
            Err(e) if is_unique(&e) => {
                bail!("agent {name} already exists in {}", tenant.name)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list_agents(&self, tenant: &str) -> Result<Vec<Agent>> {
        let tenant = self.get_tenant(tenant).await?;
        let rows = sqlx::query(
            "SELECT a.agent_id, a.tenant_id, t.name AS tenant, a.name, a.kind, a.pubkey, a.revoked
             FROM agents a
             JOIN tenants t ON t.tenant_id = a.tenant_id
             WHERE a.tenant_id = $1
             ORDER BY a.name",
        )
        .bind(&tenant.tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_agent).collect())
    }

    pub async fn revoke_agent(&self, tenant: &str, name: &str) -> Result<()> {
        let tenant = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        let n = sqlx::query(
            "UPDATE agents SET revoked = TRUE
             WHERE tenant_id = $1 AND name = $2 AND revoked = FALSE",
        )
        .bind(&tenant.tenant_id)
        .bind(&name)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            bail!("no active agent {name} in {}", tenant.name);
        }
        Ok(())
    }

    pub async fn auth(&self, token: &str) -> Option<Agent> {
        if token.is_empty() || token.len() > 128 {
            return None;
        }
        let hash = Sha256::digest(token.as_bytes()).to_vec();
        let row = sqlx::query(
            "SELECT a.agent_id, a.tenant_id, t.name AS tenant, a.name, a.kind, a.pubkey, a.revoked
             FROM agents a
             JOIN tenants t ON t.tenant_id = a.tenant_id
             WHERE a.token_hash = $1",
        )
        .bind(&hash)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;
        let agent = row_agent(&row);
        if agent.revoked {
            None
        } else {
            Some(agent)
        }
    }

    pub async fn bind_pub(&self, agent_id: Uuid, pk: &[u8; PUB_LEN]) -> Result<(), BindErr> {
        let n = sqlx::query(
            "UPDATE agents SET pubkey = $1 WHERE agent_id = $2 AND (pubkey IS NULL OR pubkey = $1)",
        )
        .bind(pk.as_slice())
        .bind(agent_id)
        .execute(&self.pool)
        .await
        .map_err(|_| BindErr::Busy)?
        .rows_affected();
        if n == 1 {
            Ok(())
        } else {
            Err(BindErr::Mismatch)
        }
    }

    async fn get_tenant(&self, key: &str) -> Result<Tenant> {
        let key = key.trim();
        if let Ok(id) = Uuid::parse_str(key) {
            if let Some(r) = sqlx::query("SELECT tenant_id, name FROM tenants WHERE tenant_id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?
            {
                return Ok(Tenant {
                    tenant_id: r.get("tenant_id"),
                    name: r.get("name"),
                });
            }
        }
        let name = slug(key, "tenant")?;
        let rows = sqlx::query("SELECT tenant_id, name FROM tenants WHERE name = $1")
            .bind(&name)
            .fetch_all(&self.pool)
            .await?;
        match rows.len() {
            0 => bail!("unknown tenant {name}"),
            1 => Ok(Tenant {
                tenant_id: rows[0].get("tenant_id"),
                name: rows[0].get("name"),
            }),
            _ => bail!("ambiguous tenant {name}; pass the tenant id"),
        }
    }
}

#[derive(Debug)]
pub enum BindErr {
    Mismatch,
    Busy,
}

fn row_live(r: &PgRow) -> LiveAgent {
    let raw: Vec<u8> = r.get("pubkey");
    LiveAgent {
        agent_id: r.get("agent_id"),
        tenant_id: r.get("tenant_id"),
        name: r.get("name"),
        kind: Kind::parse(r.get::<String, _>("kind").as_str()),
        pubkey: raw,
    }
}

fn row_agent(r: &PgRow) -> Agent {
    let raw: Option<Vec<u8>> = r.get("pubkey");
    Agent {
        agent_id: r.get("agent_id"),
        tenant_id: r.get("tenant_id"),
        tenant: r.get("tenant"),
        name: r.get("name"),
        kind: Kind::parse(r.get::<String, _>("kind").as_str()),
        pubkey: raw.and_then(|b| b.as_slice().try_into().ok()),
        revoked: r.get("revoked"),
    }
}

fn is_unique(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|d| d.code())
        .map(|c| c == "23505")
        .unwrap_or(false)
}

fn slug(s: &str, what: &str) -> Result<String> {
    let s = s.trim();
    let mut chars = s.chars();
    let ok = matches!(chars.next(), Some('a'..='z'))
        && s.len() <= 64
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !ok {
        bail!("{what} must match [a-z][a-z0-9-]{{0,63}}");
    }
    Ok(s.to_string())
}

/// Replica id stored on `sessions.plane_id`. UUID string, or name hashed to v5.
pub fn replica_id(s: &str) -> Uuid {
    Uuid::parse_str(s.trim()).unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_OID, s.as_bytes()))
}

fn rand(n: usize) -> Result<Vec<u8>> {
    let mut b = vec![0u8; n];
    getrandom::getrandom(&mut b).map_err(|e| anyhow::anyhow!(e))?;
    Ok(b)
}

#[cfg(test)]
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> Store {
        let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@127.0.0.1:5432/plane".into()
        });
        Store::connect(&url).await.expect("postgres")
    }

    fn uniq(prefix: &str) -> String {
        format!("{prefix}-{}", hex(&rand(4).expect("rand")))
    }

    #[tokio::test]
    async fn tenant_and_agent_roundtrip() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let issued = s.create_agent(&t, "box-1", Kind::Box).await.expect("agent");
        let got = s.auth(&issued.token).await.expect("auth");
        assert_eq!(got.agent_id, issued.agent.agent_id);
        assert_eq!(got.tenant, t);
        assert!(s.auth("nope").await.is_none());
    }

    #[tokio::test]
    async fn tenants_are_isolated_by_token() {
        let s = store().await;
        let alice = uniq("alice");
        let bob = uniq("bob");
        s.create_tenant(&alice, None).await.expect("tenant");
        s.create_tenant(&bob, None).await.expect("tenant");
        let a = s.create_agent(&alice, "laptop", Kind::Client).await.expect("agent");
        let b = s.create_agent(&bob, "laptop", Kind::Client).await.expect("agent");
        assert_eq!(s.auth(&a.token).await.expect("auth").tenant, alice);
        assert_eq!(s.auth(&b.token).await.expect("auth").tenant, bob);
        assert_ne!(a.token, b.token);
    }

    #[tokio::test]
    async fn revoke_blocks_auth() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let a = s.create_agent(&t, "box-1", Kind::Box).await.expect("agent");
        s.revoke_agent(&t, "box-1").await.expect("revoke");
        assert!(s.auth(&a.token).await.is_none());
    }

    #[tokio::test]
    async fn pubkey_binds_once() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let a = s.create_agent(&t, "box-1", Kind::Box).await.expect("agent");
        let pk1 = [1u8; 32];
        let pk2 = [2u8; 32];
        s.bind_pub(a.agent.agent_id, &pk1).await.expect("bind");
        s.bind_pub(a.agent.agent_id, &pk1).await.expect("bind");
        assert!(matches!(
            s.bind_pub(a.agent.agent_id, &pk2).await,
            Err(BindErr::Mismatch)
        ));
    }

    #[tokio::test]
    async fn connect_error_hides_url() {
        let url = "postgres://secret_user:secret_pass@127.0.0.1:1/plane";
        let shown = match Store::connect(url).await {
            Ok(_) => panic!("connect should fail"),
            Err(e) => format!("{e:?}\n{e:#}"),
        };
        assert!(!shown.contains("secret_"), "{shown}");
        assert!(!shown.contains(url), "{shown}");

        let shown = match Store::connect("not-a-postgres-url").await {
            Ok(_) => panic!("parse should fail"),
            Err(e) => format!("{e:?}\n{e:#}"),
        };
        assert!(!shown.contains("not-a-postgres-url"), "{shown}");
    }

    #[tokio::test]
    async fn bad_slug_rejected() {
        let s = store().await;
        assert!(s.create_tenant("Alice", None).await.is_err());
        assert!(s.create_tenant("", None).await.is_err());
    }

    #[tokio::test]
    async fn tenant_name_is_not_unique() {
        let s = store().await;
        let name = uniq("dup");
        let a = s.create_tenant(&name, None).await.expect("a");
        let b = s.create_tenant(&name, None).await.expect("b");
        assert_ne!(a.tenant_id, b.tenant_id);
        assert_eq!(a.name, b.name);
        let by_id = s.get_tenant(&a.tenant_id.to_string()).await.expect("id");
        assert_eq!(by_id.tenant_id, a.tenant_id);
        assert!(s.get_tenant(&name).await.is_err());
        let tid = Uuid::new_v4();
        let c = s.create_tenant(&name, Some(&tid.to_string())).await.expect("id");
        assert_eq!(c.tenant_id, tid);
        assert!(s.create_tenant(&name, Some("ACME")).await.is_err());
    }
}
