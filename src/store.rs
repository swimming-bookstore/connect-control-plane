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
    pub personal: bool,
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
    /// Client who owns this box. Empty = organization machine (admins).
    pub owner: String,
    /// Client org role: owner | admin | member (empty = member).
    pub org_role: String,
}

pub struct Issued {
    pub agent: Agent,
    pub token: String,
}

pub struct ConsolePlane {
    pub email: String,
    pub spaces: Vec<Issued>,
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
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS device_codes (
                device_code TEXT PRIMARY KEY,
                user_code TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                expires_at TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                token TEXT
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS api_keys (
                key_hash BYTEA PRIMARY KEY,
                tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
                name TEXT NOT NULL,
                revoked BOOLEAN NOT NULL DEFAULT FALSE
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS oidc_subjects (
                issuer TEXT NOT NULL,
                subject TEXT NOT NULL,
                tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
                name TEXT NOT NULL,
                PRIMARY KEY (issuer, subject)
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS restricted BOOLEAN NOT NULL DEFAULT FALSE",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS box_grants (
                tenant_id UUID NOT NULL REFERENCES tenants(tenant_id),
                client_id UUID NOT NULL REFERENCES agents(agent_id),
                box_id UUID NOT NULL REFERENCES agents(agent_id),
                PRIMARY KEY (client_id, box_id)
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS console_operators (
                email TEXT PRIMARY KEY,
                tenant TEXT NOT NULL DEFAULT '',
                name TEXT NOT NULL DEFAULT '',
                salt BYTEA NOT NULL,
                pass_hash BYTEA NOT NULL
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query("ALTER TABLE console_operators ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE console_operators ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS console_sessions (
                token_hash BYTEA PRIMARY KEY,
                email TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )",
        )
        .execute(&mut **tx)
        .await?;
        sqlx::query("ALTER TABLE agents ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE agents ADD COLUMN IF NOT EXISTS org_role TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE console_operators ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS personal BOOLEAN NOT NULL DEFAULT FALSE")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS open_access BOOLEAN NOT NULL DEFAULT TRUE")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE console_operators ADD COLUMN IF NOT EXISTS home_id UUID")
            .execute(&mut **tx)
            .await?;
        sqlx::query("ALTER TABLE console_operators ADD COLUMN IF NOT EXISTS display_name TEXT NOT NULL DEFAULT ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("UPDATE console_operators SET display_name = name WHERE display_name = '' AND name <> ''")
            .execute(&mut **tx)
            .await?;
        sqlx::query("UPDATE tenants SET personal = TRUE WHERE name LIKE 'me-%' AND personal = FALSE")
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "UPDATE console_operators o SET home_id = t.tenant_id
             FROM tenants t WHERE o.home_id IS NULL AND t.name = o.tenant AND t.personal = TRUE",
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
                Ok(_) => {
                    return Ok(Tenant {
                        tenant_id,
                        name,
                        personal: false,
                    })
                }
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
        let rows = sqlx::query("SELECT tenant_id, name, COALESCE(personal, FALSE) AS personal FROM tenants ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(row_tenant).collect())
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
                    owner: String::new(),
                    org_role: String::new(),
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
            "SELECT a.agent_id, a.tenant_id, t.name AS tenant, a.name, a.kind, a.pubkey, a.revoked,
                    a.owner, a.org_role
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

    pub async fn device_start(&self) -> Result<(String, String, u64, u64)> {
        let expires_in = 900u64;
        for _ in 0..8 {
            let device_code = b64(&rand(32)?);
            let user_code = user_code()?;
            let res = sqlx::query(
                "INSERT INTO device_codes (device_code, user_code, expires_at)
                 VALUES ($1, $2, now() + interval '15 minutes')",
            )
            .bind(&device_code)
            .bind(&user_code)
            .execute(&self.pool)
            .await;
            match res {
                Ok(_) => return Ok((device_code, user_code, 5, expires_in)),
                Err(e) if is_unique(&e) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        bail!("could not allocate login code")
    }

    pub async fn device_poll(&self, device_code: &str) -> Result<DevicePoll> {
        if device_code.is_empty() || device_code.len() > 128 {
            return Ok(DevicePoll::Expired);
        }
        let row = sqlx::query(
            "SELECT status, token, expires_at < now() AS expired
             FROM device_codes WHERE device_code = $1",
        )
        .bind(device_code)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            return Ok(DevicePoll::Expired);
        };
        if r.get::<bool, _>("expired") {
            let _ = sqlx::query("DELETE FROM device_codes WHERE device_code = $1")
                .bind(device_code)
                .execute(&self.pool)
                .await;
            return Ok(DevicePoll::Expired);
        }
        match r.get::<String, _>("status").as_str() {
            "pending" => Ok(DevicePoll::Pending),
            "denied" => {
                let _ = sqlx::query("DELETE FROM device_codes WHERE device_code = $1")
                    .bind(device_code)
                    .execute(&self.pool)
                    .await;
                Ok(DevicePoll::Denied)
            }
            "approved" => {
                let token: Option<String> = r.get("token");
                let _ = sqlx::query("DELETE FROM device_codes WHERE device_code = $1")
                    .bind(device_code)
                    .execute(&self.pool)
                    .await;
                match token.filter(|s| !s.is_empty()) {
                    Some(token) => Ok(DevicePoll::Done { token }),
                    None => Ok(DevicePoll::Expired),
                }
            }
            _ => Ok(DevicePoll::Expired),
        }
    }

    pub async fn list_pending_logins(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT user_code,
                    to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS exp
             FROM device_codes
             WHERE status = 'pending' AND expires_at > now()
             ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("user_code"), r.get("exp")))
            .collect())
    }

    pub async fn approve_login(&self, user_code: &str, tenant: &str, name: &str) -> Result<String> {
        let code = normalize_user_code(user_code)?;
        let n = sqlx::query(
            "SELECT 1 FROM device_codes
             WHERE user_code = $1 AND status = 'pending' AND expires_at > now()",
        )
        .bind(&code)
        .fetch_optional(&self.pool)
        .await?;
        if n.is_none() {
            bail!("no pending login {code}");
        }
        let issued = self.issue_client(tenant, name).await?;
        let n = sqlx::query(
            "UPDATE device_codes SET status = 'approved', token = $1
             WHERE user_code = $2 AND status = 'pending' AND expires_at > now()",
        )
        .bind(&issued.token)
        .bind(&code)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            bail!("no pending login {code}");
        }
        Ok(issued.agent.name)
    }

    pub async fn deny_login(&self, user_code: &str) -> Result<()> {
        let code = normalize_user_code(user_code)?;
        let n = sqlx::query(
            "UPDATE device_codes SET status = 'denied'
             WHERE user_code = $1 AND status = 'pending' AND expires_at > now()",
        )
        .bind(&code)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            bail!("no pending login {code}");
        }
        Ok(())
    }

    pub async fn issue_client(&self, tenant: &str, name: &str) -> Result<Issued> {
        match self.create_agent(tenant, name, Kind::Client).await {
            Ok(i) => Ok(i),
            Err(e) if e.to_string().contains("already exists") => {
                self.rotate_client_token(tenant, name).await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn mint_api_key(&self, tenant: &str, name: &str) -> Result<String> {
        let t = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        let key = format!("ck_{}", b64(&rand(24)?));
        let hash = Sha256::digest(key.as_bytes()).to_vec();
        sqlx::query(
            "INSERT INTO api_keys (key_hash, tenant_id, name) VALUES ($1, $2, $3)
             ON CONFLICT (key_hash) DO NOTHING",
        )
        .bind(&hash)
        .bind(t.tenant_id)
        .bind(&name)
        .execute(&self.pool)
        .await?;
        Ok(key)
    }

    pub async fn exchange_api_key(&self, key: &str) -> Result<Issued> {
        if key.is_empty() || key.len() > 128 {
            bail!("bad api key");
        }
        let hash = Sha256::digest(key.as_bytes()).to_vec();
        let row = sqlx::query(
            "SELECT t.name AS tenant, k.name, k.revoked
             FROM api_keys k
             JOIN tenants t ON t.tenant_id = k.tenant_id
             WHERE k.key_hash = $1",
        )
        .bind(&hash)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            bail!("bad api key");
        };
        if r.get::<bool, _>("revoked") {
            bail!("bad api key");
        }
        let tenant: String = r.get("tenant");
        let name: String = r.get("name");
        self.issue_client(&tenant, &name).await
    }

    pub async fn bind_oidc(&self, issuer: &str, subject: &str, tenant: &str, name: &str) -> Result<()> {
        let issuer = issuer.trim();
        let subject = subject.trim();
        if issuer.is_empty() || subject.is_empty() {
            bail!("issuer and subject required");
        }
        let t = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        sqlx::query(
            "INSERT INTO oidc_subjects (issuer, subject, tenant_id, name)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (issuer, subject) DO UPDATE SET tenant_id = $3, name = $4",
        )
        .bind(issuer)
        .bind(subject)
        .bind(t.tenant_id)
        .bind(&name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn issue_oidc(&self, issuer: &str, subject: &str) -> Result<Issued> {
        let row = sqlx::query(
            "SELECT t.name AS tenant, o.name
             FROM oidc_subjects o
             JOIN tenants t ON t.tenant_id = o.tenant_id
             WHERE o.issuer = $1 AND o.subject = $2",
        )
        .bind(issuer.trim())
        .bind(subject.trim())
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            bail!("unknown oidc subject");
        };
        let tenant: String = r.get("tenant");
        let name: String = r.get("name");
        self.issue_client(&tenant, &name).await
    }

    pub async fn approve_device_issued(&self, device_code: &str, issued: &Issued) -> Result<()> {
        let n = sqlx::query(
            "UPDATE device_codes SET status = 'approved', token = $1
             WHERE device_code = $2 AND status = 'pending' AND expires_at > now()",
        )
        .bind(&issued.token)
        .bind(device_code)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            bail!("no pending login");
        }
        Ok(())
    }

    async fn rotate_client_token(&self, tenant: &str, name: &str) -> Result<Issued> {
        let tenant = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        let token = b64(&rand(32)?);
        let token_hash = Sha256::digest(token.as_bytes()).to_vec();
        let row = sqlx::query(
            "UPDATE agents SET token_hash = $1, revoked = FALSE, pubkey = NULL
             WHERE tenant_id = $2 AND name = $3 AND kind = 'client'
             RETURNING agent_id, pubkey, revoked, owner, org_role",
        )
        .bind(&token_hash)
        .bind(&tenant.tenant_id)
        .bind(&name)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            bail!("no client {name} in {}", tenant.name);
        };
        let raw: Option<Vec<u8>> = r.get("pubkey");
        Ok(Issued {
            agent: Agent {
                agent_id: r.get("agent_id"),
                tenant_id: tenant.tenant_id,
                tenant: tenant.name,
                name,
                kind: Kind::Client,
                pubkey: raw.and_then(|b| b.as_slice().try_into().ok()),
                revoked: r.get("revoked"),
                owner: r.try_get("owner").unwrap_or_default(),
                org_role: r.try_get("org_role").unwrap_or_default(),
            },
            token,
        })
    }

    pub async fn auth(&self, token: &str) -> Option<Agent> {
        if token.is_empty() || token.len() > 128 {
            return None;
        }
        let hash = Sha256::digest(token.as_bytes()).to_vec();
        let row = sqlx::query(
            "SELECT a.agent_id, a.tenant_id, t.name AS tenant, a.name, a.kind, a.pubkey, a.revoked,
                    a.owner, a.org_role
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

    pub async fn grant_box(&self, tenant: &str, client: &str, box_name: &str) -> Result<()> {
        let c = self.agent_in(tenant, client).await?;
        let b = self.agent_in(tenant, box_name).await?;
        if c.kind != Kind::Client {
            bail!("{client} is not a client");
        }
        if b.kind != Kind::Box {
            bail!("{box_name} is not a box");
        }
        sqlx::query("UPDATE agents SET restricted = TRUE WHERE agent_id = $1")
            .bind(c.agent_id)
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "INSERT INTO box_grants (tenant_id, client_id, box_id) VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(c.tenant_id)
        .bind(c.agent_id)
        .bind(b.agent_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn revoke_grant(&self, tenant: &str, client: &str, box_name: &str) -> Result<()> {
        let c = self.agent_in(tenant, client).await?;
        let b = self.agent_in(tenant, box_name).await?;
        let n = sqlx::query("DELETE FROM box_grants WHERE client_id = $1 AND box_id = $2")
            .bind(c.agent_id)
            .bind(b.agent_id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if n == 0 {
            bail!("no grant {client} → {box_name}");
        }
        Ok(())
    }

    pub async fn clear_grants(&self, tenant: &str, client: &str) -> Result<()> {
        let c = self.agent_in(tenant, client).await?;
        sqlx::query("DELETE FROM box_grants WHERE client_id = $1")
            .bind(c.agent_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("UPDATE agents SET restricted = FALSE WHERE agent_id = $1")
            .bind(c.agent_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_grants(&self, tenant: &str, client: &str) -> Result<(bool, Vec<String>)> {
        let c = self.agent_in(tenant, client).await?;
        let restricted: bool = sqlx::query_scalar("SELECT restricted FROM agents WHERE agent_id = $1")
            .bind(c.agent_id)
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT a.name FROM box_grants g JOIN agents a ON a.agent_id = g.box_id
             WHERE g.client_id = $1 ORDER BY a.name",
        )
        .bind(c.agent_id)
        .fetch_all(&self.pool)
        .await?;
        Ok((restricted, rows.into_iter().map(|r| r.get("name")).collect()))
    }

    pub async fn list_box_clients(&self, tenant: &str, box_name: &str) -> Result<(bool, Vec<String>)> {
        let b = self.agent_in(tenant, box_name).await?;
        if b.kind != Kind::Box {
            bail!("{box_name} is not a box");
        }
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agents WHERE tenant_id = $1 AND kind = 'client' AND revoked = FALSE AND restricted = TRUE",
        )
        .bind(b.tenant_id)
        .fetch_one(&self.pool)
        .await?;
        if n == 0 {
            let open = self.org_open_access(tenant).await.unwrap_or(true);
            return Ok((!open, Vec::new()));
        }
        let rows = sqlx::query(
            "SELECT a.name FROM box_grants g JOIN agents a ON a.agent_id = g.client_id
             WHERE g.box_id = $1 AND a.revoked = FALSE ORDER BY a.name",
        )
        .bind(b.agent_id)
        .fetch_all(&self.pool)
        .await?;
        Ok((true, rows.into_iter().map(|r| r.get("name")).collect()))
    }

    pub async fn list_online(&self, tenant: &str) -> Result<Vec<LiveAgent>> {
        let t = self.get_tenant(tenant).await?;
        self.list_sessions(t.tenant_id).await
    }

    pub async fn list_oidc(&self, tenant: &str) -> Result<Vec<(String, String, String)>> {
        let t = self.get_tenant(tenant).await?;
        let rows = sqlx::query(
            "SELECT issuer, subject, name FROM oidc_subjects WHERE tenant_id = $1 ORDER BY name, issuer",
        )
        .bind(t.tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("issuer"), r.get("subject"), r.get("name")))
            .collect())
    }

    pub async fn list_api_key_names(&self, tenant: &str) -> Result<Vec<(String, bool)>> {
        let t = self.get_tenant(tenant).await?;
        let rows = sqlx::query(
            "SELECT name, revoked FROM api_keys WHERE tenant_id = $1 ORDER BY name",
        )
        .bind(t.tenant_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get("name"), r.get("revoked")))
            .collect())
    }

    /// Unrestricted client may use every box if the org is open.
    /// Locked org (`open_access = false`): nobody until assigned.
    /// After `acl grant`, only listed boxes.
    pub async fn client_may_box(&self, client_id: Uuid, box_id: Uuid) -> bool {
        let row = sqlx::query(
            "SELECT a.restricted, a.tenant_id, COALESCE(t.open_access, TRUE) AS open_access
             FROM agents a JOIN tenants t ON t.tenant_id = a.tenant_id
             WHERE a.agent_id = $1",
        )
        .bind(client_id)
        .fetch_optional(&self.pool)
        .await;
        let Ok(Some(r)) = row else {
            return false;
        };
        let restricted: bool = r.try_get("restricted").unwrap_or(false);
        if restricted {
            return sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM box_grants WHERE client_id = $1 AND box_id = $2)",
            )
            .bind(client_id)
            .bind(box_id)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(false);
        }
        r.try_get("open_access").unwrap_or(true)
    }

    pub async fn org_open_access(&self, tenant: &str) -> Result<bool> {
        let t = self.get_tenant(tenant).await?;
        let v: bool = sqlx::query_scalar(
            "SELECT COALESCE(open_access, TRUE) FROM tenants WHERE tenant_id = $1",
        )
        .bind(t.tenant_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(v)
    }

    pub async fn set_org_open_access(&self, tenant: &str, open: bool) -> Result<()> {
        let t = self.get_tenant(tenant).await?;
        sqlx::query("UPDATE tenants SET open_access = $1 WHERE tenant_id = $2")
            .bind(open)
            .bind(t.tenant_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn agent_in(&self, tenant: &str, name: &str) -> Result<Agent> {
        let tenant = self.get_tenant(tenant).await?;
        let name = slug(name, "agent")?;
        let row = sqlx::query(
            "SELECT a.agent_id, a.tenant_id, t.name AS tenant, a.name, a.kind, a.pubkey, a.revoked,
                    a.owner, a.org_role
             FROM agents a JOIN tenants t ON t.tenant_id = a.tenant_id
             WHERE a.tenant_id = $1 AND a.name = $2",
        )
        .bind(tenant.tenant_id)
        .bind(&name)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref()
            .map(row_agent)
            .ok_or_else(|| anyhow::anyhow!("no agent {name} in {}", tenant.name))
    }

    pub async fn console_needs_setup(&self) -> Result<bool> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM console_operators")
            .fetch_one(&self.pool)
            .await?;
        Ok(n == 0)
    }

    pub async fn console_register(
        &self,
        tenant: &str,
        name: &str,
        email: &str,
        password: &str,
    ) -> Result<String> {
        let email = norm_email(email)?;
        let name = slug(name, "agent")?;
        let (salt, hash) = hash_pass(password)?;
        let home_id = self.create_personal_tenant(&name).await?;
        let company = tenant.trim();
        let (store_key, role) = if company.is_empty() {
            (home_id.to_string(), "owner")
        } else {
            let company = slug(company, "tenant")?;
            if company == "personal" {
                bail!("personal is reserved");
            }
            if self.get_tenant(&company).await.is_err() {
                self.create_tenant(&company, None).await?;
            }
            if self.agent_in(&company, &name).await.is_err() {
                self.create_agent(&company, &name, Kind::Client).await?;
            }
            let n: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM console_operators WHERE tenant = $1")
                    .bind(&company)
                    .fetch_one(&self.pool)
                    .await?;
            (company, if n == 0 { "owner" } else { "member" })
        };
        let tenant_col = if company.is_empty() {
            String::new()
        } else {
            store_key.clone()
        };
        sqlx::query(
            "INSERT INTO console_operators (email, tenant, name, salt, pass_hash, role, home_id, display_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&email)
        .bind(&tenant_col)
        .bind(&name)
        .bind(&salt)
        .bind(&hash)
        .bind(role)
        .bind(home_id)
        .bind(&name)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if is_unique(&e) {
                anyhow::anyhow!("that email is already used")
            } else {
                e.into()
            }
        })?;
        if company.is_empty() {
            sqlx::query("UPDATE agents SET org_role = 'owner' WHERE tenant_id = $1 AND name = $2")
                .bind(home_id)
                .bind(&name)
                .execute(&self.pool)
                .await?;
        } else {
            sqlx::query(
                "UPDATE agents SET org_role = $1 WHERE tenant_id = (SELECT tenant_id FROM tenants WHERE name = $2) AND name = $3 AND kind = 'client'",
            )
            .bind(role)
            .bind(&store_key)
            .bind(&name)
            .execute(&self.pool)
            .await?;
        }
        self.console_login(&email, password).await
    }

    async fn create_personal_tenant(&self, name: &str) -> Result<Uuid> {
        let tenant_id = Uuid::new_v4();
        let hidden = format!("_{}", tenant_id.simple());
        sqlx::query("INSERT INTO tenants (tenant_id, name, personal) VALUES ($1, $2, TRUE)")
            .bind(tenant_id)
            .bind(&hidden)
            .execute(&self.pool)
            .await?;
        let _ = self
            .create_agent(&tenant_id.to_string(), name, Kind::Client)
            .await?;
        sqlx::query("UPDATE agents SET org_role = 'owner' WHERE tenant_id = $1 AND name = $2")
            .bind(tenant_id)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(tenant_id)
    }

    pub async fn console_create_org(&self, email: &str, org: &str) -> Result<String> {
        let email = norm_email(email)?;
        let org = slug(org, "tenant")?;
        if org == "personal" {
            bail!("personal is reserved");
        }
        if self.get_tenant(&org).await.is_ok() {
            bail!("organization {org} already exists");
        }
        let row = sqlx::query("SELECT name, tenant, home_id FROM console_operators WHERE email = $1")
            .bind(&email)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("not signed in"))?;
        let name: String = row.get("name");
        self.create_tenant(&org, None).await?;
        if self.agent_in(&org, &name).await.is_err() {
            self.create_agent(&org, &name, Kind::Client).await?;
        }
        sqlx::query(
            "UPDATE console_operators SET tenant = $1, role = 'owner' WHERE email = $2",
        )
        .bind(&org)
        .bind(&email)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "UPDATE agents SET org_role = 'owner' WHERE tenant_id = (SELECT tenant_id FROM tenants WHERE name = $1) AND name = $2 AND kind = 'client'",
        )
        .bind(&org)
        .bind(&name)
        .execute(&self.pool)
        .await?;
        Ok(org)
    }

    pub async fn console_setup(&self, email: &str, password: &str) -> Result<()> {
        let _ = (email, password);
        bail!("use register");
    }

    pub async fn console_login(&self, email: &str, password: &str) -> Result<String> {
        let email = norm_email(email)?;
        let row = sqlx::query("SELECT salt, pass_hash FROM console_operators WHERE email = $1")
            .bind(&email)
            .fetch_optional(&self.pool)
            .await?;
        let Some(r) = row else {
            bail!("bad email or password");
        };
        let salt: Vec<u8> = r.get("salt");
        let expect: Vec<u8> = r.get("pass_hash");
        if hash_pass_with(&salt, password) != expect {
            bail!("bad email or password");
        }
        let raw = b64(&rand(32)?);
        let th = Sha256::digest(raw.as_bytes()).to_vec();
        sqlx::query(
            "INSERT INTO console_sessions (token_hash, email, expires_at)
             VALUES ($1, $2, now() + interval '14 days')",
        )
        .bind(&th)
        .bind(&email)
        .execute(&self.pool)
        .await?;
        Ok(raw)
    }

    /// Console email/password → plane client tokens for every space this person has.
    pub async fn console_plane_login(&self, email: &str, password: &str) -> Result<ConsolePlane> {
        let email = norm_email(email)?;
        let row = sqlx::query(
            "SELECT salt, pass_hash, tenant, name, home_id FROM console_operators WHERE email = $1",
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await?;
        let Some(r) = row else {
            bail!("bad email or password");
        };
        let salt: Vec<u8> = r.get("salt");
        let expect: Vec<u8> = r.get("pass_hash");
        if hash_pass_with(&salt, password) != expect {
            bail!("bad email or password");
        }
        let company: String = r.get("tenant");
        let name: String = r.get("name");
        let home: Option<Uuid> = r.try_get("home_id").ok().flatten();
        let mut spaces = Vec::new();
        if !company.is_empty() {
            spaces.push(self.issue_client(&company, &name).await?);
        }
        if let Some(home) = home {
            let already = spaces.iter().any(|i| i.agent.tenant_id == home);
            if !already {
                spaces.push(self.issue_client(&home.to_string(), &name).await?);
            }
        }
        if spaces.is_empty() {
            bail!("no plane identity");
        }
        Ok(ConsolePlane { email, spaces })
    }

    pub async fn console_who(
        &self,
        token: &str,
    ) -> Option<(String, String, String, String, bool, String)> {
        let email = self.console_session(token).await?;
        let row = sqlx::query(
            "SELECT o.tenant, o.name, o.role, o.home_id, o.display_name
             FROM console_operators o WHERE o.email = $1",
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;
        let tenant: String = row.get("tenant");
        let name: String = row.get("name");
        let mut role: String = row.try_get("role").unwrap_or_default();
        let home: Option<Uuid> = row.try_get("home_id").ok().flatten();
        let personal = tenant.is_empty()
            || tenant.starts_with("me-")
            || tenant.starts_with('_')
            || home.is_some() && tenant.is_empty();
        let personal = personal || home.map(|id| tenant == id.to_string()).unwrap_or(false);
        if role.is_empty() {
            role = "owner".into();
        }
        let org = if personal {
            "personal".into()
        } else {
            if tenant.is_empty() {
                return None;
            }
            tenant
        };
        let mut display: String = row.try_get("display_name").unwrap_or_default();
        if display.is_empty() {
            display = name.clone();
        }
        let _ = home;
        Some((email, org, name, role, personal, display))
    }

    pub async fn console_set_profile(
        &self,
        email: &str,
        display: &str,
        password: Option<&str>,
    ) -> Result<()> {
        let email = norm_email(email)?;
        let display = display.trim();
        if display.is_empty() || display.len() > 80 {
            bail!("name looks wrong");
        }
        let n = sqlx::query("UPDATE console_operators SET display_name = $1 WHERE email = $2")
            .bind(display)
            .bind(&email)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if n == 0 {
            bail!("not signed in");
        }
        if let Some(password) = password.filter(|p| !p.is_empty()) {
            let (salt, hash) = hash_pass(password)?;
            sqlx::query("UPDATE console_operators SET salt = $1, pass_hash = $2 WHERE email = $3")
                .bind(&salt)
                .bind(&hash)
                .bind(&email)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// Tenant key for store calls. `personal` → home_id UUID string.
    pub async fn console_scope(&self, token: &str, org: &str) -> Option<String> {
        let email = self.console_session(token).await?;
        let row = sqlx::query("SELECT tenant, name, home_id FROM console_operators WHERE email = $1")
            .bind(&email)
            .fetch_optional(&self.pool)
            .await
            .ok()
            .flatten()?;
        let tenant: String = row.get("tenant");
        let name: String = row.get("name");
        let home: Option<Uuid> = row.try_get("home_id").ok().flatten();
        if org == "personal" {
            if let Some(id) = home {
                return Some(id.to_string());
            }
            if tenant.starts_with("me-") || tenant.starts_with('_') {
                return Some(tenant);
            }
            return self.ensure_home(&email, &name).await.ok().map(|id| id.to_string());
        }
        if tenant == org {
            Some(tenant)
        } else {
            None
        }
    }

    async fn ensure_home(&self, email: &str, name: &str) -> Result<Uuid> {
        if let Some(id) = sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT home_id FROM console_operators WHERE email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await?
        .flatten()
        {
            return Ok(id);
        }
        let id = self.create_personal_tenant(name).await?;
        sqlx::query("UPDATE console_operators SET home_id = $1 WHERE email = $2")
            .bind(id)
            .bind(email)
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    pub async fn set_org_role(&self, tenant: &str, name: &str, role: &str) -> Result<()> {
        let role = match role {
            "owner" | "admin" | "member" => role,
            _ => bail!("role must be owner, admin, or member"),
        };
        let a = self.agent_in(tenant, name).await?;
        if a.kind != Kind::Client {
            bail!("{name} is not a person");
        }
        if a.org_role == "owner" && role != "owner" {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agents WHERE tenant_id = $1 AND kind = 'client' AND org_role = 'owner' AND revoked = FALSE",
            )
            .bind(a.tenant_id)
            .fetch_one(&self.pool)
            .await?;
            if n <= 1 {
                bail!("cannot demote the last owner");
            }
        }
        sqlx::query("UPDATE agents SET org_role = $1 WHERE agent_id = $2")
            .bind(role)
            .bind(a.agent_id)
            .execute(&self.pool)
            .await?;
        sqlx::query("UPDATE console_operators SET role = $1 WHERE tenant = $2 AND name = $3")
            .bind(role)
            .bind(tenant)
            .bind(&a.name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_box_owner(&self, tenant: &str, box_name: &str, owner: &str) -> Result<()> {
        let b = self.agent_in(tenant, box_name).await?;
        if b.kind != Kind::Box {
            bail!("{box_name} is not a machine");
        }
        let owner = owner.trim();
        if owner.is_empty() {
            sqlx::query("UPDATE agents SET owner = '' WHERE agent_id = $1")
                .bind(b.agent_id)
                .execute(&self.pool)
                .await?;
            return Ok(());
        }
        let o = self.agent_in(tenant, owner).await?;
        if o.kind != Kind::Client {
            bail!("{owner} is not a person");
        }
        sqlx::query("UPDATE agents SET owner = $1 WHERE agent_id = $2")
            .bind(&o.name)
            .bind(b.agent_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn create_box(&self, tenant: &str, name: &str, owner: &str) -> Result<Issued> {
        let i = self.create_agent(tenant, name, Kind::Box).await?;
        if !owner.trim().is_empty() {
            self.set_box_owner(tenant, &i.agent.name, owner).await?;
        }
        Ok(i)
    }

    pub async fn console_logout(&self, token: &str) -> Result<()> {
        let th = Sha256::digest(token.as_bytes()).to_vec();
        sqlx::query("DELETE FROM console_sessions WHERE token_hash = $1")
            .bind(&th)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn console_session(&self, token: &str) -> Option<String> {
        if token.is_empty() {
            return None;
        }
        let th = Sha256::digest(token.as_bytes()).to_vec();
        let row = sqlx::query(
            "SELECT email FROM console_sessions WHERE token_hash = $1 AND expires_at > now()",
        )
        .bind(&th)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()?;
        Some(row.get("email"))
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
        if key == "personal" {
            bail!("pass the personal tenant id");
        }
        if let Ok(id) = Uuid::parse_str(key) {
            if let Some(r) = sqlx::query(
                "SELECT tenant_id, name, COALESCE(personal, FALSE) AS personal FROM tenants WHERE tenant_id = $1",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            {
                return Ok(row_tenant(r));
            }
        }
        if key.starts_with("me-") || key.starts_with('_') {
            if let Some(r) = sqlx::query(
                "SELECT tenant_id, name, COALESCE(personal, FALSE) AS personal FROM tenants WHERE name = $1",
            )
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
            {
                return Ok(row_tenant(r));
            }
        }
        let name = slug(key, "tenant")?;
        let mut rows = sqlx::query(
            "SELECT tenant_id, name, COALESCE(personal, FALSE) AS personal FROM tenants WHERE name = $1 AND COALESCE(personal, FALSE) = FALSE",
        )
        .bind(&name)
        .fetch_all(&self.pool)
        .await?;
        match rows.len() {
            0 => bail!("unknown tenant {name}"),
            1 => Ok(row_tenant(rows.remove(0))),
            _ => bail!("ambiguous tenant {name}; pass the tenant id"),
        }
    }
}

fn row_tenant(r: PgRow) -> Tenant {
    Tenant {
        tenant_id: r.get("tenant_id"),
        name: r.get("name"),
        personal: r.try_get("personal").unwrap_or(false),
    }
}

#[derive(Debug)]
pub enum BindErr {
    Mismatch,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DevicePoll {
    Pending,
    Denied,
    Expired,
    Done { token: String },
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
        owner: r.try_get("owner").unwrap_or_default(),
        org_role: r.try_get("org_role").unwrap_or_default(),
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

fn norm_email(s: &str) -> Result<String> {
    let s = s.trim().to_ascii_lowercase();
    if s.len() < 3 || s.len() > 120 || !s.contains('@') {
        bail!("email looks wrong");
    }
    Ok(s)
}

fn hash_pass(password: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    if password.len() < 8 {
        bail!("password must be at least 8 characters");
    }
    let salt = rand(16)?;
    Ok((salt.clone(), hash_pass_with(&salt, password)))
}

fn hash_pass_with(salt: &[u8], password: &str) -> Vec<u8> {
    let mut d = Sha256::new();
    d.update(salt);
    d.update(password.as_bytes());
    d.finalize().to_vec()
}

fn user_code() -> Result<String> {
    const ALPH: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let mut raw = [0u8; 8];
    getrandom::getrandom(&mut raw).map_err(|e| anyhow::anyhow!(e))?;
    let mut s = String::with_capacity(9);
    for (i, b) in raw.iter().enumerate() {
        if i == 4 {
            s.push('-');
        }
        s.push(ALPH[(*b as usize) % ALPH.len()] as char);
    }
    Ok(s)
}

fn normalize_user_code(s: &str) -> Result<String> {
    let t: String = s
        .chars()
        .filter(|c| *c != '-' && !c.is_whitespace())
        .flat_map(|c| c.to_uppercase())
        .collect();
    if t.len() != 8 || !t.chars().all(|c| c.is_ascii_alphanumeric()) {
        bail!("user code must look like ABCD-EFGH");
    }
    Ok(format!("{}-{}", &t[..4], &t[4..]))
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

    #[test]
    fn user_code_shape() {
        let c = user_code().expect("code");
        assert_eq!(c.len(), 9);
        assert_eq!(c.chars().nth(4), Some('-'));
        assert_eq!(normalize_user_code("abcd efgh").expect("n"), "ABCD-EFGH");
        assert_eq!(normalize_user_code("ABCD-EFGH").expect("n"), "ABCD-EFGH");
        assert!(normalize_user_code("short").is_err());
    }

    #[tokio::test]
    async fn device_login_approve_and_poll() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let (dc, uc, _, _) = s.device_start().await.expect("start");
        assert!(matches!(s.device_poll(&dc).await.expect("p"), DevicePoll::Pending));
        s.approve_login(&uc, &t, "alice").await.expect("approve");
        match s.device_poll(&dc).await.expect("done") {
            DevicePoll::Done { token } => {
                let a = s.auth(&token).await.expect("auth");
                assert_eq!(a.name, "alice");
                assert_eq!(a.kind, Kind::Client);
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(s.device_poll(&dc).await.expect("gone"), DevicePoll::Expired));
    }

    #[tokio::test]
    async fn device_login_deny() {
        let s = store().await;
        let (dc, uc, _, _) = s.device_start().await.expect("start");
        s.deny_login(&uc).await.expect("deny");
        assert!(matches!(s.device_poll(&dc).await.expect("d"), DevicePoll::Denied));
    }

    #[tokio::test]
    async fn api_key_exchanges_for_client() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let key = s.mint_api_key(&t, "ci").await.expect("key");
        let a = s.exchange_api_key(&key).await.expect("ex");
        assert_eq!(a.agent.name, "ci");
        assert_eq!(a.agent.kind, Kind::Client);
        assert!(s.auth(&a.token).await.is_some());
        assert!(s.exchange_api_key("ck_nope").await.is_err());
    }

    #[tokio::test]
    async fn oidc_bind_and_issue() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        s.bind_oidc("https://iss", "sub-1", &t, "alice").await.expect("bind");
        let a = s.issue_oidc("https://iss", "sub-1").await.expect("iss");
        assert_eq!(a.agent.name, "alice");
        assert!(s.issue_oidc("https://iss", "nope").await.is_err());
    }

    #[tokio::test]
    async fn grant_restricts_client() {
        let s = store().await;
        let t = uniq("acme");
        s.create_tenant(&t, None).await.expect("tenant");
        let alice = s.create_agent(&t, "alice", Kind::Client).await.expect("c");
        let box1 = s.create_agent(&t, "box-1", Kind::Box).await.expect("b1");
        let box2 = s.create_agent(&t, "box-2", Kind::Box).await.expect("b2");
        assert!(s.client_may_box(alice.agent.agent_id, box1.agent.agent_id).await);
        assert!(s.client_may_box(alice.agent.agent_id, box2.agent.agent_id).await);
        s.grant_box(&t, "alice", "box-1").await.expect("grant");
        assert!(s.client_may_box(alice.agent.agent_id, box1.agent.agent_id).await);
        assert!(!s.client_may_box(alice.agent.agent_id, box2.agent.agent_id).await);
        s.grant_box(&t, "alice", "box-2").await.expect("g2");
        assert!(s.client_may_box(alice.agent.agent_id, box2.agent.agent_id).await);
        s.revoke_grant(&t, "alice", "box-2").await.expect("rv");
        assert!(!s.client_may_box(alice.agent.agent_id, box2.agent.agent_id).await);
        s.clear_grants(&t, "alice").await.expect("open");
        assert!(s.client_may_box(alice.agent.agent_id, box2.agent.agent_id).await);
    }
}
