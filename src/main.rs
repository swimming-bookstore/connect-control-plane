use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tonic::transport::{Identity, Server, ServerTlsConfig};

use connect_control_plane::plane::Plane;
use connect_control_plane::store::{replica_id, Kind, Store};

#[derive(Parser)]
#[command(
    name = "connect control plane",
    about = "Connect Control Plane. Agents connect; tenants never mix."
)]
struct Cli {
    #[arg(long, global = true, env = "CONNECT_CONFIG")]
    config: Option<PathBuf>,
    /// Postgres URL (tenants, agents, token hashes)
    #[arg(long, global = true)]
    database_url: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Listen for agents (TLS required)
    Serve {
        #[arg(long)]
        bind: Option<SocketAddr>,
        #[arg(long)]
        tls_cert: Option<PathBuf>,
        #[arg(long)]
        tls_key: Option<PathBuf>,
        /// Replica id. Share DATABASE_URL across replicas.
        #[arg(long)]
        plane_id: Option<String>,
    },
    Tenant {
        #[command(subcommand)]
        cmd: TenantCmd,
    },
    Agent {
        #[command(subcommand)]
        cmd: AgentCmd,
    },
    Login {
        #[command(subcommand)]
        cmd: LoginCmd,
    },
    Oidc {
        #[command(subcommand)]
        cmd: OidcCmd,
    },
    Acl {
        #[command(subcommand)]
        cmd: AclCmd,
    },
}

#[derive(Subcommand)]
enum TenantCmd {
    Add {
        name: String,
        /// Tenant UUID. Generated if omitted.
        #[arg(long)]
        id: Option<String>,
    },
    List,
}

#[derive(Subcommand)]
enum LoginCmd {
    /// Pending laptop logins (user codes)
    List,
    /// Approve a laptop. Issues (or rotates) a client token.
    Approve {
        user_code: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
    },
    Deny {
        user_code: String,
    },
}

#[derive(Subcommand)]
enum AclCmd {
    /// Allow client to use this box. First grant locks the client to listed boxes only.
    Grant {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        client: String,
        #[arg(long)]
        r#box: String,
    },
    Revoke {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        client: String,
        #[arg(long)]
        r#box: String,
    },
    /// Drop the allowlist. Client sees every box in the tenant again.
    Open {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        client: String,
    },
    List {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        client: String,
    },
}

#[derive(Subcommand)]
enum OidcCmd {
    /// Map an IdP subject to a client agent.
    Bind {
        #[arg(long)]
        issuer: String,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum AgentCmd {
    Add {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        /// Client token (talks to boxes). Default is a box agent.
        #[arg(long)]
        client: bool,
    },
    List {
        #[arg(long)]
        tenant: String,
    },
    Revoke {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
    },
    /// API key that the gateway exchanges for a client token (CI).
    Key {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
    },
    /// Assign a machine to a person. Empty owner = organization machine.
    Owner {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "")]
        owner: String,
    },
    /// owner | admin | member
    Role {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
        role: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "connect_control_plane=info".into()),
        )
        .compact()
        .init();

    let cli = Cli::parse();
    let cfg = connect_control_plane::cfg::Cfg::load(cli.config.as_deref())?;
    let database_url = cfg.database_url(cli.database_url)?;
    let store = Store::connect(&database_url).await?;
    match cli.cmd {
        Cmd::Serve {
            bind,
            tls_cert,
            tls_key,
            plane_id,
        } => {
            let bind = connect_control_plane::cfg::addr(
                bind,
                cfg.plane.bind.as_deref(),
                "0.0.0.0:4433",
            )?;
            let tls_cert = tls_cert
                .or(cfg.plane.tls_cert.clone())
                .context("--tls-cert or [plane].tls_cert")?;
            let tls_key = tls_key
                .or(cfg.plane.tls_key.clone())
                .context("--tls-key or [plane].tls_key")?;
            let plane_id = connect_control_plane::cfg::first(plane_id, cfg.plane.id.clone(), "PLANE_ID");
            serve(store, bind, tls_cert, tls_key, plane_id).await
        }
        Cmd::Tenant { cmd } => tenant(store, cmd).await,
        Cmd::Agent { cmd } => agent(store, cmd).await,
        Cmd::Login { cmd } => login(store, cmd).await,
        Cmd::Oidc { cmd } => oidc(store, cmd).await,
        Cmd::Acl { cmd } => acl(store, cmd).await,
    }
}

async fn serve(
    store: Store,
    bind: SocketAddr,
    cert: PathBuf,
    key: PathBuf,
    plane_id: Option<String>,
) -> Result<()> {
    let cert_pem = std::fs::read(&cert).with_context(|| format!("read {}", cert.display()))?;
    let key_pem = std::fs::read(&key).with_context(|| format!("read {}", key.display()))?;
    let identity = Identity::from_pem(cert_pem, key_pem);
    let plane_id = match plane_id {
        Some(s) => replica_id(&s),
        None => uuid::Uuid::new_v4(),
    };
    let _ = store.clear_plane(plane_id).await;
    let plane = Plane::with_id(store.clone(), plane_id);
    tracing::info!(plane = %plane_id, "listening {bind} (tls)");
    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))?
        .add_service(plane.into_service())
        .serve_with_shutdown(bind, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    let _ = store.clear_plane(plane_id).await;
    Ok(())
}

async fn tenant(store: Store, cmd: TenantCmd) -> Result<()> {
    match cmd {
        TenantCmd::Add { name, id } => {
            let t = store.create_tenant(&name, id.as_deref()).await?;
            tracing::info!(id = %t.tenant_id, tenant = %t.name, "tenant created");
        }
        TenantCmd::List => {
            for t in store.list_tenants().await? {
                tracing::info!(id = %t.tenant_id, tenant = %t.name, "tenant");
            }
        }
    }
    Ok(())
}

async fn agent(store: Store, cmd: AgentCmd) -> Result<()> {
    match cmd {
        AgentCmd::Add {
            tenant,
            name,
            client,
        } => {
            let kind = if client { Kind::Client } else { Kind::Box };
            let issued = store.create_agent(&tenant, &name, kind).await?;
            tracing::info!(
                tenant = %issued.agent.tenant,
                tenant_id = %issued.agent.tenant_id,
                agent = %issued.agent.name,
                kind = kind.as_str(),
                id = %issued.agent.agent_id,
                token = %issued.token,
                "agent created"
            );
            tracing::warn!("store the token now; it is not saved in plaintext");
        }
        AgentCmd::List { tenant } => {
            for a in store.list_agents(&tenant).await? {
                let key = if a.pubkey.is_some() {
                    "bound"
                } else {
                    "unbound"
                };
                let status = if a.revoked { "revoked" } else { "active" };
                tracing::info!(
                    agent = %a.name,
                    kind = a.kind.as_str(),
                    id = %a.agent_id,
                    status,
                    key,
                    owner = %a.owner,
                    role = %a.org_role,
                    "agent"
                );
            }
        }
        AgentCmd::Revoke { tenant, name } => {
            store.revoke_agent(&tenant, &name).await?;
            tracing::info!(
                agent = %name,
                "revoked (new sessions blocked; live stream ends on disconnect)"
            );
        }
        AgentCmd::Key { tenant, name } => {
            let key = store.mint_api_key(&tenant, &name).await?;
            tracing::info!(tenant, name, key, "api key (gateway /v1/key)");
            tracing::warn!("store the key now; it is not saved in plaintext");
        }
        AgentCmd::Owner {
            tenant,
            name,
            owner,
        } => {
            store.set_box_owner(&tenant, &name, &owner).await?;
            tracing::info!(tenant, name, owner, "box owner");
        }
        AgentCmd::Role {
            tenant,
            name,
            role,
        } => {
            store.set_org_role(&tenant, &name, &role).await?;
            tracing::info!(tenant, name, role, "org role");
        }
    }
    Ok(())
}

async fn login(store: Store, cmd: LoginCmd) -> Result<()> {
    match cmd {
        LoginCmd::List => {
            let rows = store.list_pending_logins().await?;
            if rows.is_empty() {
                tracing::info!("no pending logins");
            }
            for (code, exp) in rows {
                tracing::info!(user_code = %code, expires = %exp, "pending");
            }
        }
        LoginCmd::Approve {
            user_code,
            tenant,
            name,
        } => {
            let agent = store.approve_login(&user_code, &tenant, &name).await?;
            tracing::info!(user_code, tenant, agent, "approved");
        }
        LoginCmd::Deny { user_code } => {
            store.deny_login(&user_code).await?;
            tracing::info!(user_code, "denied");
        }
    }
    Ok(())
}

async fn oidc(store: Store, cmd: OidcCmd) -> Result<()> {
    match cmd {
        OidcCmd::Bind {
            issuer,
            subject,
            tenant,
            name,
        } => {
            store.bind_oidc(&issuer, &subject, &tenant, &name).await?;
            tracing::info!(issuer, subject, tenant, name, "oidc bound");
        }
    }
    Ok(())
}

async fn acl(store: Store, cmd: AclCmd) -> Result<()> {
    match cmd {
        AclCmd::Grant {
            tenant,
            client,
            r#box,
        } => {
            store.grant_box(&tenant, &client, &r#box).await?;
            tracing::info!(tenant, client, box_name = %r#box, "granted");
        }
        AclCmd::Revoke {
            tenant,
            client,
            r#box,
        } => {
            store.revoke_grant(&tenant, &client, &r#box).await?;
            tracing::info!(tenant, client, box_name = %r#box, "grant revoked");
        }
        AclCmd::Open { tenant, client } => {
            store.clear_grants(&tenant, &client).await?;
            tracing::info!(tenant, client, "unrestricted");
        }
        AclCmd::List { tenant, client } => {
            let (restricted, boxes) = store.list_grants(&tenant, &client).await?;
            if !restricted {
                tracing::info!(client, "unrestricted (all boxes in tenant)");
            } else if boxes.is_empty() {
                tracing::info!(client, "restricted, no boxes");
            } else {
                for b in boxes {
                    tracing::info!(client, box_name = %b, "grant");
                }
            }
        }
    }
    Ok(())
}
