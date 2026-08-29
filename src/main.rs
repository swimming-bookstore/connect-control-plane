use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
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
    /// Postgres URL (tenants, agents, token hashes)
    #[arg(long, global = true, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Listen for agents (TLS required)
    Serve {
        #[arg(long, default_value = "0.0.0.0:4433")]
        bind: SocketAddr,
        #[arg(long)]
        tls_cert: PathBuf,
        #[arg(long)]
        tls_key: PathBuf,
        /// Replica id. Share DATABASE_URL across replicas.
        #[arg(long, env = "PLANE_ID")]
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
    let Some(database_url) = cli.database_url else {
        bail!("DATABASE_URL is required");
    };
    let store = Store::connect(&database_url).await?;
    match cli.cmd {
        Cmd::Serve {
            bind,
            tls_cert,
            tls_key,
            plane_id,
        } => serve(store, bind, tls_cert, tls_key, plane_id).await,
        Cmd::Tenant { cmd } => tenant(store, cmd).await,
        Cmd::Agent { cmd } => agent(store, cmd).await,
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
                tracing::info!(agent = %a.name, kind = a.kind.as_str(), id = %a.agent_id, status, key, "agent");
            }
        }
        AgentCmd::Revoke { tenant, name } => {
            store.revoke_agent(&tenant, &name).await?;
            tracing::info!(
                agent = %name,
                "revoked (new sessions blocked; live stream ends on disconnect)"
            );
        }
    }
    Ok(())
}
