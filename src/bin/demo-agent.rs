//! Example agent: TLS session to the control plane, presence, text App pipe.
//! Not a production agent — enough to prove the plane works.

mod pb {
    tonic::include_proto!("connect");
}

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use pb::client_msg::Msg as CMsg;
use pb::plane_client::PlaneClient;
use pb::server_msg::Msg as SMsg;
use pb::{App, ClientMsg, Hello, Peer};

#[derive(Parser)]
#[command(
    name = "demo-agent",
    about = "Example agent for Connect Control Plane: join and chat"
)]
struct Cli {
    /// host:port of the plane
    #[arg(long, default_value = "127.0.0.1:4433")]
    coord: String,
    #[arg(long)]
    token: String,
    /// 32-byte identity, created if missing
    #[arg(long, default_value = "demo.key")]
    key: PathBuf,
    /// PEM CA (use the plane's cert for a self-signed demo)
    #[arg(long)]
    tls_ca: PathBuf,
    /// SNI / cert DNS name
    #[arg(long, default_value = "localhost")]
    tls_domain: String,
    /// Connect, print Welcome, exit
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "demo_agent=info".into()),
        )
        .with_target(false)
        .without_time()
        .compact()
        .init();
    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<()> {
    let pk = load_or_create_key(&cli.key)?;
    let uri = coord_uri(&cli.coord);
    let ca =
        std::fs::read(&cli.tls_ca).with_context(|| format!("read {}", cli.tls_ca.display()))?;

    let channel = Channel::from_shared(uri.clone())
        .with_context(|| uri.clone())?
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(cli.tls_domain),
        )?
        .connect()
        .await
        .with_context(|| format!("dial {uri}"))?;

    let mut client = PlaneClient::new(channel);
    let (tx, rx) = mpsc::channel(32);
    tx.send(ClientMsg {
        msg: Some(CMsg::Hello(Hello {
            token: cli.token,
            r#pub: pk.to_vec(),
        })),
    })
    .await?;

    let mut inbound = client
        .session(ReceiverStream::new(rx))
        .await
        .context("session")?
        .into_inner();

    let mut pending = Vec::new();
    let welcome = loop {
        let msg = inbound
            .message()
            .await?
            .ok_or_else(|| anyhow!("plane closed before welcome"))?;
        match msg.msg {
            Some(SMsg::Welcome(w)) => break w,
            Some(SMsg::Delta(d)) => pending.push(d),
            _ => {}
        }
    };

    tracing::info!(
        tenant = %welcome.tenant,
        name = %welcome.name,
        id = %welcome.agent_id,
        "welcome"
    );
    let mut peers: HashMap<String, String> = HashMap::new();
    for p in &welcome.peers {
        remember(&mut peers, p);
        tracing::info!(peer = %p.name, id = %p.id, "peer");
    }
    if welcome.peers.is_empty() {
        tracing::info!("no peers yet");
    }
    for d in pending {
        for p in d.upsert {
            remember(&mut peers, &p);
            tracing::info!(peer = %p.name, id = %p.id, "online");
        }
    }
    if cli.once {
        return Ok(());
    }

    tracing::info!("chat: type a line to send to every peer, or @name text");
    let mut stdin = Some(BufReader::new(tokio::io::stdin()).lines());
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            line = async {
                match stdin.as_mut() {
                    Some(s) => s.next_line().await,
                    None => std::future::pending().await,
                }
            } => {
                match line {
                    Ok(Some(line)) => {
                        let line = line.trim().to_string();
                        if line.is_empty() {
                            continue;
                        }
                        if let Err(e) = send_line(&tx, &peers, &line).await {
                            tracing::warn!("{e:#}");
                        }
                    }
                    _ => stdin = None, // keep the session after stdin closes
                }
            }
            msg = inbound.message() => {
                let Some(msg) = msg.context("plane stream")? else {
                    tracing::info!("plane closed");
                    break;
                };
                match msg.msg {
                    Some(SMsg::Delta(d)) => {
                        for p in d.upsert {
                            remember(&mut peers, &p);
                            tracing::info!(peer = %p.name, id = %p.id, "online");
                        }
                        for id in d.remove {
                            let name = peers.remove(&id).unwrap_or_default();
                            tracing::info!(peer = %if name.is_empty() { "-" } else { &name }, id, "offline");
                        }
                    }
                    Some(SMsg::App(a)) => {
                        let from = peers.get(&a.src).map(|s| s.as_str()).unwrap_or(&a.src);
                        let text = String::from_utf8_lossy(&a.data);
                        tracing::info!(from, channel = %a.channel, %text, "recv");
                    }
                    Some(SMsg::Welcome(w)) => {
                        tracing::warn!(id = %w.agent_id, "unexpected second welcome");
                    }
                    None => {}
                }
            }
        }
    }
    Ok(())
}

async fn send_line(
    tx: &mpsc::Sender<ClientMsg>,
    peers: &HashMap<String, String>,
    line: &str,
) -> Result<()> {
    let (dsts, body) = if let Some(rest) = line.strip_prefix('@') {
        let (name, text) = rest.split_once(char::is_whitespace).unwrap_or((rest, ""));
        let text = text.trim();
        if text.is_empty() {
            bail!("usage: @name text");
        }
        let id = peers
            .iter()
            .find(|(_, n)| *n == name)
            .map(|(id, _)| id.clone())
            .or_else(|| peers.contains_key(name).then(|| name.to_string()))
            .ok_or_else(|| anyhow!("no peer {name}"))?;
        (vec![id], text.to_string())
    } else {
        (peers.keys().cloned().collect(), line.to_string())
    };
    if dsts.is_empty() {
        tracing::info!("no peers to send to");
        return Ok(());
    }
    for dst in dsts {
        tx.send(ClientMsg {
            msg: Some(CMsg::App(App {
                src: String::new(),
                dst: dst.clone(),
                data: body.as_bytes().to_vec(),
                channel: String::new(),
            })),
        })
        .await?;
        let name = peers.get(&dst).map(|s| s.as_str()).unwrap_or(&dst);
        tracing::info!(to = name, %body, "send");
    }
    Ok(())
}

fn remember(peers: &mut HashMap<String, String>, p: &Peer) {
    peers.insert(p.id.clone(), p.name.clone());
}

fn coord_uri(coord: &str) -> String {
    if coord.starts_with("https://") || coord.starts_with("http://") {
        coord.to_string()
    } else {
        format!("https://{coord}")
    }
}

fn load_or_create_key(path: &std::path::Path) -> Result<[u8; 32]> {
    if path.exists() {
        let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
        return bytes
            .try_into()
            .map_err(|_| anyhow!("{} must be 32 bytes", path.display()));
    }
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    let mut raw = [0u8; 32];
    getrandom::getrandom(&mut raw).map_err(|e| anyhow!(e))?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
        .with_context(|| format!("create {}", path.display()))?
        .write_all(&raw)?;
    Ok(raw)
}
