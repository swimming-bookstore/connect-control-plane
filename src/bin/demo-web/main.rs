//! Leptos UI for the two-replica demo: planes, boxes, alice, bob.

mod pb {
    tonic::include_proto!("connect");
}
mod ui;

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use connect_control_plane::store::{Kind, Store};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::process::Command;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use pb::client_msg::Msg as CMsg;
use pb::plane_client::PlaneClient;
use pb::server_msg::Msg as SMsg;
use pb::{App, ClientMsg, Hello, Peer};

const ROOT: &str = env!("CARGO_MANIFEST_DIR");

#[derive(Parser)]
#[command(name = "demo-web", about = "Leptos demo UI for Connect Control Plane")]
struct Cli {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, default_value = "127.0.0.1:3055")]
    bind: SocketAddr,
    /// Do not DROP/CREATE the plane database
    #[arg(long)]
    keep_db: bool,
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct Ev {
    pane: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Deserialize)]
struct InCmd {
    pane: String,
    #[serde(default)]
    line: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    text: String,
}

#[derive(Clone)]
struct Hub {
    buf: Arc<Mutex<HashMap<String, Vec<Ev>>>>,
    tx: broadcast::Sender<Ev>,
    cmds: Arc<HashMap<String, mpsc::Sender<String>>>,
    ready: Arc<AtomicBool>,
    playing: Arc<AtomicBool>,
    online: Arc<AtomicUsize>,
}

impl Hub {
    fn new(cmds: HashMap<String, mpsc::Sender<String>>) -> Self {
        let (tx, _) = broadcast::channel(512);
        Self {
            buf: Arc::new(Mutex::new(HashMap::new())),
            tx,
            cmds: Arc::new(cmds),
            ready: Arc::new(AtomicBool::new(false)),
            playing: Arc::new(AtomicBool::new(false)),
            online: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn emit(&self, ev: Ev) {
        {
            let mut g = self.buf.lock().await;
            g.entry(ev.pane.clone()).or_default().push(ev.clone());
        }
        let _ = self.tx.send(ev);
    }
}

fn peer_ev(pane: &str, name: &str, on: bool) -> Ev {
    Ev {
        pane: pane.to_string(),
        kind: if on { "peer".into() } else { "gone".into() },
        name: Some(name.to_string()),
        ..Ev::default()
    }
}

fn parse_plane_line(pane: &str, line: &str) -> Option<Ev> {
    let rest = line.split("agent=").nth(1)?;
    let name = rest.split_whitespace().next()?.trim_matches('"');
    if name.is_empty() {
        return None;
    }
    let kind = if line.contains(" offline ") {
        "left"
    } else if line.contains(" online ") {
        "joined"
    } else {
        return None;
    };
    Some(Ev {
        pane: pane.to_string(),
        kind: kind.into(),
        name: Some(name.to_string()),
        ..Ev::default()
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "demo_web=info,connect_control_plane=info".into()),
        )
        .compact()
        .init();

    let cli = Cli::parse();
    let Some(database_url) = cli.database_url else {
        bail!("DATABASE_URL is required");
    };
    if !cli.keep_db {
        reset_db(&database_url)?;
    }

    let demo = PathBuf::from(ROOT).join("target/demo-web");
    std::fs::create_dir_all(&demo)?;
    let status = std::process::Command::new(PathBuf::from(ROOT).join("scripts/dev-certs.sh"))
        .arg(&demo)
        .status()
        .context("dev-certs")?;
    if !status.success() {
        bail!("dev-certs failed");
    }

    let store = Store::connect(&database_url).await?;
    let _ = store.create_tenant("acme", None).await;
    let box1 = store.create_agent("acme", "box-1", Kind::Box).await?;
    let box2 = store.create_agent("acme", "box-2", Kind::Box).await?;
    let alice = store.create_agent("acme", "alice", Kind::Client).await?;
    let bob = store.create_agent("acme", "bob", Kind::Client).await?;

    let plane_bin = sibling_bin("connect-control-plane")?;
    let mut child_a = spawn_plane(
        &plane_bin,
        &database_url,
        "127.0.0.1:14433",
        "a",
        demo.join("cert.pem"),
        demo.join("key.pem"),
    )?;
    let mut child_b = spawn_plane(
        &plane_bin,
        &database_url,
        "127.0.0.1:14434",
        "b",
        demo.join("cert.pem"),
        demo.join("key.pem"),
    )?;

    let mut cmds = HashMap::new();
    let mut rx_map = HashMap::new();
    for name in ["box-1", "box-2", "alice", "bob"] {
        let (tx, rx) = mpsc::channel::<String>(32);
        cmds.insert(name.to_string(), tx);
        rx_map.insert(name.to_string(), rx);
    }
    let hub = Hub::new(cmds);

    pipe_child("plane-a", child_a.stderr.take(), hub.clone());
    pipe_child("plane-a", child_a.stdout.take(), hub.clone());
    pipe_child("plane-b", child_b.stderr.take(), hub.clone());
    pipe_child("plane-b", child_b.stdout.take(), hub.clone());

    wait_tcp("127.0.0.1:14433").await?;
    wait_tcp("127.0.0.1:14434").await?;

    let ca = demo.join("ca.pem");
    spawn_agent(
        hub.clone(),
        "box-1",
        "127.0.0.1:14433",
        box1.token,
        demo.join("box-1.key"),
        ca.clone(),
        rx_map.remove("box-1").unwrap(),
    );
    spawn_agent(
        hub.clone(),
        "box-2",
        "127.0.0.1:14434",
        box2.token,
        demo.join("box-2.key"),
        ca.clone(),
        rx_map.remove("box-2").unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    spawn_agent(
        hub.clone(),
        "alice",
        "127.0.0.1:14433",
        alice.token,
        demo.join("alice.key"),
        ca.clone(),
        rx_map.remove("alice").unwrap(),
    );
    spawn_agent(
        hub.clone(),
        "bob",
        "127.0.0.1:14434",
        bob.token,
        demo.join("bob.key"),
        ca,
        rx_map.remove("bob").unwrap(),
    );

    for _ in 0..80 {
        if hub.online.load(Ordering::SeqCst) >= 4 {
            hub.ready.store(true, Ordering::SeqCst);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !hub.ready.load(Ordering::SeqCst) {
        tracing::error!("agents did not come online");
    }

    let app = Router::new()
        .route("/", get(|| async { Html(ui::page()) }))
        .route("/health", get(health))
        .route("/play", post(play))
        .route("/ws", get(ws_upgrade))
        .with_state(hub);

    tracing::info!("demo ui http://{}", cli.bind);
    let listener = tokio::net::TcpListener::bind(cli.bind).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    drop(child_a);
    drop(child_b);
    Ok(())
}

async fn health(State(hub): State<Hub>) -> impl IntoResponse {
    if hub.ready.load(Ordering::SeqCst) {
        (axum::http::StatusCode::OK, "ok")
    } else {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "starting")
    }
}

async fn play(State(hub): State<Hub>) -> impl IntoResponse {
    if hub
        .playing
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        let hub = hub.clone();
        tokio::spawn(async move {
            run_script(&hub).await;
            hub.playing.store(false, Ordering::SeqCst);
        });
    }
    axum::http::StatusCode::NO_CONTENT
}

async fn run_script(hub: &Hub) {
    let seq = [
        ("alice", "@box-2 hello from alice"),
        ("bob", "@box-1 hello from bob"),
        ("box-2", "@alice got it"),
        ("box-1", "@bob got it"),
    ];
    for (i, (pane, line)) in seq.iter().enumerate() {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(2800)).await;
        }
        if let Some(tx) = hub.cmds.get(*pane) {
            let _ = tx.send((*line).into()).await;
        }
    }
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(hub): State<Hub>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_session(socket, hub))
}

async fn ws_session(socket: WebSocket, hub: Hub) {
    let (mut sink, mut stream) = socket.split();
    {
        let g = hub.buf.lock().await;
        for pane in ["plane-a", "plane-b", "box-1", "box-2", "alice", "bob"] {
            if let Some(evs) = g.get(pane) {
                for msg in evs {
                    if sink
                        .send(Message::Text(serde_json::to_string(msg).unwrap().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
    let mut rx = hub.tx.subscribe();
    loop {
        tokio::select! {
            incoming = stream.next() => {
                let Some(Ok(Message::Text(t))) = incoming else { break };
                if let Ok(cmd) = serde_json::from_str::<InCmd>(&t) {
                    let line = if !cmd.to.is_empty() {
                        format!("@{} {}", cmd.to, cmd.text)
                    } else {
                        cmd.line
                    };
                    if let Some(tx) = hub.cmds.get(&cmd.pane) {
                        let _ = tx.send(line).await;
                    }
                }
            }
            out = rx.recv() => {
                let Ok(msg) = out else { break };
                if sink
                    .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

fn reset_db(url: &str) -> Result<()> {
    let db = url.rsplit('/').next().unwrap_or("");
    if db != "plane" {
        return Ok(());
    }
    let admin = url
        .rsplit_once('/')
        .map(|(a, _)| format!("{a}/postgres"))
        .unwrap();
    let drop = std::process::Command::new("psql")
        .arg(&admin)
        .args(["-c", "DROP DATABASE IF EXISTS plane WITH (FORCE)"])
        .status()
        .context("psql drop")?;
    if !drop.success() {
        bail!("drop database plane failed");
    }
    let create = std::process::Command::new("psql")
        .arg(&admin)
        .args(["-c", "CREATE DATABASE plane"])
        .status()
        .context("psql create")?;
    if !create.success() {
        bail!("create database plane failed");
    }
    Ok(())
}

async fn wait_tcp(addr: &str) -> Result<()> {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("{addr} did not listen")
}

fn sibling_bin(name: &str) -> Result<PathBuf> {
    let mut p = std::env::current_exe().context("current_exe")?;
    p.set_file_name(name);
    if p.exists() {
        return Ok(p);
    }
    let fallback = PathBuf::from(ROOT).join("target/debug").join(name);
    if fallback.exists() {
        return Ok(fallback);
    }
    bail!("missing {name} next to demo-web")
}

fn spawn_plane(
    bin: &Path,
    database_url: &str,
    bind: &str,
    plane_id: &str,
    cert: PathBuf,
    key: PathBuf,
) -> Result<tokio::process::Child> {
    Command::new(bin)
        .args([
            "serve",
            "--bind",
            bind,
            "--plane-id",
            plane_id,
            "--tls-cert",
        ])
        .arg(cert)
        .arg("--tls-key")
        .arg(key)
        .env("DATABASE_URL", database_url)
        .env("PLANE_ID", plane_id)
        .env("RUST_LOG", "connect_control_plane=info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("spawn plane {plane_id}"))
}

fn pipe_child(pane: &'static str, pipe: Option<impl tokio::io::AsyncRead + Unpin + Send + 'static>, hub: Hub) {
    let Some(pipe) = pipe else { return };
    tokio::spawn(async move {
        let mut lines = BufReader::new(pipe).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = strip_ansi(&line);
            if let Some(ev) = parse_plane_line(pane, &line) {
                hub.emit(ev).await;
            }
        }
    });
}

fn spawn_agent(
    hub: Hub,
    name: &'static str,
    coord: &'static str,
    token: String,
    key: PathBuf,
    ca: PathBuf,
    rx: mpsc::Receiver<String>,
) {
    tokio::spawn(async move {
        if let Err(e) = agent_session(hub.clone(), name, coord, token, key, ca, rx).await {
            tracing::error!(agent = name, "{e:#}");
            hub.emit(Ev {
                pane: name.into(),
                kind: "err".into(),
                text: Some(format!("{e:#}")),
                ..Ev::default()
            })
            .await;
        }
    });
}

async fn agent_session(
    hub: Hub,
    name: &str,
    coord: &str,
    token: String,
    key: PathBuf,
    ca: PathBuf,
    mut stdin: mpsc::Receiver<String>,
) -> Result<()> {
    let pk = load_or_create_key(&key)?;
    let uri = format!("https://{coord}");
    let ca = std::fs::read(&ca).with_context(|| format!("read {}", ca.display()))?;
    let channel = Channel::from_shared(uri.clone())?
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name("localhost"),
        )?
        .connect()
        .await
        .with_context(|| format!("dial {uri}"))?;
    let mut client = PlaneClient::new(channel);
    let (tx, rx) = mpsc::channel(32);
    tx.send(ClientMsg {
        msg: Some(CMsg::Hello(Hello {
            token,
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
    hub.online.fetch_add(1, Ordering::SeqCst);
    let mut peers: HashMap<String, String> = HashMap::new();
    for p in &welcome.peers {
        remember(&mut peers, p);
        hub.emit(peer_ev(name, &p.name, true)).await;
    }
    for d in pending {
        for p in d.upsert {
            remember(&mut peers, &p);
            hub.emit(peer_ev(name, &p.name, true)).await;
        }
    }

    loop {
        tokio::select! {
            line = stdin.recv() => {
                let Some(line) = line else { break };
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                match send_line(&tx, &peers, &line).await {
                    Ok((names, body)) => {
                        for n in names {
                            hub.emit(Ev {
                                pane: name.into(),
                                kind: "send".into(),
                                to: Some(n),
                                text: Some(body.clone()),
                                ..Ev::default()
                            })
                            .await;
                        }
                    }
                    Err(e) => {
                        hub.emit(Ev {
                            pane: name.into(),
                            kind: "err".into(),
                            text: Some(format!("{e:#}")),
                            ..Ev::default()
                        })
                        .await;
                    }
                }
            }
            msg = inbound.message() => {
                let Some(msg) = msg.context("plane stream")? else {
                    hub.emit(Ev {
                        pane: name.into(),
                        kind: "err".into(),
                        text: Some("disconnected".into()),
                        ..Ev::default()
                    })
                    .await;
                    break;
                };
                match msg.msg {
                    Some(SMsg::Delta(d)) => {
                        for p in d.upsert {
                            remember(&mut peers, &p);
                            hub.emit(peer_ev(name, &p.name, true)).await;
                        }
                        for id in d.remove {
                            let gone = peers.remove(&id).unwrap_or_default();
                            if !gone.is_empty() {
                                hub.emit(peer_ev(name, &gone, false)).await;
                            }
                        }
                    }
                    Some(SMsg::App(a)) => {
                        let from = peers.get(&a.src).map(|s| s.as_str()).unwrap_or(&a.src);
                        let text = String::from_utf8_lossy(&a.data).into_owned();
                        hub.emit(Ev {
                            pane: name.into(),
                            kind: "recv".into(),
                            from: Some(from.to_string()),
                            text: Some(text),
                            ..Ev::default()
                        })
                        .await;
                    }
                    _ => {}
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
) -> Result<(Vec<String>, String)> {
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
        (vec![(id, name.to_string())], text.to_string())
    } else {
        (
            peers
                .iter()
                .map(|(id, n)| (id.clone(), n.clone()))
                .collect(),
            line.to_string(),
        )
    };
    if dsts.is_empty() {
        bail!("no peers to send to");
    }
    let mut names = Vec::new();
    for (dst, n) in dsts {
        tx.send(ClientMsg {
            msg: Some(CMsg::App(App {
                src: String::new(),
                dst,
                data: body.as_bytes().to_vec(),
                channel: String::new(),
            })),
        })
        .await?;
        names.push(n);
    }
    Ok((names, body))
}

fn remember(peers: &mut HashMap<String, String>, p: &Peer) {
    peers.insert(p.id.clone(), p.name.clone());
}

fn load_or_create_key(path: &Path) -> Result<[u8; 32]> {
    if path.exists() {
        let bytes = std::fs::read(path)?;
        return bytes
            .try_into()
            .map_err(|_| anyhow!("{} must be 32 bytes", path.display()));
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
    opts.open(path)?.write_all(&raw)?;
    Ok(raw)
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                while let Some(d) = chars.next() {
                    if d.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}
