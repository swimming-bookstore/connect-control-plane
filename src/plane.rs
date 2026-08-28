use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::client_msg::Msg as CMsg;
use crate::pb::plane_server::Plane as PlaneRpc;
use crate::pb::server_msg::Msg as SMsg;
use crate::pb::{App, ClientMsg, Delta, Hello, Peer, ServerMsg, Welcome};
use crate::store::{BindErr, Kind, Store};
use uuid::Uuid;

const PUB_LEN: usize = 32;
const CHAN: usize = 256;
const MAX_MSG: usize = 256 * 1024;

#[derive(Clone)]
pub struct Plane {
    store: Store,
    live: Live,
    plane_id: Uuid,
}

/// Local sockets only. Presence, channels, and `App` routing live in Postgres.
#[derive(Clone)]
struct Live {
    conns: Arc<Mutex<HashMap<String, Slot>>>,
}

struct Slot {
    tenant_id: Uuid,
    kind: Kind,
    tx: mpsc::Sender<Result<ServerMsg, Status>>,
}

#[derive(Debug)]
struct Session {
    tenant_id: Uuid,
    tenant: String,
    agent_id: String,
    name: String,
    kind: Kind,
    pubkey: Vec<u8>,
}

impl Plane {
    pub fn new(store: Store) -> Self {
        Self::with_id(store, Uuid::new_v4())
    }

    pub fn with_id(store: Store, plane_id: Uuid) -> Self {
        let live = Live {
            conns: Arc::new(Mutex::new(HashMap::new())),
        };
        let p = Self {
            store: store.clone(),
            live: live.clone(),
            plane_id: plane_id.clone(),
        };
        tokio::spawn(listen_loop(store, live, plane_id));
        p
    }

    pub fn into_service(self) -> crate::pb::plane_server::PlaneServer<Self> {
        crate::pb::plane_server::PlaneServer::new(self)
            .max_decoding_message_size(MAX_MSG)
            .max_encoding_message_size(MAX_MSG)
    }

    async fn connect(
        &self,
        hello: Hello,
        tx: mpsc::Sender<Result<ServerMsg, Status>>,
    ) -> Result<Session, Status> {
        let agent = self
            .store
            .auth(&hello.token)
            .await
            .ok_or_else(|| Status::unauthenticated("bad token"))?;
        let pk: [u8; PUB_LEN] = hello
            .r#pub
            .as_slice()
            .try_into()
            .map_err(|_| Status::unauthenticated("bad token"))?;
        if let Some(bound) = agent.pubkey {
            if bound != pk {
                return Err(Status::unauthenticated("bad token"));
            }
        } else {
            match self.store.bind_pub(agent.agent_id, &pk).await {
                Ok(()) => {}
                Err(BindErr::Mismatch) => return Err(Status::unauthenticated("bad token")),
                Err(BindErr::Busy) => return Err(Status::internal("bind failed")),
            }
        }

        let sess = Session {
            tenant_id: agent.tenant_id.clone(),
            tenant: agent.tenant.clone(),
            agent_id: agent.agent_id.to_string(),
            name: agent.name.clone(),
            kind: agent.kind,
            pubkey: pk.to_vec(),
        };

        {
            let g = self.live.conns.lock().await;
            if g.contains_key(&sess.agent_id) {
                return Err(Status::already_exists("already connected"));
            }
        }

        if !self
            .store
            .session_up(
                agent.agent_id,
                sess.tenant_id,
                &sess.name,
                sess.kind,
                &sess.pubkey,
                self.plane_id,
            )
            .await
            .map_err(|_| Status::internal("session"))?
        {
            return Err(Status::already_exists("already connected"));
        }

        let peers = self.peers_for(&sess).await;
        let _ = tx.try_send(Ok(ServerMsg {
            msg: Some(SMsg::Welcome(Welcome {
                agent_id: sess.agent_id.clone(),
                tenant: sess.tenant.clone(),
                name: sess.name.clone(),
                peers,
            })),
        }));

        {
            let mut g = self.live.conns.lock().await;
            g.insert(
                sess.agent_id.clone(),
                Slot {
                    tenant_id: sess.tenant_id.clone(),
                    kind: sess.kind,
                    tx: tx.clone(),
                },
            );
        }

        self.fanout_local(
            sess.tenant_id,
            sess.kind,
            Some(&sess.agent_id),
            ServerMsg {
                msg: Some(SMsg::Delta(Delta {
                    upsert: vec![Peer {
                        id: sess.agent_id.clone(),
                        name: sess.name.clone(),
                        r#pub: sess.pubkey.clone(),
                    }],
                    remove: vec![],
                })),
            },
        )
        .await;
        tracing::info!(
            tenant = %sess.tenant,
            agent = %sess.name,
            kind = sess.kind.as_str(),
            id = %sess.agent_id,
            plane = %self.plane_id,
            "online"
        );
        Ok(sess)
    }


    async fn peers_for(&self, sess: &Session) -> Vec<Peer> {
        let Ok(rows) = self.store.list_sessions(sess.tenant_id).await else {
            return Vec::new();
        };
        rows.into_iter()
            .filter(|a| a.agent_id.to_string() != sess.agent_id && sees(sess.kind, a.kind))
            .map(|a| Peer {
                id: a.agent_id.to_string(),
                name: a.name,
                r#pub: a.pubkey,
            })
            .collect()
    }

    async fn handle_app(&self, sess: &Session, mut app: App) {
        app.src = sess.agent_id.clone();
        let Some(other) = resolve_dst(&app, &sess.agent_id) else {
            return;
        };
        let from_kind = sess.kind;
        let dst_kind = {
            let g = self.live.conns.lock().await;
            if let Some(s) = g.get(&other) {
                if s.tenant_id != sess.tenant_id {
                    return;
                }
                s.kind
            } else {
                drop(g);
                let Ok(id) = Uuid::parse_str(&other) else {
                    return;
                };
                match self.store.get_session(id).await {
                    Ok(Some(a)) if a.tenant_id == sess.tenant_id => a.kind,
                    _ => return,
                }
            }
        };
        if !sees(from_kind, dst_kind) {
            return;
        }
        app.channel = pipe_tag(&sess.agent_id, &other);
        app.dst = other.clone();

        let local = {
            let g = self.live.conns.lock().await;
            g.get(&other).map(|s| s.tx.clone())
        };
        if let Some(tx) = local {
            if tx
                .try_send(Ok(ServerMsg {
                    msg: Some(SMsg::App(app)),
                }))
                .is_err()
            {
                tracing::warn!("app drop");
            }
            return;
        }
        let _ = self.store.enqueue_app(sess.tenant_id, &app).await;
    }

    async fn disconnect(&self, sess: &Session) {
        self.live.conns.lock().await.remove(&sess.agent_id);
        if let Ok(aid) = Uuid::parse_str(&sess.agent_id) {
            let _ = self
                .store
                .session_down(aid, sess.tenant_id, self.plane_id, sess.kind)
                .await;
        }
        self.fanout_local(
            sess.tenant_id,
            sess.kind,
            Some(&sess.agent_id),
            ServerMsg {
                msg: Some(SMsg::Delta(Delta {
                    upsert: vec![],
                    remove: vec![sess.agent_id.clone()],
                })),
            },
        )
        .await;
        tracing::info!(
            tenant = %sess.tenant,
            agent = %sess.name,
            kind = sess.kind.as_str(),
            id = %sess.agent_id,
            "offline"
        );
    }

    async fn fanout_local(
        &self,
        tenant_id: Uuid,
        kind: Kind,
        except: Option<&str>,
        msg: ServerMsg,
    ) {
        let txs: Vec<_> = {
            let g = self.live.conns.lock().await;
            g.iter()
                .filter(|(id, s)| {
                    s.tenant_id == tenant_id
                        && except.map(|e| *id != e).unwrap_or(true)
                        && sees(s.kind, kind)
                })
                .map(|(_, s)| s.tx.clone())
                .collect()
        };
        for tx in txs {
            let _ = tx.try_send(Ok(msg.clone()));
        }
    }

    #[cfg(test)]
    async fn online(&self, tenant: Uuid) -> Vec<String> {
        let mut ids: Vec<_> = self
            .store
            .list_sessions(tenant)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|a| a.agent_id.to_string())
            .collect();
        ids.sort();
        ids
    }

    async fn drive(
        &self,
        mut inbound: Streaming<ClientMsg>,
        tx: mpsc::Sender<Result<ServerMsg, Status>>,
    ) {
        let first = match tokio::time::timeout(Duration::from_secs(15), inbound.message()).await {
            Ok(Ok(Some(m))) => m,
            _ => {
                let _ = tx
                    .send(Err(Status::unauthenticated("hello required")))
                    .await;
                return;
            }
        };
        let hello = match first.msg {
            Some(CMsg::Hello(h)) => h,
            _ => {
                let _ = tx
                    .send(Err(Status::invalid_argument("first message must be hello")))
                    .await;
                return;
            }
        };
        let sess = match self.connect(hello, tx.clone()).await {
            Ok(s) => s,
            Err(st) => {
                let _ = tx.send(Err(st)).await;
                return;
            }
        };
        while let Ok(Some(m)) = inbound.message().await {
            if let Some(CMsg::App(a)) = m.msg {
                self.handle_app(&sess, a).await;
            }
        }
        self.disconnect(&sess).await;
    }
}

async fn listen_loop(store: Store, live: Live, plane_id: Uuid) {
    let mut hb = tokio::time::interval(Duration::from_secs(5));
    loop {
        match sqlx::postgres::PgListener::connect(store.db_url()).await {
            Ok(mut lis) => {
                if lis.listen("ccp_presence").await.is_err() || lis.listen("ccp_app").await.is_err()
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                loop {
                    tokio::select! {
                        _ = hb.tick() => {
                            let _ = store.heartbeat(plane_id).await;
                            let _ = store.reap_stale().await;
                            drain_queue(&store, &live, plane_id).await;
                        }
                        n = lis.recv() => {
                            let Ok(n) = n else { break };
                            match n.channel() {
                                "ccp_presence" => {
                                    handle_presence(&store, &live, plane_id, n.payload()).await;
                                }
                                "ccp_app" => drain_queue(&store, &live, plane_id).await,
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

async fn drain_queue(store: &Store, live: &Live, plane_id: Uuid) {
    let Ok(apps) = store.take_queued(plane_id).await else {
        return;
    };
    for (_, app) in apps {
        let tx = {
            let g = live.conns.lock().await;
            g.get(&app.dst).map(|s| s.tx.clone())
        };
        if let Some(tx) = tx {
            let _ = tx.try_send(Ok(ServerMsg {
                msg: Some(SMsg::App(app)),
            }));
        }
    }
}

async fn handle_presence(store: &Store, live: &Live, plane_id: Uuid, payload: &str) {
    let mut it = payload.split('\t');
    let op = it.next().unwrap_or("");
    let tenant = it.next().unwrap_or("");
    let agent = it.next().unwrap_or("");
    let pid = it.next().unwrap_or("");
    let Ok(tenant) = Uuid::parse_str(tenant) else {
        return;
    };
    if agent.is_empty() || Uuid::parse_str(pid).ok() == Some(plane_id) {
        return;
    }
    let txs_and_msg = match op {
        "u" => {
            let Ok(id) = Uuid::parse_str(agent) else {
                return;
            };
            let Ok(Some(a)) = store.get_session(id).await else {
                return;
            };
            let kind = a.kind;
            let msg = ServerMsg {
                msg: Some(SMsg::Delta(Delta {
                    upsert: vec![Peer {
                        id: agent.to_string(),
                        name: a.name,
                        r#pub: a.pubkey,
                    }],
                    remove: vec![],
                })),
            };
            Some((kind, msg))
        }
        "d" => {
            let kind = match it.next() {
                Some("client") => Kind::Client,
                _ => Kind::Box,
            };
            Some((
                kind,
                ServerMsg {
                    msg: Some(SMsg::Delta(Delta {
                        upsert: vec![],
                        remove: vec![agent.to_string()],
                    })),
                },
            ))
        }
        _ => None,
    };
    let Some((kind, msg)) = txs_and_msg else {
        return;
    };
    let txs: Vec<_> = {
        let g = live.conns.lock().await;
        g.values()
            .filter(|s| s.tenant_id == tenant && sees(s.kind, kind))
            .map(|s| s.tx.clone())
            .collect()
    };
    for tx in txs {
        let _ = tx.try_send(Ok(msg.clone()));
    }
}

fn resolve_dst(app: &App, from: &str) -> Option<String> {
    if !app.channel.is_empty() {
        let (a, b) = parse_pipe(&app.channel)?;
        if from == a {
            return Some(b);
        }
        if from == b {
            return Some(a);
        }
        return None;
    }
    if app.dst.is_empty() || app.dst == from {
        None
    } else {
        Some(app.dst.clone())
    }
}

fn pipe_tag(a: &str, b: &str) -> String {
    if a <= b {
        format!("{a}:{b}")
    } else {
        format!("{b}:{a}")
    }
}

fn parse_pipe(ch: &str) -> Option<(String, String)> {
    let (a, b) = ch.split_once(':')?;
    if a.is_empty() || b.is_empty() {
        None
    } else {
        Some((a.to_string(), b.to_string()))
    }
}

fn sees(a: Kind, b: Kind) -> bool {
    !(a == Kind::Client && b == Kind::Client)
}

#[tonic::async_trait]
impl PlaneRpc for Plane {
    type SessionStream = ReceiverStream<Result<ServerMsg, Status>>;

    async fn session(
        &self,
        req: Request<Streaming<ClientMsg>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let (tx, rx) = mpsc::channel(CHAN);
        let plane = self.clone();
        tokio::spawn(async move {
            plane.drive(req.into_inner(), tx).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{replica_id, Store};

    async fn setup() -> (Store, Plane) {
        let store = Store::connect(&pg_url()).await.expect("postgres");
        let plane = Plane::new(store.clone());
        (store, plane)
    }

    fn pg_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@127.0.0.1:5432/plane".into()
        })
    }

    fn uniq(prefix: &str) -> String {
        let mut b = [0u8; 4];
        getrandom::getrandom(&mut b).expect("rand");
        format!(
            "{prefix}-{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        )
    }

    fn pk(n: u8) -> Vec<u8> {
        vec![n; 32]
    }

    fn hello(token: &str, pk: Vec<u8>) -> Hello {
        Hello {
            token: token.into(),
            r#pub: pk,
        }
    }

    fn app(dst: &str, data: &[u8]) -> App {
        App {
            src: "spoof".into(),
            dst: dst.into(),
            data: data.to_vec(),
            channel: String::new(),
        }
    }

    async fn open(plane: &Plane, h: Hello) -> (Session, mpsc::Receiver<Result<ServerMsg, Status>>) {
        let (tx, rx) = mpsc::channel(CHAN);
        let sess = plane.connect(h, tx).await.expect("connect");
        (sess, rx)
    }

    fn pop_welcome(rx: &mut mpsc::Receiver<Result<ServerMsg, Status>>) -> Welcome {
        match rx.try_recv().expect("welcome").expect("status").msg {
            Some(SMsg::Welcome(w)) => w,
            other => panic!("expected welcome, got {other:?}"),
        }
    }

    fn pop_app(rx: &mut mpsc::Receiver<Result<ServerMsg, Status>>) -> App {
        match rx.try_recv().expect("app").expect("status").msg {
            Some(SMsg::App(a)) => a,
            other => panic!("expected app, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn alice_never_sees_bob() {
        let (store, plane) = setup().await;
        let alice = uniq("alice");
        let bob = uniq("bob");
        let ta = store.create_tenant(&alice, None).await.expect("tenant");
        let tb = store.create_tenant(&bob, None).await.expect("tenant");
        let a = store
            .create_agent(&alice, "laptop", Kind::Client)
            .await
            .expect("agent");
        let b = store
            .create_agent(&bob, "laptop", Kind::Client)
            .await
            .expect("agent");

        let (sa, mut ra) = open(&plane, hello(&a.token, pk(1))).await;
        let (sb, mut rb) = open(&plane, hello(&b.token, pk(2))).await;
        let wa = pop_welcome(&mut ra);
        let wb = pop_welcome(&mut rb);
        assert!(wa.peers.is_empty());
        assert!(wb.peers.is_empty());
        assert_eq!(wa.tenant, alice);
        assert_eq!(wb.tenant, bob);

        plane.handle_app(&sa, app(&sb.agent_id, b"hi")).await;
        assert!(rb.try_recv().is_err());
        assert_eq!(
            plane.online(ta.tenant_id).await,
            vec![sa.agent_id.clone()]
        );
        assert_eq!(
            plane.online(tb.tenant_id).await,
            vec![sb.agent_id.clone()]
        );
    }

    #[tokio::test]
    async fn app_does_not_cross_tenants() {
        let (store, plane) = setup().await;
        let alice = uniq("alice");
        let bob = uniq("bob");
        store.create_tenant(&alice, None).await.expect("tenant");
        store.create_tenant(&bob, None).await.expect("tenant");
        let a = store
            .create_agent(&alice, "laptop", Kind::Client)
            .await
            .expect("agent");
        let box_b = store
            .create_agent(&bob, "box", Kind::Box)
            .await
            .expect("agent");
        let (sa, mut ra) = open(&plane, hello(&a.token, pk(1))).await;
        let (sx, mut rx) = open(&plane, hello(&box_b.token, pk(2))).await;
        let _ = pop_welcome(&mut ra);
        let _ = pop_welcome(&mut rx);
        while ra.try_recv().is_ok() {}
        while rx.try_recv().is_ok() {}
        plane.handle_app(&sa, app(&sx.agent_id, b"nope")).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn same_tenant_forwards_and_overwrites_src() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "alice", Kind::Client)
            .await
            .expect("agent");
        let b = store
            .create_agent(&t, "box", Kind::Box)
            .await
            .expect("agent");
        let (sa, mut ra) = open(&plane, hello(&a.token, pk(1))).await;
        let (sb, mut rb) = open(&plane, hello(&b.token, pk(2))).await;
        let _ = pop_welcome(&mut ra);
        let wb = pop_welcome(&mut rb);
        assert_eq!(wb.peers.len(), 1);
        assert_eq!(wb.peers[0].id, sa.agent_id);

        match ra.try_recv().expect("delta").expect("status").msg {
            Some(SMsg::Delta(d)) => {
                assert_eq!(d.upsert[0].id, sb.agent_id);
            }
            other => panic!("{other:?}"),
        }

        plane.handle_app(&sa, app(&sb.agent_id, b"hello")).await;
        let got = pop_app(&mut rb);
        assert_eq!(got.src, sa.agent_id);
        assert_eq!(got.dst, sb.agent_id);
        assert_eq!(got.data, b"hello");
        assert!(!got.channel.is_empty());
    }

    #[tokio::test]
    async fn two_users_same_box_isolated_channels() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let alice = store
            .create_agent(&t, "alice", Kind::Client)
            .await
            .expect("agent");
        let bob = store
            .create_agent(&t, "bob", Kind::Client)
            .await
            .expect("agent");
        let box_a = store
            .create_agent(&t, "box", Kind::Box)
            .await
            .expect("agent");

        let (sa, mut ra) = open(&plane, hello(&alice.token, pk(1))).await;
        let (sb, mut rb) = open(&plane, hello(&bob.token, pk(2))).await;
        let (sx, mut rx) = open(&plane, hello(&box_a.token, pk(3))).await;
        let _ = pop_welcome(&mut ra);
        let _ = pop_welcome(&mut rb);
        let _ = pop_welcome(&mut rx);
        while ra.try_recv().is_ok() {}
        while rb.try_recv().is_ok() {}
        while rx.try_recv().is_ok() {}

        plane.handle_app(&sa, app(&sx.agent_id, b"from-alice")).await;
        plane.handle_app(&sb, app(&sx.agent_id, b"from-bob")).await;

        let to_box_a = pop_app(&mut rx);
        let to_box_b = pop_app(&mut rx);
        assert_eq!(to_box_a.src, sa.agent_id);
        assert_eq!(to_box_b.src, sb.agent_id);
        assert_ne!(to_box_a.channel, to_box_b.channel);
        assert!(!to_box_a.channel.is_empty());
        assert_eq!(to_box_a.data, b"from-alice");
        assert_eq!(to_box_b.data, b"from-bob");

        plane
            .handle_app(
                &sx,
                App {
                    src: String::new(),
                    dst: String::new(),
                    data: b"only-alice".to_vec(),
                    channel: to_box_a.channel.clone(),
                },
            )
            .await;
        let reply = pop_app(&mut ra);
        assert_eq!(reply.data, b"only-alice");
        assert_eq!(reply.channel, to_box_a.channel);
        assert_eq!(reply.src, sx.agent_id);
        assert_eq!(reply.dst, sa.agent_id);
        assert!(rb.try_recv().is_err());

        plane
            .handle_app(
                &sb,
                App {
                    src: String::new(),
                    dst: sx.agent_id.clone(),
                    data: b"steal".to_vec(),
                    channel: to_box_a.channel.clone(),
                },
            )
            .await;
        assert!(rx.try_recv().is_err());
        assert!(ra.try_recv().is_err());
    }

    #[tokio::test]
    async fn boxes_can_talk() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "box-a", Kind::Box)
            .await
            .expect("agent");
        let b = store
            .create_agent(&t, "box-b", Kind::Box)
            .await
            .expect("agent");
        let (sa, mut ra) = open(&plane, hello(&a.token, pk(1))).await;
        let (sb, mut rb) = open(&plane, hello(&b.token, pk(2))).await;
        let _ = pop_welcome(&mut ra);
        let wb = pop_welcome(&mut rb);
        assert_eq!(wb.peers.len(), 1);
        assert_eq!(wb.peers[0].id, sa.agent_id);
        while ra.try_recv().is_ok() {}
        plane.handle_app(&sa, app(&sb.agent_id, b"status?")).await;
        let got = pop_app(&mut rb);
        assert_eq!(got.src, sa.agent_id);
        assert_eq!(got.data, b"status?");
        assert!(!got.channel.is_empty());
    }

    #[tokio::test]
    async fn clients_cannot_talk() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "alice", Kind::Client)
            .await
            .expect("agent");
        let b = store
            .create_agent(&t, "bob", Kind::Client)
            .await
            .expect("agent");
        let (sa, mut ra) = open(&plane, hello(&a.token, pk(1))).await;
        let (sb, mut rb) = open(&plane, hello(&b.token, pk(2))).await;
        let wa = pop_welcome(&mut ra);
        let wb = pop_welcome(&mut rb);
        assert!(wa.peers.is_empty());
        assert!(wb.peers.is_empty());
        plane.handle_app(&sa, app(&sb.agent_id, b"nope")).await;
        assert!(rb.try_recv().is_err());
    }

    #[tokio::test]
    async fn two_planes_share_directory() {
        let store = Store::connect(&pg_url()).await.expect("postgres");
        let pa = Plane::with_id(store.clone(), replica_id("plane-a"));
        let pb = Plane::with_id(store.clone(), replica_id("plane-b"));
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let alice = store
            .create_agent(&t, "alice", Kind::Client)
            .await
            .expect("agent");
        let box_a = store
            .create_agent(&t, "box", Kind::Box)
            .await
            .expect("agent");
        let (sx, mut rx) = open(&pb, hello(&box_a.token, pk(2))).await;
        let (sa, mut ra) = open(&pa, hello(&alice.token, pk(1))).await;
        let _ = pop_welcome(&mut rx);
        let wa = pop_welcome(&mut ra);
        assert!(wa.peers.iter().any(|p| p.id == sx.agent_id));
        pa.handle_app(&sa, app(&sx.agent_id, b"cross")).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let got = loop {
            match rx.try_recv() {
                Ok(Ok(m)) => match m.msg {
                    Some(SMsg::App(a)) => break a,
                    _ => continue,
                },
                Ok(Err(e)) => panic!("{e}"),
                Err(_) => panic!("no cross-plane app"),
            }
        };
        assert_eq!(got.data, b"cross");
        assert_eq!(got.src, sa.agent_id);
    }

    #[tokio::test]
    async fn duplicate_session_rejected() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "one", Kind::Box)
            .await
            .expect("agent");
        let (_s, _r) = open(&plane, hello(&a.token, pk(1))).await;
        let (tx, _rx) = mpsc::channel(CHAN);
        let err = plane
            .connect(hello(&a.token, pk(1)), tx)
            .await
            .expect_err("dup");
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
    }

    #[tokio::test]
    async fn reconnect_after_disconnect() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "one", Kind::Box)
            .await
            .expect("agent");
        let (s, _r) = open(&plane, hello(&a.token, pk(1))).await;
        plane.disconnect(&s).await;
        let (_s2, mut r2) = open(&plane, hello(&a.token, pk(1))).await;
        let w = pop_welcome(&mut r2);
        assert_eq!(w.agent_id, s.agent_id);
    }

    #[tokio::test]
    async fn wrong_pub_after_bind_is_unauthenticated() {
        let (store, plane) = setup().await;
        let t = uniq("acme");
        store.create_tenant(&t, None).await.expect("tenant");
        let a = store
            .create_agent(&t, "one", Kind::Box)
            .await
            .expect("agent");
        let (s, _r) = open(&plane, hello(&a.token, pk(1))).await;
        plane.disconnect(&s).await;
        let (tx, _rx) = mpsc::channel(CHAN);
        let err = plane
            .connect(hello(&a.token, pk(2)), tx)
            .await
            .expect_err("pub");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
