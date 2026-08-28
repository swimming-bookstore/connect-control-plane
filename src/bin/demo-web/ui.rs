use leptos::prelude::*;

#[component]
fn Replica(pane: &'static str, title: &'static str, bind: &'static str, color: &'static str) -> impl IntoView {
    let style = format!("--c:{color}");
    view! {
        <section class="card replica" data-pane=pane data-kind="replica" style=style>
            <header>
                <span class="live"></span>
                <div>
                    <h2>{title}</h2>
                    <code>{bind}</code>
                </div>
            </header>
            <div class="roster"></div>
            <p class="empty">"waiting for agents…"</p>
        </section>
    }
}

#[component]
fn Chat(
    pane: &'static str,
    title: &'static str,
    initial: &'static str,
    role: &'static str,
    via: &'static str,
    color: &'static str,
) -> impl IntoView {
    let style = format!("--c:{color}");
    view! {
        <section class="card chat" data-pane=pane data-kind="chat" data-role=role style=style>
            <header>
                <span class="avatar">{initial}</span>
                <div>
                    <h2>{title}</h2>
                    <p><span class="tag">{role}</span>" on "{via}</p>
                </div>
            </header>
            <div class="who">
                <span class="lbl">"To"</span>
                <div class="peers"></div>
            </div>
            <div class="feed"></div>
            <form class="composer">
                <input class="msg" type="text" autocomplete="off" spellcheck="false" placeholder="Select who, then type a message" disabled />
                <button type="submit" disabled>"Send"</button>
            </form>
        </section>
    }
}

#[component]
fn App() -> impl IntoView {
    view! {
        <header class="top">
            <div>
                <strong>"connect-control-plane"</strong>
                <span>"Two stateless replicas. Clients never see each other."</span>
            </div>
            <button id="play" type="button">"Play demo"</button>
        </header>
        <main class="grid">
            <Replica pane="plane-a" title="Replica A" bind="127.0.0.1:14433" color="#79c0ff"/>
            <Replica pane="plane-b" title="Replica B" bind="127.0.0.1:14434" color="#58a6ff"/>
            <Chat pane="box-1" title="box-1" initial="1" role="box" via="replica A" color="#7ee787"/>
            <Chat pane="box-2" title="box-2" initial="2" role="box" via="replica B" color="#56d4dd"/>
            <Chat pane="alice" title="Alice" initial="A" role="client" via="replica A" color="#d2a8ff"/>
            <Chat pane="bob" title="Bob" initial="B" role="client" via="replica B" color="#ffa657"/>
        </main>
    }
}

const CSS: &str = r#"
:root{--bg:#0b0f14;--card:#12181f;--line:#243044;--muted:#8b9bb4;--text:#e7eef9;}
*{box-sizing:border-box;}
html,body{margin:0;height:100%;background:var(--bg);color:var(--text);
  font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;}
body{display:flex;flex-direction:column;}
.top{flex:none;display:flex;align-items:center;justify-content:space-between;gap:16px;
  padding:10px 16px;border-bottom:1px solid var(--line);background:#0e141b;}
.top strong{display:block;font-size:14px;}
.top span{color:var(--muted);font-size:12px;}
#play{background:#1f6feb;color:#fff;border:0;border-radius:8px;padding:8px 14px;
  font:inherit;font-size:13px;font-weight:600;cursor:pointer;}
#play:hover{filter:brightness(1.08);}
body.rec,body.rec *{cursor:none !important;}
body.rec .top{display:none;}
.grid{flex:1;min-height:0;display:grid;grid-template-columns:1fr 1fr;
  grid-template-rows:auto minmax(0,1fr) minmax(0,1fr);
  gap:10px;padding:10px;}
.card{display:flex;flex-direction:column;min-height:0;background:var(--card);
  border:1px solid var(--line);border-radius:12px;overflow:hidden;}
.card>header{flex:none;display:flex;align-items:center;gap:10px;padding:10px 12px;
  border-bottom:1px solid var(--line);}
.card h2{margin:0;font-size:15px;color:var(--c);}
.card header p,.card header code{margin:0;color:var(--muted);font-size:11px;}
.card header code{font-family:ui-monospace,Menlo,monospace;}
.live{width:8px;height:8px;border-radius:50%;background:var(--c);box-shadow:0 0 0 4px color-mix(in srgb,var(--c) 20%,transparent);}
.avatar{width:32px;height:32px;border-radius:10px;display:grid;place-items:center;
  background:color-mix(in srgb,var(--c) 22%,#000);color:var(--c);font-weight:700;}
.tag{display:inline-block;padding:1px 6px;border-radius:999px;background:#1b2430;
  color:var(--muted);font-size:10px;letter-spacing:.04em;text-transform:uppercase;margin-right:6px;}
.replica{min-height:132px;}
.replica .roster{flex:none;display:flex;flex-wrap:wrap;gap:8px;align-content:flex-start;padding:10px 12px 12px;min-height:40px;}
.replica .empty{margin:0;padding:0 12px 12px;color:var(--muted);font-size:12px;}
.replica.has-agents .empty{display:none;}
.chip{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;
  border:1px solid color-mix(in srgb,var(--chip) 45%,var(--line));
  background:color-mix(in srgb,var(--chip) 14%,transparent);color:var(--chip);
  font-size:12px;font-weight:600;}
.chip .dot{width:7px;height:7px;border-radius:50%;background:var(--chip);}
.feed{flex:1;overflow:auto;padding:10px 12px;display:flex;flex-direction:column;gap:6px;
  scrollbar-width:none;}
.feed::-webkit-scrollbar{display:none;}
.feed:empty::before{content:"Messages show up here.";color:var(--muted);font-size:13px;margin:auto;}
.bubble{max-width:85%;padding:8px 10px;border-radius:12px;font-size:13px;line-height:1.35;}
.bubble small{display:block;opacity:.7;font-size:10px;margin-bottom:2px;}
.bubble.in{align-self:flex-start;background:#1b2430;color:var(--text);}
.bubble.out{align-self:flex-end;background:color-mix(in srgb,var(--c) 22%,#1b2430);color:var(--text);}
.who{flex:none;display:flex;align-items:center;gap:8px;padding:8px 12px;border-bottom:1px solid var(--line);}
.who .lbl{flex:none;color:var(--muted);font-size:12px;}
.peers{display:flex;flex-wrap:wrap;gap:6px;flex:1;}
.peers:empty::after{content:"none online";color:var(--muted);font-size:12px;font-style:italic;}
.peer{border:1px solid var(--line);background:#0e141b;color:var(--chip);border-radius:999px;
  padding:4px 10px;font:inherit;font-size:12px;font-weight:600;cursor:pointer;}
.peer.on{border-color:color-mix(in srgb,var(--chip) 55%,var(--line));
  background:color-mix(in srgb,var(--chip) 16%,transparent);}
.composer{flex:none;display:flex;align-items:center;gap:8px;padding:8px;border-top:1px solid var(--line);}
.composer input{flex:1;min-width:0;border:1px solid var(--line);border-radius:8px;background:#0e141b;
  color:var(--text);padding:8px 10px;font:inherit;font-size:13px;outline:none;}
.composer input:focus{border-color:var(--c);}
.composer input:disabled{opacity:.5;}
.composer button{border:0;border-radius:8px;background:var(--c);color:#0b0f14;
  padding:8px 12px;font:inherit;font-size:13px;font-weight:700;cursor:pointer;}
.composer button:disabled{opacity:.4;cursor:not-allowed;}
"#;

const JS: &str = r##"
(() => {
  const rec = new URLSearchParams(location.search).has("rec");
  if (rec) document.body.classList.add("rec");
  const COLOR = {"alice":"#d2a8ff","bob":"#ffa657","box-1":"#7ee787","box-2":"#56d4dd"};
  const cards = {};
  document.querySelectorAll("[data-pane]").forEach((el) => {
    cards[el.dataset.pane] = {
      el,
      kind: el.dataset.kind,
      roster: el.querySelector(".roster"),
      peers: el.querySelector(".peers"),
      feed: el.querySelector(".feed"),
      input: el.querySelector(".msg"),
      btn: el.querySelector("button[type=submit]"),
      form: el.querySelector("form"),
      selected: null,
    };
  });

  function chipStyle(name) {
    return "--chip:" + (COLOR[name] || "#8b9bb4");
  }
  function setPeer(card, name, on) {
    const box = card.peers;
    if (!box) return;
    let b = box.querySelector('[data-name="'+name+'"]');
    if (!on) {
      if (b) b.remove();
      if (card.selected === name) card.selected = null;
      syncComposer(card);
      return;
    }
    if (!b) {
      b = document.createElement("button");
      b.type = "button";
      b.className = "peer";
      b.dataset.name = name;
      b.style = chipStyle(name);
      b.textContent = name;
      b.addEventListener("click", () => {
        card.selected = name;
        syncComposer(card);
      });
      box.appendChild(b);
    }
    syncComposer(card);
  }
  function syncComposer(card) {
    if (!card.input) return;
    const to = card.selected;
    card.input.disabled = !to;
    card.btn.disabled = !to;
    card.input.placeholder = to ? ("Message " + to) : "Select who, then type a message";
    card.el.querySelectorAll(".peer").forEach((p) => p.classList.toggle("on", p.dataset.name === to));
  }
  function bubble(card, dir, who, text) {
    if (!card.feed) return;
    const d = document.createElement("div");
    d.className = "bubble " + dir;
    d.innerHTML = "<small></small>";
    d.querySelector("small").textContent = who;
    d.appendChild(document.createTextNode(text));
    card.feed.appendChild(d);
    card.feed.scrollTop = card.feed.scrollHeight;
  }
  function roster(card, name, on) {
    if (!card.roster) return;
    let c = card.roster.querySelector('[data-name="'+name+'"]');
    if (!on) { if (c) c.remove(); }
    else if (!c) {
      c = document.createElement("span");
      c.className = "chip";
      c.dataset.name = name;
      c.style = chipStyle(name);
      c.innerHTML = "<span class='dot'></span>";
      c.appendChild(document.createTextNode(name));
      card.roster.appendChild(c);
    }
    card.el.classList.toggle("has-agents", card.roster.children.length > 0);
  }

  const ws = new WebSocket((location.protocol === "https:" ? "wss:" : "ws:") + "//" + location.host + "/ws");
  ws.onmessage = (e) => {
    const m = JSON.parse(e.data);
    const card = cards[m.pane];
    if (!card) return;
    if (m.kind === "joined") roster(card, m.name, true);
    else if (m.kind === "left") roster(card, m.name, false);
    else if (m.kind === "peer") setPeer(card, m.name, true);
    else if (m.kind === "gone") setPeer(card, m.name, false);
    else if (m.kind === "send") bubble(card, "out", "to " + m.to, m.text);
    else if (m.kind === "recv") bubble(card, "in", m.from, m.text);
    else if (m.kind === "err") bubble(card, "in", "error", m.text || "");
  };
  Object.values(cards).forEach((card) => {
    if (!card.form) return;
    card.form.addEventListener("submit", (e) => {
      e.preventDefault();
      const text = card.input.value.trim();
      const to = card.selected;
      if (!text || !to) return;
      ws.send(JSON.stringify({ pane: card.el.dataset.pane, to, text }));
      card.input.value = "";
    });
  });
  document.getElementById("play").addEventListener("click", () => {
    fetch("/play", { method: "POST" });
  });
})();
"##;

pub fn page() -> String {
    let body = Owner::new().with(|| view! { <App/> }.to_html());
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"/>\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\
         <title>ccp-demo</title><style>{CSS}</style></head>\
         <body>{body}<script>{JS}</script></body></html>"
    )
}
