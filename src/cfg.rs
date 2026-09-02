//! Shared process config. One TOML for plane, gateway, console.
//!
//! Load order: `--config` / `CONNECT_CONFIG` / `/etc/connect/connect.toml`.
//! CLI wins, then the file, then env (`DATABASE_URL`, `PLANE_ID`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Cfg {
    pub database_url: Option<String>,
    pub plane: Plane,
    pub gateway: Gateway,
    pub console: Console,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Plane {
    pub bind: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Gateway {
    pub bind: Option<String>,
    #[serde(default)]
    pub oidc: Vec<Oidc>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Oidc {
    pub issuer: String,
    pub userinfo: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Console {
    pub bind: Option<String>,
}

impl Cfg {
    /// Explicit path, else `CONNECT_CONFIG`, else `/etc/connect/connect.toml` if present.
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = match explicit {
            Some(p) => Some(p.to_path_buf()),
            None => std::env::var("CONNECT_CONFIG")
                .ok()
                .map(PathBuf::from)
                .filter(|p| !p.as_os_str().is_empty())
                .or_else(|| {
                    let p = PathBuf::from("/etc/connect/connect.toml");
                    p.is_file().then_some(p)
                }),
        };
        let Some(path) = path else {
            return Ok(Self::default());
        };
        Self::from_path(&path)
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("parse {}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self> {
        Ok(toml::from_str(text)?)
    }

    pub fn database_url(&self, cli: Option<String>) -> Result<String> {
        match first(cli, self.database_url.clone(), "DATABASE_URL") {
            Some(u) => Ok(u),
            None => bail!("database_url is required (config file, --database-url, or DATABASE_URL)"),
        }
    }
}

/// CLI, then file, then env. Empty strings skip.
pub fn first(cli: Option<String>, file: Option<String>, env: &str) -> Option<String> {
    cli.filter(|s| !s.trim().is_empty())
        .or_else(|| file.filter(|s| !s.trim().is_empty()))
        .or_else(|| std::env::var(env).ok().filter(|s| !s.trim().is_empty()))
}

pub fn addr(cli: Option<SocketAddr>, file: Option<&str>, default: &str) -> Result<SocketAddr> {
    if let Some(a) = cli {
        return Ok(a);
    }
    let s = file.filter(|s| !s.is_empty()).unwrap_or(default);
    s.parse().with_context(|| format!("bind address {s}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
database_url = "postgres://connect@127.0.0.1/connect"

[plane]
bind = "0.0.0.0:4433"
tls_cert = "/etc/connect/plane.pem"
tls_key = "/etc/connect/plane-key.pem"
id = "plane-a"

[gateway]
bind = "127.0.0.1:8787"

[[gateway.oidc]]
issuer = "https://accounts.google.com"
userinfo = "https://openidconnect.googleapis.com/v1/userinfo"

[[gateway.oidc]]
issuer = "https://dex.example"
userinfo = "https://dex.example/userinfo"

[console]
bind = "127.0.0.1:3040"
"#;

    #[test]
    fn parse_full() {
        let c = Cfg::parse(SAMPLE).unwrap();
        assert!(c.database_url.unwrap().contains("postgres"));
        assert_eq!(c.plane.id.as_deref(), Some("plane-a"));
        assert_eq!(c.gateway.oidc.len(), 2);
        assert_eq!(c.gateway.oidc[1].issuer, "https://dex.example");
        assert_eq!(c.console.bind.as_deref(), Some("127.0.0.1:3040"));
    }

    #[test]
    fn empty_ok() {
        let c = Cfg::parse("").unwrap();
        assert!(c.database_url.is_none());
        assert!(c.gateway.oidc.is_empty());
    }

    #[test]
    fn cli_wins_database() {
        let c = Cfg::parse("database_url = \"from-file\"").unwrap();
        assert_eq!(c.database_url(Some("from-cli".into())).unwrap(), "from-cli");
        assert_eq!(c.database_url(None).unwrap(), "from-file");
    }

    #[test]
    fn addr_prefers_cli() {
        let a: SocketAddr = "10.0.0.1:9".parse().unwrap();
        assert_eq!(addr(Some(a), Some("127.0.0.1:1"), "0.0.0.0:2").unwrap(), a);
        assert_eq!(
            addr(None, Some("127.0.0.1:8787"), "0.0.0.0:2")
                .unwrap()
                .to_string(),
            "127.0.0.1:8787"
        );
    }
}
