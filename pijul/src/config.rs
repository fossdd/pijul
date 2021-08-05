use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;
use log::debug;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Global {
    pub author: Author,
    pub unrecord_changes: Option<usize>,
    pub colors: Option<Choice>,
    pub pager: Option<Choice>,
    pub template: Option<Templates>,
    pub ignore_kinds: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Author {
    pub key_path: Option<String>,
    pub name: String,
    pub email: Option<String>,
    pub full_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Choice {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "always")]
    Always,
    #[serde(rename = "never")]
    Never,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Templates {
    pub message: Option<PathBuf>,
    pub description: Option<PathBuf>,
}

pub const GLOBAL_CONFIG_DIR: &str = ".pijulconfig";
const CONFIG_DIR: &str = "pijul";

pub fn global_config_dir() -> Option<PathBuf> {
    if let Some(mut dir) = dirs_next::config_dir() {
        dir.push(CONFIG_DIR);
        Some(dir)
    } else {
        None
    }
}

impl Global {
    pub fn load() -> Result<(Global, u64), anyhow::Error> {
        if let Some(mut dir) = global_config_dir() {
            dir.push("config.toml");
            let (s, meta) = std::fs::read(&dir)
                .and_then(|x| Ok((x, std::fs::metadata(&dir)?)))
                .or_else(|e| {
                    // Read from `$HOME/.config/pijul` dir
                    if let Some(mut dir) = dirs_next::home_dir() {
                        dir.push(".config");
                        dir.push(CONFIG_DIR);
                        dir.push("config.toml");
                        std::fs::read(&dir).and_then(|x| Ok((x, std::fs::metadata(&dir)?)))
                    } else {
                        Err(e.into())
                    }
                })
                .or_else(|e| {
                    // Read from `$HOME/.pijulconfig`
                    if let Some(mut dir) = dirs_next::home_dir() {
                        dir.push(GLOBAL_CONFIG_DIR);
                        std::fs::read(&dir).and_then(|x| Ok((x, std::fs::metadata(&dir)?)))
                    } else {
                        Err(e.into())
                    }
                })?;
            debug!("s = {:?}", s);
            if let Ok(t) = toml::from_slice(&s) {
                let ts = meta
                    .modified()?
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                Ok((t, ts))
            } else {
                bail!("Could not read configuration file at {:?}", dir)
            }
        } else {
            bail!("Global configuration file missing")
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    pub default_remote: Option<String>,
    #[serde(default)]
    pub extra_dependencies: Vec<String>,
    #[serde(default)]
    pub remotes: HashMap<String, RemoteName>,
    #[serde(default)]
    pub hooks: Hooks,
    pub colors: Option<Choice>,
    pub pager: Option<Choice>,
}

#[derive(Debug)]
pub enum RemoteName {
    Name(String),
    Split(SplitRemote),
}

#[derive(Clone, Copy, Debug)]
pub enum Direction {
    Push,
    Pull,
}

impl RemoteName {
    pub fn with_dir(&self, d: Direction) -> &str {
        match (self, d) {
            (RemoteName::Name(ref s), _) => s,
            (RemoteName::Split(ref s), Direction::Pull) => &s.pull,
            (RemoteName::Split(ref s), Direction::Push) => &s.push,
        }
    }
}

use serde::de::{self, MapAccess, Visitor};
use serde::de::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;

impl<'de> Deserialize<'de> for RemoteName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringOrStruct(PhantomData<fn() -> RemoteName>);
        impl<'de> Visitor<'de> for StringOrStruct {
            type Value = RemoteName;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("string or map")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RemoteName::Name(value.to_string()))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
                // into a `Deserializer`, allowing it to be used as the input to T's
                // `Deserialize` implementation. T then deserializes itself using
                // the entries from the map visitor.
                Ok(RemoteName::Split(Deserialize::deserialize(
                    de::value::MapAccessDeserializer::new(map),
                )?))
            }
        }
        deserializer.deserialize_any(StringOrStruct(PhantomData))
    }
}

#[derive(Debug, Deserialize)]
pub struct SplitRemote {
    pub pull: String,
    pub push: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Hooks {
    #[serde(default)]
    pub record: Vec<HookEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HookEntry(toml::Value);

#[derive(Debug, Serialize, Deserialize)]
struct RawHook {
    command: String,
    args: Vec<String>,
}

impl HookEntry {
    pub fn run(&self) -> Result<(), anyhow::Error> {
        let (proc, s) = match &self.0 {
            toml::Value::String(ref s) => {
                if s.is_empty() {
                    return Ok(());
                }
                (
                    if cfg!(target_os = "windows") {
                        std::process::Command::new("cmd")
                            .args(&["/C", s])
                            .output()
                            .expect("failed to execute process")
                    } else {
                        std::process::Command::new(
                            std::env::var("SHELL").unwrap_or("sh".to_string()),
                        )
                        .arg("-c")
                        .arg(s)
                        .output()
                        .expect("failed to execute process")
                    },
                    s.clone(),
                )
            }
            v => {
                let hook = v.clone().try_into::<RawHook>()?;
                (
                    std::process::Command::new(&hook.command)
                        .args(&hook.args)
                        .output()
                        .expect("failed to execute process"),
                    hook.command,
                )
            }
        };
        if !proc.status.success() {
            let mut stderr = std::io::stderr();
            writeln!(stderr, "Hook {:?} exited with code {:?}", s, proc.status)?;
            std::process::exit(proc.status.code().unwrap_or(1))
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Remote_ {
    ssh: Option<SshRemote>,
    local: Option<String>,
    url: Option<String>,
}

#[derive(Debug)]
pub enum Remote {
    Ssh(SshRemote),
    Local { local: String },
    Http { url: String },
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshRemote {
    pub addr: String,
}

impl<'de> serde::Deserialize<'de> for Remote {
    fn deserialize<D>(deserializer: D) -> Result<Remote, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let r = Remote_::deserialize(deserializer)?;
        if let Some(ssh) = r.ssh {
            Ok(Remote::Ssh(ssh))
        } else if let Some(local) = r.local {
            Ok(Remote::Local { local })
        } else if let Some(url) = r.url {
            Ok(Remote::Http { url })
        } else {
            Ok(Remote::None)
        }
    }
}

impl serde::Serialize for Remote {
    fn serialize<D>(&self, serializer: D) -> Result<D::Ok, D::Error>
    where
        D: serde::ser::Serializer,
    {
        let r = match *self {
            Remote::Ssh(ref ssh) => Remote_ {
                ssh: Some(ssh.clone()),
                local: None,
                url: None,
            },
            Remote::Local { ref local } => Remote_ {
                local: Some(local.to_string()),
                ssh: None,
                url: None,
            },
            Remote::Http { ref url } => Remote_ {
                local: None,
                ssh: None,
                url: Some(url.to_string()),
            },
            Remote::None => Remote_ {
                local: None,
                ssh: None,
                url: None,
            },
        };
        r.serialize(serializer)
    }
}
