use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;
use log::debug;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Global {
    pub author: libpijul::change::Author,
    pub unrecord_changes: Option<usize>,
    pub colors: Option<Choice>,
    pub pager: Option<Choice>,
    pub template: Option<Templates>,
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

const CONFIG_DIR: &str = "pijul";

impl Global {
    pub fn load() -> Result<Global, anyhow::Error> {
        if let Some(mut dir) = dirs_next::config_dir() {
            dir.push(CONFIG_DIR);
            dir.push("config.toml");
            let s = std::fs::read(&dir)
                .or_else(|e| {
                    // Read from `$HOME/.config/pijul` dir
                    if let Some(mut dir) = dirs_next::home_dir() {
                        dir.push(".config");
                        dir.push(CONFIG_DIR);
                        dir.push("config.toml");
                        std::fs::read(&dir)
                    } else {
                        Err(e.into())
                    }
                })
                .or_else(|e| {
                    // Read from `$HOME/.pijulconfig`
                    if let Some(mut dir) = dirs_next::home_dir() {
                        dir.push(".pijulconfig");
                        std::fs::read(&dir)
                    } else {
                        Err(e.into())
                    }
                })?;
            debug!("s = {:?}", s);
            if let Ok(t) = toml::from_slice(&s) {
                Ok(t)
            } else {
                bail!("Could not read configuration file at {:?}", dir)
            }
        } else {
            bail!("Global configuration file missing")
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Config {
    pub current_channel: Option<String>,
    pub default_remote: Option<String>,
    #[serde(default)]
    pub extra_dependencies: Vec<String>,
    #[serde(default)]
    pub remotes: HashMap<String, String>,
    #[serde(default)]
    pub hooks: Hooks,
    pub colors: Option<Choice>,
    pub pager: Option<Choice>,
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

impl Config {
    pub fn save(&self, path: &std::path::Path) -> Result<(), anyhow::Error> {
        let config = toml::to_string(self)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(config.as_bytes())?;
        Ok(())
    }

    pub fn get_current_channel<'a>(&'a self, alt: Option<&'a str>) -> (&'a str, bool) {
        if let Some(channel) = alt {
            (channel.as_ref(), alt == self.current_channel.as_deref())
        } else if let Some(ref channel) = self.current_channel {
            (channel.as_str(), true)
        } else {
            (crate::DEFAULT_CHANNEL, true)
        }
    }

    pub fn current_channel(&self) -> Option<&str> {
        if let Some(ref channel) = self.current_channel {
            Some(channel.as_str())
        } else {
            None
        }
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
