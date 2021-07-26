use std::path::PathBuf;

use crate::{config, current_dir};
use anyhow::bail;
use libpijul::DOT_DIR;
use log::debug;

pub struct Repository {
    pub pristine: libpijul::pristine::sanakirja::Pristine,
    pub changes: libpijul::changestore::filesystem::FileSystem,
    pub working_copy: libpijul::working_copy::filesystem::FileSystem,
    pub config: config::Config,
    pub path: PathBuf,
    pub changes_dir: PathBuf,
}

pub const PRISTINE_DIR: &str = "pristine";
pub const CHANGES_DIR: &str = "changes";
pub const CONFIG_FILE: &str = "config";

impl Repository {
    fn find_root_(cur: Option<PathBuf>, dot_dir: &str) -> Result<PathBuf, anyhow::Error> {
        let mut cur = if let Some(cur) = cur {
            cur
        } else {
            current_dir()?
        };
        cur.push(dot_dir);
        loop {
            debug!("{:?}", cur);
            if std::fs::metadata(&cur).is_err() {
                cur.pop();
                if cur.pop() {
                    cur.push(DOT_DIR);
                } else {
                    bail!("No Pijul repository found")
                }
            } else {
                break;
            }
        }
        Ok(cur)
    }

    pub fn find_root(cur: Option<PathBuf>) -> Result<Self, anyhow::Error> {
        Self::find_root_with_dot_dir(cur, DOT_DIR)
    }

    pub fn find_root_with_dot_dir(
        cur: Option<PathBuf>,
        dot_dir: &str,
    ) -> Result<Self, anyhow::Error> {
        let cur = Self::find_root_(cur, dot_dir)?;
        let mut pristine_dir = cur.clone();
        pristine_dir.push(PRISTINE_DIR);
        let mut changes_dir = cur.clone();
        changes_dir.push(CHANGES_DIR);
        let mut working_copy_dir = cur.clone();
        working_copy_dir.pop();
        let config_path = cur.join(CONFIG_FILE);
        let config = if let Ok(config) = std::fs::read(&config_path) {
            if let Ok(toml) = toml::from_slice(&config) {
                toml
            } else {
                bail!("Could not read configuration file at {:?}", config_path)
            }
        } else {
            config::Config::default()
        };
        Ok(Repository {
            pristine: libpijul::pristine::sanakirja::Pristine::new(&pristine_dir.join("db"))?,
            working_copy: libpijul::working_copy::filesystem::FileSystem::from_root(
                &working_copy_dir,
            ),
            changes: libpijul::changestore::filesystem::FileSystem::from_root(&working_copy_dir),
            config,
            path: working_copy_dir,
            changes_dir,
        })
    }

    pub fn init(path: Option<std::path::PathBuf>) -> Result<Self, anyhow::Error> {
        let cur = if let Some(path) = path {
            path
        } else {
            current_dir()?
        };
        let mut pristine_dir = cur.clone();
        pristine_dir.push(DOT_DIR);
        pristine_dir.push(PRISTINE_DIR);
        if std::fs::metadata(&pristine_dir).is_err() {
            std::fs::create_dir_all(&pristine_dir)?;
            let mut changes_dir = cur.clone();
            changes_dir.push(DOT_DIR);
            changes_dir.push(CHANGES_DIR);
            Ok(Repository {
                pristine: libpijul::pristine::sanakirja::Pristine::new(&pristine_dir.join("db"))?,
                working_copy: libpijul::working_copy::filesystem::FileSystem::from_root(&cur),
                changes: libpijul::changestore::filesystem::FileSystem::from_root(&cur),
                config: config::Config::default(),
                path: cur,
                changes_dir,
            })
        } else {
            bail!("Already in a repository")
        }
    }
}
