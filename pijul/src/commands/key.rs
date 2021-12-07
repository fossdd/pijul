use crate::config::*;
use crate::repository::Repository;
use anyhow::bail;
use clap::Parser;
use log::debug;

use std::io::Write;
use std::path::Path;

#[derive(Parser, Debug)]
pub struct Key {
    #[clap(subcommand)]
    subcmd: Option<SubCommand>,
}

#[derive(Parser, Debug)]
pub enum SubCommand {
    Generate {
        #[clap(long = "email")]
        email: Option<String>,
        login: String,
    },
    Prove {
        #[clap(short = 'k')]
        no_cert_check: bool,
        remote: String,
    },
}

impl Key {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        match self.subcmd {
            Some(SubCommand::Generate { email, login }) => {
                if let Some(mut dir) = global_config_dir() {
                    std::fs::create_dir_all(&dir)?;
                    dir.push("secretkey.json");
                    if std::fs::metadata(&dir).is_ok() {
                        bail!("Cannot overwrite key file {:?}", dir)
                    }
                    debug!("creating file {:?}", dir);
                    let mut f = open_secret_file(&dir)?;
                    let pass = rpassword::read_password_from_tty(Some(
                        "Password for the new key (press enter to leave it unencrypted): ",
                    ))?;
                    let pass = if pass.is_empty() {
                        None
                    } else {
                        Some(pass.as_ref())
                    };

                    let k = libpijul::key::SKey::generate(None);
                    serde_json::to_writer_pretty(&mut f, &k.save(pass))?;
                    f.write_all(b"\n")?;
                    let mut stderr = std::io::stderr();
                    writeln!(stderr, "Wrote secret key in {:?}", dir)?;
                    dir.pop();

                    dir.push("publickey.json");
                    debug!("creating file {:?}", dir);
                    let mut f = std::fs::File::create(&dir)?;
                    let pk = k.public_key();
                    serde_json::to_writer_pretty(&mut f, &pk)?;
                    f.write_all(b"\n")?;

                    dir.pop();
                    dir.push("identities");
                    std::fs::create_dir_all(&dir)?;
                    dir.push(&pk.key);
                    debug!("creating file {:?}", dir);
                    let mut f = std::fs::File::create(&dir)?;
                    serde_json::to_writer_pretty(
                        &mut f,
                        &super::Identity {
                            public_key: pk,
                            origin: String::new(),
                            login,
                            email,
                            name: None,
                            last_modified: std::time::SystemTime::now()
                                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        },
                    )?;
                    f.write_all(b"\n")?;
                }
            }
            Some(SubCommand::Prove {
                remote,
                no_cert_check,
            }) => {
                let mut remote = if let Ok(repo) = Repository::find_root(None) {
                    use crate::remote::*;
                    if let RemoteRepo::Ssh(ssh) = repo
                        .remote(
                            None,
                            &remote,
                            crate::DEFAULT_CHANNEL,
                            Direction::Pull,
                            no_cert_check,
                            false,
                        )
                        .await?
                    {
                        ssh
                    } else {
                        bail!("No such remote: {}", remote)
                    }
                } else if let Some(mut ssh) = crate::remote::ssh::ssh_remote(&remote, false) {
                    if let Some(c) = ssh.connect(&remote, crate::DEFAULT_CHANNEL).await? {
                        c
                    } else {
                        bail!("No such remote: {}", remote)
                    }
                } else {
                    bail!("No such remote: {}", remote)
                };
                let key = super::load_key()?;
                remote.prove(key).await?;
            }
            None => {}
        }
        Ok(())
    }
}

#[cfg(unix)]
fn open_secret_file(path: &Path) -> Result<std::fs::File, std::io::Error> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_secret_file(path: &Path) -> Result<std::fs::File, std::io::Error> {
    std::fs::File::create(path)
}
