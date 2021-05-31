use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use clap::Clap;
use libpijul::changestore::ChangeStore;
use libpijul::{DepsTxnT, GraphTxnT, MutTxnT, MutTxnTExt, TxnT};
use libpijul::{HashMap, HashSet};
use log::*;

use crate::progress::PROGRESS;
use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Apply {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Apply change to this channel
    #[clap(long = "channel")]
    channel: Option<String>,
    /// Only apply the dependencies of the change, not the change itself. Only applicable for a single change.
    #[clap(long = "deps-only")]
    deps_only: bool,
    /// The change that need to be applied. If this value is missing, read the change in text format on the standard input.
    change: Vec<String>,
}

impl Apply {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path).await?;
        let mut txn = repo.pristine.mut_txn_begin()?;
        let (channel_name, is_current_channel) =
            repo.config.get_current_channel(self.channel.as_deref());
        let channel = if let Some(channel) = txn.load_channel(&channel_name)? {
            channel
        } else {
            bail!("Channel {:?} not found", channel_name)
        };
        let mut hashes = Vec::new();
        for ch in self.change.iter() {
            hashes.push(if let Ok(h) = txn.hash_from_prefix(ch) {
                h.0
            } else {
                let change = libpijul::change::Change::deserialize(&ch, None);
                let change = match change {
                    Ok(change) => change,
                    Err(libpijul::change::ChangeError::Io(e)) => {
                        if let std::io::ErrorKind::NotFound = e.kind() {
                            let extra = if std::path::Path::new(&ch).is_relative() {
                                " Using the full path to the change file may help."
                            } else {
                                ""
                            };
                            bail!("File {} not found, and not recognised as a prefix of a known change identifier.{}", ch, extra)
                        } else {
                            return Err(e.into())
                        }
                    }
                    Err(e) => return Err(e.into())
                };
                repo.changes.save_change(&change)?
            })
        }
        if hashes.is_empty() {
            let mut change = std::io::BufReader::new(std::io::stdin());
            let change = libpijul::change::Change::read(&mut change, &mut HashMap::default())?;
            hashes.push(repo.changes.save_change(&change)?)
        }
        if self.deps_only {
            if hashes.len() > 1 {
                bail!("--deps-only is only applicable to a single change")
            }
            let mut channel = channel.write().unwrap();
            txn.apply_deps_rec(&repo.changes, &mut channel, hashes.last().unwrap())?;
        } else {
            let mut channel = channel.write().unwrap();
            for hash in hashes.iter() {
                txn.apply_change_rec(&repo.changes, &mut channel, hash)?
            }
        }

        let mut touched = HashSet::default();
        for d in hashes.iter() {
            if let Some(int) = txn.get_internal(&d.into())? {
                debug!("int = {:?}", int);
                for inode in txn.iter_rev_touched(int)? {
                    debug!("{:?}", inode);
                    let (int_, inode) = inode?;
                    if int_ < int {
                        continue;
                    } else if int_ > int {
                        break;
                    }
                    touched.insert(*inode);
                }
            }
        }

        let txn = Arc::new(RwLock::new(txn));
        if is_current_channel {
            let mut touched_files = Vec::with_capacity(touched.len());
            let txn_ = txn.read().unwrap();
            for i in touched {
                if let Some((path, _)) =
                    libpijul::fs::find_path(&repo.changes, &*txn_, &*channel.read()?, false, i)?
                {
                    touched_files.push(path)
                } else {
                    touched_files.clear();
                    break;
                }
            }
            std::mem::drop(txn_);
            PROGRESS
                .borrow_mut()
                .unwrap()
                .push(crate::progress::Cursor::Spin {
                    i: 0,
                    pre: "Outputting repository".into(),
                });
            for path in touched_files.iter() {
                libpijul::output::output_repository_no_pending(
                    repo.working_copy.clone(),
                    &repo.changes,
                    txn.clone(),
                    channel.clone(),
                    &path,
                    true,
                    None,
                    num_cpus::get(),
                    0,
                )?;
            }
            if !touched_files.is_empty() {
                libpijul::output::output_repository_no_pending(
                    repo.working_copy.clone(),
                    &repo.changes,
                    txn.clone(),
                    channel.clone(),
                    "",
                    true,
                    None,
                    num_cpus::get(),
                    0,
                )?;
            }
            PROGRESS.join();
        }
        let txn = if let Ok(txn) = Arc::try_unwrap(txn) {
            txn.into_inner().unwrap()
        } else {
            unreachable!()
        };
        txn.commit()?;
        Ok(())
    }
}
