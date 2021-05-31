use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::bail;
use canonical_path::CanonicalPathBuf;
use clap::Clap;
use libpijul::pristine::{ChangeId, ChannelMutTxnT, Position};
use libpijul::{ChannelTxnT, DepsTxnT, MutTxnT, TxnT, TxnTExt};
use log::*;

use crate::progress::PROGRESS;
use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Reset {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    pub repo_path: Option<PathBuf>,
    /// Reset the working copy to this channel, and change the current channel to this channel.
    #[clap(long = "channel")]
    pub channel: Option<String>,
    /// Print this file to the standard output, without modifying the repository (works for a single file only).
    #[clap(long = "dry-run")]
    pub dry_run: bool,
    /// Only reset these files
    pub files: Vec<PathBuf>,
}

impl Reset {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        self.reset(true).await
    }

    pub async fn switch(self) -> Result<(), anyhow::Error> {
        self.reset(false).await
    }

    async fn reset(self, overwrite_changes: bool) -> Result<(), anyhow::Error> {
        let has_repo_path = self.repo_path.is_some();
        let repo = Repository::find_root(self.repo_path).await?;
        let txn = repo.pristine.mut_txn_begin()?;

        let config_path = repo.config_path();
        let mut config = repo.config;
        let (channel_name, _) = config.get_current_channel(self.channel.as_deref());
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        let channel = if let Some(channel) = txn.load_channel(&channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };

        if self.dry_run {
            if self.files.len() != 1 {
                bail!("reset --dry-run needs exactly one file");
            }
            let (pos, _ambiguous) = if has_repo_path {
                let root = std::fs::canonicalize(repo.path.join(&self.files[0]))?;
                let path = root.strip_prefix(&repo_path)?;
                use path_slash::PathExt;
                let path = path.to_slash_lossy();
                txn.follow_oldest_path(&repo.changes, &channel, &path)?
            } else {
                let mut root = crate::current_dir()?;
                root.push(&self.files[0]);
                let root = std::fs::canonicalize(&root)?;
                let path = root.strip_prefix(&repo_path)?;
                use path_slash::PathExt;
                let path = path.to_slash_lossy();
                txn.follow_oldest_path(&repo.changes, &channel, &path)?
            };
            libpijul::output::output_file(
                &repo.changes,
                &txn,
                &channel.read().unwrap(),
                pos,
                &mut libpijul::vertex_buffer::Writer::new(std::io::stdout()),
            )?;
            return Ok(());
        }

        let txn = Arc::new(RwLock::new(txn));

        let (current_channel, _) = config.get_current_channel(None);
        if self.channel.as_deref() == Some(current_channel) {
            if !overwrite_changes {
                return Ok(());
            }
        } else if self.channel.is_some() {
            if !self.files.is_empty() {
                bail!("Cannot use --channel with individual paths. Did you mean --dry-run?")
            }
            let channel = {
                let txn = txn.read().unwrap();
                txn.load_channel(current_channel)?
            };
            if let Some(channel) = channel {
                let mut state = libpijul::RecordBuilder::new();
                state.record(
                    txn.clone(),
                    libpijul::Algorithm::default(),
                    channel.clone(),
                    repo.working_copy.clone(),
                    &repo.changes,
                    "",
                    num_cpus::get(),
                )?;
                let rec = state.finish();
                debug!("actions = {:?}", rec.actions);
                if !rec.actions.is_empty() {
                    bail!("Cannot change channel, as there are unrecorded changes.")
                }
            }
        }

        let now = std::time::Instant::now();
        if self.files.is_empty() {
            if self.channel.is_none() || self.channel.as_deref() == Some(current_channel) {
                let last_modified = last_modified(&*txn.read().unwrap(), &*channel.read()?);
                libpijul::output::output_repository_no_pending(
                    repo.working_copy.clone(),
                    &repo.changes,
                    txn.clone(),
                    channel.clone(),
                    "",
                    true,
                    Some(last_modified),
                    num_cpus::get(),
                    0,
                )?;
                let mut txn = if let Ok(txn) = Arc::try_unwrap(txn) {
                    txn.into_inner().unwrap()
                } else {
                    unreachable!()
                };
                txn.touch_channel(&mut *channel.write()?, None);
                txn.commit()?;
                return Ok(());
            }
            let mut inodes = HashSet::new();
            let txn_ = txn.read().unwrap();
            if let Some(cur) = txn_.load_channel(current_channel)? {
                let mut changediff = HashSet::new();
                let (a, b, s) = libpijul::pristine::last_common_state(
                    &*txn_,
                    &*cur.read()?,
                    &*channel.read()?,
                )?;
                let s: libpijul::Merkle = s.into();
                debug!("last common state {:?}", s);
                let (a, b) = if s == libpijul::Merkle::zero() {
                    (None, None)
                } else {
                    (Some(a), Some(b))
                };
                changes_after(&*txn_, &*cur.read()?, a, &mut changediff, &mut inodes)?;
                changes_after(&*txn_, &*channel.read()?, b, &mut changediff, &mut inodes)?;
            }

            if self.channel.is_some() {
                config.current_channel = self.channel;
                config.save(&config_path)?;
            }
            let mut paths = Vec::with_capacity(inodes.len());
            for pos in inodes.iter() {
                if let Some((path, _)) =
                    libpijul::fs::find_path(&repo.changes, &*txn_, &*channel.read()?, false, *pos)?
                {
                    paths.push(path)
                } else {
                    paths.clear();
                    break;
                }
            }
            PROGRESS
                .borrow_mut()
                .unwrap()
                .push(crate::progress::Cursor::Spin {
                    i: 0,
                    pre: "Outputting repository".into(),
                });
            std::mem::drop(txn_);
            for path in paths.iter() {
                debug!("resetting {:?}", path);
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
            if paths.is_empty() {
                libpijul::output::output_repository_no_pending(
                    repo.working_copy,
                    &repo.changes,
                    txn.clone(),
                    channel,
                    "",
                    true,
                    None,
                    num_cpus::get(),
                    0,
                )?;
            }
            PROGRESS.join();
        } else {
            PROGRESS
                .borrow_mut()
                .unwrap()
                .push(crate::progress::Cursor::Spin {
                    i: 0,
                    pre: "Outputting repository".into(),
                });
            for root in self.files.iter() {
                let root = std::fs::canonicalize(&root)?;
                let path = root.strip_prefix(&repo_path)?;
                use path_slash::PathExt;
                let path = path.to_slash_lossy();
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
            PROGRESS.join();
        }
        let txn = if let Ok(txn) = Arc::try_unwrap(txn) {
            txn.into_inner().unwrap()
        } else {
            unreachable!()
        };
        txn.commit()?;
        debug!("now = {:?}", now.elapsed());
        let locks = libpijul::TIMERS.lock().unwrap();
        info!(
            "retrieve: {:?}, graph: {:?}, output: {:?}",
            locks.alive_retrieve, locks.alive_graph, locks.alive_output,
        );
        Ok(())
    }
}

fn changes_after<T: ChannelTxnT + DepsTxnT>(
    txn: &T,
    chan: &T::Channel,
    from: Option<u64>,
    changediff: &mut HashSet<ChangeId>,
    inodes: &mut HashSet<Position<ChangeId>>,
) -> Result<(), anyhow::Error> {
    let f = if let Some(f) = from {
        (f + 1).into()
    } else {
        0u64.into()
    };
    for x in libpijul::pristine::changeid_log(txn, chan, f)? {
        let (n, p) = x?;
        let n: u64 = (*n).into();
        debug!("{:?} {:?} {:?}", n, p, from);
        if changediff.insert(p.a) {
            for y in txn.iter_rev_touched_files(&p.a, None)? {
                let (uu, pos) = y?;
                debug_assert!(uu >= &p.a);
                if uu > &p.a {
                    break;
                }
                inodes.insert(*pos);
            }
        }
    }
    Ok(())
}

fn last_modified<T: ChannelTxnT>(txn: &T, channel: &T::Channel) -> std::time::SystemTime {
    std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(txn.last_modified(channel))
}
