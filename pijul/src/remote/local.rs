use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use libpijul::pristine::{Hash, Merkle, MutTxnT, Position, TxnT};
use libpijul::*;
use log::debug;

#[derive(Clone)]
pub struct Local {
    pub channel: String,
    pub root: std::path::PathBuf,
    pub changes_dir: std::path::PathBuf,
    pub pristine: Arc<libpijul::pristine::sanakirja::Pristine>,
    pub name: String,
}

pub fn get_state<T: TxnTExt>(
    txn: &T,
    channel: &libpijul::pristine::ChannelRef<T>,
    mid: Option<u64>,
) -> Result<Option<(u64, Merkle)>, anyhow::Error> {
    if let Some(mid) = mid {
        Ok(txn.get_changes(&channel, mid)?.map(|(_, m)| (mid, m)))
    } else {
        Ok(txn.reverse_log(&*channel.read(), None)?.next().map(|n| {
            let (n, (_, m)) = n.unwrap();
            (n, m.into())
        }))
    }
}

impl Local {
    pub fn get_state(&mut self, mid: Option<u64>) -> Result<Option<(u64, Merkle)>, anyhow::Error> {
        let txn = self.pristine.txn_begin()?;
        let channel = txn.load_channel(&self.channel)?.unwrap();
        Ok(get_state(&txn, &channel, mid)?)
    }

    pub fn get_id(&self) -> Result<Option<libpijul::pristine::RemoteId>, anyhow::Error> {
        let txn = self.pristine.txn_begin()?;
        if let Some(channel) = txn.load_channel(&self.channel)? {
            Ok(Some(*txn.id(&*channel.read())))
        } else {
            Ok(None)
        }
    }

    pub fn download_changelist<
        A,
        F: FnMut(&mut A, u64, Hash, Merkle) -> Result<(), anyhow::Error>,
    >(
        &mut self,
        mut f: F,
        a: &mut A,
        from: u64,
        paths: &[String],
    ) -> Result<HashSet<Position<Hash>>, anyhow::Error> {
        let store = libpijul::changestore::filesystem::FileSystem::from_root(&self.root);
        let remote_txn = self.pristine.txn_begin()?;
        let remote_channel = if let Some(channel) = remote_txn.load_channel(&self.channel)? {
            channel
        } else {
            debug!(
                "Local::download_changelist found no channel named {:?}",
                self.channel
            );
            bail!("No channel {} found for remote {}", self.name, self.channel)
        };
        let mut paths_ = HashSet::new();
        let mut result = HashSet::new();
        for s in paths {
            if let Ok((p, _ambiguous)) = remote_txn.follow_oldest_path(&store, &remote_channel, s) {
                debug!("p = {:?}", p);
                result.insert(Position {
                    change: remote_txn.get_external(&p.change)?.unwrap().into(),
                    pos: p.pos,
                });
                paths_.insert(p);
                paths_.extend(
                    libpijul::fs::iter_graph_descendants(
                        &remote_txn,
                        &remote_channel.read().graph,
                        p,
                    )?
                    .map(|x| x.unwrap()),
                );
            }
        }
        debug!("paths_ = {:?}", paths_);
        debug!("from = {:?}", from);
        for x in remote_txn.log(&*remote_channel.read(), from)? {
            let (n, (h, m)) = x?;
            assert!(n >= from);
            let h_int = remote_txn.get_internal(h)?.unwrap();
            if paths_.is_empty()
                || paths_.iter().any(|x| {
                    remote_txn
                        .get_touched_files(x, Some(h_int))
                        .unwrap()
                        .is_some()
                })
            {
                debug!("put_remote {:?} {:?} {:?}", n, h, m);
                f(a, n, h.into(), m.into())?;
            }
        }
        Ok(result)
    }

    pub fn upload_changes(
        &mut self,
        mut local: PathBuf,
        to_channel: Option<&str>,
        changes: &[Hash],
    ) -> Result<(), anyhow::Error> {
        let store = libpijul::changestore::filesystem::FileSystem::from_root(&self.root);
        let txn = self.pristine.arc_txn_begin()?;
        let channel = txn
            .write()
            .open_or_create_channel(to_channel.unwrap_or(&self.channel))?;
        for c in changes {
            libpijul::changestore::filesystem::push_filename(&mut local, &c);
            libpijul::changestore::filesystem::push_filename(&mut self.changes_dir, &c);
            std::fs::create_dir_all(&self.changes_dir.parent().unwrap())?;
            debug!("hard link {:?} {:?}", local, self.changes_dir);
            if std::fs::metadata(&self.changes_dir).is_err() {
                if std::fs::hard_link(&local, &self.changes_dir).is_err() {
                    std::fs::copy(&local, &self.changes_dir)?;
                }
            }
            debug!("hard link done");
            libpijul::changestore::filesystem::pop_filename(&mut local);
            libpijul::changestore::filesystem::pop_filename(&mut self.changes_dir);
        }
        let repo = libpijul::working_copy::filesystem::FileSystem::from_root(&self.root);
        upload_changes(&store, &mut *txn.write(), &channel, changes)?;
        libpijul::output::output_repository_no_pending(
            &repo,
            &store,
            &txn,
            &channel,
            "",
            true,
            None,
            num_cpus::get(),
            0,
        )?;
        txn.commit()?;
        Ok(())
    }

    pub async fn download_changes(
        &mut self,
        pro_n: usize,
        hashes: &mut tokio::sync::mpsc::UnboundedReceiver<libpijul::pristine::Hash>,
        send: &mut tokio::sync::mpsc::Sender<libpijul::pristine::Hash>,
        mut path: &mut PathBuf,
    ) -> Result<(), anyhow::Error> {
        while let Some(c) = hashes.recv().await {
            libpijul::changestore::filesystem::push_filename(&mut self.changes_dir, &c);
            libpijul::changestore::filesystem::push_filename(&mut path, &c);
            super::PROGRESS.borrow_mut().unwrap()[pro_n].incr();
            if std::fs::metadata(&path).is_ok() {
                debug!("metadata {:?} ok", path);
                libpijul::changestore::filesystem::pop_filename(&mut path);
                continue;
            }
            std::fs::create_dir_all(&path.parent().unwrap())?;
            if std::fs::hard_link(&self.changes_dir, &path).is_err() {
                std::fs::copy(&self.changes_dir, &path)?;
            }
            debug!("hard link done");
            libpijul::changestore::filesystem::pop_filename(&mut self.changes_dir);
            libpijul::changestore::filesystem::pop_filename(&mut path);
            debug!("sent");
            send.send(c).await?;
        }
        Ok(())
    }

    pub async fn update_identities(
        &mut self,
        _rev: Option<u64>,
        mut path: PathBuf,
    ) -> Result<u64, anyhow::Error> {
        let mut other_path = self.root.join(DOT_DIR);
        other_path.push("identities");
        let r = if let Ok(r) = std::fs::read_dir(&other_path) {
            r
        } else {
            return Ok(0);
        };
        std::fs::create_dir_all(&path)?;
        for id in r {
            let id = id?;
            let m = id.metadata()?;
            let p = id.path();
            path.push(p.file_name().unwrap());
            if let Ok(ml) = std::fs::metadata(&path) {
                if ml.modified()? < m.modified()? {
                    std::fs::remove_file(&path)?;
                } else {
                    continue;
                }
            }
            if std::fs::hard_link(&p, &path).is_err() {
                std::fs::copy(&p, &path)?;
            }
            path.pop();
        }
        Ok(0)
    }
}

pub fn upload_changes<T: MutTxnTExt, C: libpijul::changestore::ChangeStore>(
    store: &C,
    txn: &mut T,
    channel: &libpijul::pristine::ChannelRef<T>,
    changes: &[Hash],
) -> Result<(), anyhow::Error> {
    let mut ws = libpijul::ApplyWorkspace::new();
    let mut channel = channel.write();
    for c in changes {
        txn.apply_change_ws(store, &mut *channel, c, &mut ws)?;
    }
    Ok(())
}
