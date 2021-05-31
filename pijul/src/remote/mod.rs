use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use lazy_static::lazy_static;
use libpijul::pristine::{Base32, ChannelRef, GraphIter, Hash, Merkle, MutTxnT, RemoteRef, TxnT};
use libpijul::DOT_DIR;
use libpijul::{MutTxnTExt, TxnTExt};
use log::{debug, info};

use crate::repository::*;

pub mod ssh;
use ssh::*;

pub mod local;
use local::*;

pub mod http;
use http::*;

use crate::progress::PROGRESS;

pub enum RemoteRepo {
    Local(Local),
    Ssh(Ssh),
    Http(Http),
    LocalChannel(String),
    None,
}

impl Repository {
    pub async fn remote(
        &self,
        self_path: Option<&Path>,
        name: &str,
        channel: &str,
        no_cert_check: bool,
    ) -> Result<RemoteRepo, anyhow::Error> {
        if let Some(name) = self.config.remotes.get(name) {
            unknown_remote(self_path, name, channel, no_cert_check).await
        } else {
            unknown_remote(self_path, name, channel, no_cert_check).await
        }
    }
}

pub async fn unknown_remote(
    self_path: Option<&Path>,
    name: &str,
    channel: &str,
    no_cert_check: bool,
) -> Result<RemoteRepo, anyhow::Error> {
    if let Ok(url) = url::Url::parse(name) {
        let scheme = url.scheme();
        if scheme == "http" || scheme == "https" {
            debug!("unknown_remote, http = {:?}", name);
            return Ok(RemoteRepo::Http(Http {
                url,
                channel: channel.to_string(),
                client: reqwest::ClientBuilder::new()
                    .danger_accept_invalid_certs(no_cert_check)
                    .build()?,
                name: name.to_string(),
            }));
        } else if scheme == "ssh" {
            return if let Some(mut ssh) = ssh_remote(name) {
                debug!("unknown_remote, ssh = {:?}", ssh);
                Ok(RemoteRepo::Ssh(ssh.connect(name, channel).await?))
            } else {
                bail!("Remote not found: {:?}", name)
            };
        } else {
            bail!("Remote scheme not supported: {:?}", scheme)
        }
    }
    if let Ok(root) = std::fs::canonicalize(name) {
        if let Some(path) = self_path {
            let path = std::fs::canonicalize(path)?;
            if path == root {
                return Ok(RemoteRepo::LocalChannel(channel.to_string()));
            }
        }

        let mut dot_dir = root.join(DOT_DIR);
        let changes_dir = dot_dir.join(CHANGES_DIR);

        dot_dir.push(PRISTINE_DIR);
        debug!("dot_dir = {:?}", dot_dir);
        if let Ok(pristine) = libpijul::pristine::sanakirja::Pristine::new(&dot_dir.join("db")) {
            debug!("pristine done");
            return Ok(RemoteRepo::Local(Local {
                root: Path::new(name).to_path_buf(),
                channel: channel.to_string(),
                changes_dir,
                pristine: Arc::new(pristine),
                name: name.to_string(),
            }));
        }
    }
    if let Some(mut ssh) = ssh_remote(name) {
        debug!("unknown_remote, ssh = {:?}", ssh);
        Ok(RemoteRepo::Ssh(ssh.connect(name, channel).await?))
    } else {
        bail!("Remote not found: {:?}", name)
    }
}

impl RemoteRepo {
    fn name(&self) -> Option<&str> {
        match *self {
            RemoteRepo::Ssh(ref s) => Some(s.name.as_str()),
            RemoteRepo::Local(ref l) => Some(l.name.as_str()),
            RemoteRepo::Http(ref h) => Some(h.name.as_str()),
            RemoteRepo::LocalChannel(_) => None,
            RemoteRepo::None => unreachable!(),
        }
    }

    pub fn repo_name(&self) -> Result<Option<String>, anyhow::Error> {
        match *self {
            RemoteRepo::Ssh(ref s) => {
                if let Some(sep) = s.name.rfind(|c| c == ':' || c == '/') {
                    Ok(Some(s.name.split_at(sep + 1).1.to_string()))
                } else {
                    Ok(Some(s.name.as_str().to_string()))
                }
            }
            RemoteRepo::Local(ref l) => {
                if let Some(file) = l.root.file_name() {
                    Ok(Some(
                        file.to_str()
                            .context("failed to decode local repository name")?
                            .to_string(),
                    ))
                } else {
                    Ok(None)
                }
            }
            RemoteRepo::Http(ref h) => {
                if let Some(name) = libpijul::path::file_name(h.url.path()) {
                    if !name.trim().is_empty() {
                        return Ok(Some(name.trim().to_string()));
                    }
                }
                Ok(h.url.host().map(|h| h.to_string()))
            }
            RemoteRepo::LocalChannel(_) => Ok(None),
            RemoteRepo::None => unreachable!(),
        }
    }

    pub async fn finish(&mut self) -> Result<(), anyhow::Error> {
        if let RemoteRepo::Ssh(s) = self {
            s.finish().await?
        }
        Ok(())
    }

    pub async fn update_changelist<T: MutTxnTExt + TxnTExt>(
        &mut self,
        txn: &mut T,
        path: &[String],
    ) -> Result<Option<(HashSet<Position<Hash>>, RemoteRef<T>)>, anyhow::Error> {
        debug!("update_changelist");
        let name = if let Some(name) = self.name() {
            name
        } else {
            return Ok(None);
        };
        let mut remote = txn.open_or_create_remote(name).unwrap();
        let n = self
            .dichotomy_changelist(txn, &remote.lock()?.remote)
            .await?;
        debug!("update changelist {:?}", n);
        let v: Vec<_> = txn
            .iter_remote(&remote.lock()?.remote, n)?
            .filter_map(|k| {
                debug!("filter_map {:?}", k);
                let k = (*k.unwrap().0).into();
                if k >= n {
                    Some(k)
                } else {
                    None
                }
            })
            .collect();
        for k in v {
            debug!("deleting {:?}", k);
            txn.del_remote(&mut remote, k)?;
        }
        debug!("deleted");
        let paths = self.download_changelist(txn, &mut remote, n, path).await?;
        Ok(Some((paths, remote)))
    }

    async fn dichotomy_changelist<T: MutTxnT + TxnTExt>(
        &mut self,
        txn: &T,
        remote: &T::Remote,
    ) -> Result<u64, anyhow::Error> {
        let mut a = 0;
        let (mut b, state): (_, Merkle) = if let Some((u, v)) = txn.last_remote(remote)? {
            debug!("dichotomy_changelist: {:?} {:?}", u, v);
            (u, (&v.b).into())
        } else {
            debug!("the local copy of the remote has no changes");
            return Ok(0);
        };
        if let Some((_, s)) = self.get_state(txn, Some(b)).await? {
            if s == state {
                // The local list is already up to date.
                return Ok(b + 1);
            }
        }
        // Else, find the last state we have in common with the
        // remote, it might be older than the last known state (if
        // changes were unrecorded on the remote).
        while a < b {
            let mid = (a + b) / 2;
            let (mid, state) = txn.get_remote_state(remote, mid)?.unwrap();
            let remote_state = self.get_state(txn, Some(mid)).await?;
            debug!("dichotomy {:?} {:?} {:?}", mid, state, remote_state);
            if let Some((_, remote_state)) = remote_state {
                if remote_state == state.b {
                    if a == mid {
                        return Ok(a + 1);
                    } else {
                        a = mid;
                        continue;
                    }
                }
            }
            if b == mid {
                break;
            } else {
                b = mid
            }
        }
        Ok(a)
    }

    async fn get_state<T: libpijul::TxnTExt>(
        &mut self,
        txn: &T,
        mid: Option<u64>,
    ) -> Result<Option<(u64, Merkle)>, anyhow::Error> {
        match *self {
            RemoteRepo::Local(ref mut l) => l.get_state(mid),
            RemoteRepo::Ssh(ref mut s) => s.get_state(mid).await,
            RemoteRepo::Http(ref mut h) => h.get_state(mid).await,
            RemoteRepo::LocalChannel(ref channel) => {
                if let Some(channel) = txn.load_channel(&channel)? {
                    local::get_state(txn, &channel, mid)
                } else {
                    Ok(None)
                }
            }
            RemoteRepo::None => unreachable!(),
        }
    }

    pub async fn archive<W: std::io::Write + Send + 'static>(
        &mut self,
        prefix: Option<String>,
        state: Option<(Merkle, &[Hash])>,
        umask: u16,
        w: W,
    ) -> Result<u64, anyhow::Error> {
        match *self {
            RemoteRepo::Local(ref mut l) => {
                debug!("archiving local repo");
                let changes = libpijul::changestore::filesystem::FileSystem::from_root(&l.root);
                let mut tarball = libpijul::output::Tarball::new(w, prefix, umask);
                let conflicts = if let Some((state, extra)) = state {
                    let mut txn = l.pristine.mut_txn_begin()?;
                    let mut channel = txn.load_channel(&l.channel)?.unwrap();
                    txn.archive_with_state(&changes, &mut channel, &state, extra, &mut tarball, 0)?
                } else {
                    let txn = l.pristine.txn_begin()?;
                    let channel = txn.load_channel(&l.channel)?.unwrap();
                    txn.archive(&changes, &channel, &mut tarball)?
                };
                Ok(conflicts.len() as u64)
            }
            RemoteRepo::Ssh(ref mut s) => s.archive(prefix, state, w).await,
            RemoteRepo::Http(ref mut h) => h.archive(prefix, state, w).await,
            RemoteRepo::LocalChannel(_) => unreachable!(),
            RemoteRepo::None => unreachable!(),
        }
    }

    async fn download_changelist<T: MutTxnTExt>(
        &mut self,
        txn: &mut T,
        remote: &mut RemoteRef<T>,
        from: u64,
        paths: &[String],
    ) -> Result<HashSet<Position<Hash>>, anyhow::Error> {
        match *self {
            RemoteRepo::Local(ref mut l) => l.download_changelist(txn, remote, from, paths),
            RemoteRepo::Ssh(ref mut s) => s.download_changelist(txn, remote, from, paths).await,
            RemoteRepo::Http(ref h) => h.download_changelist(txn, remote, from, paths).await,
            RemoteRepo::LocalChannel(_) => Ok(HashSet::new()),
            RemoteRepo::None => unreachable!(),
        }
    }

    pub async fn upload_changes<T: MutTxnTExt>(
        &mut self,
        txn: &mut T,
        local: PathBuf,
        to_channel: Option<&str>,
        changes: &[Hash],
    ) -> Result<(), anyhow::Error> {
        match self {
            RemoteRepo::Local(ref mut l) => l.upload_changes(local, to_channel, changes),
            RemoteRepo::Ssh(ref mut s) => s.upload_changes(local, to_channel, changes).await,
            RemoteRepo::Http(ref h) => h.upload_changes(local, to_channel, changes).await,
            RemoteRepo::LocalChannel(ref channel) => {
                let mut channel = txn.open_or_create_channel(channel)?;
                let store = libpijul::changestore::filesystem::FileSystem::from_changes(local);
                local::upload_changes(&store, txn, &mut channel, changes)
            }
            RemoteRepo::None => unreachable!(),
        }
    }

    /// Start (and possibly complete) the download of a change.
    pub async fn download_changes(
        &mut self,
        pro_n: usize,
        hashes: &mut tokio::sync::mpsc::UnboundedReceiver<libpijul::pristine::Hash>,
        send: &mut tokio::sync::mpsc::Sender<libpijul::pristine::Hash>,
        path: &mut PathBuf,
        full: bool,
    ) -> Result<bool, anyhow::Error> {
        debug!("download_changes");
        match *self {
            RemoteRepo::Local(ref mut l) => l.download_changes(pro_n, hashes, send, path).await?,
            RemoteRepo::Ssh(ref mut s) => {
                s.download_changes(pro_n, hashes, send, path, full).await?
            }
            RemoteRepo::Http(ref mut h) => {
                h.download_changes(pro_n, hashes, send, path, full).await?
            }
            RemoteRepo::LocalChannel(_) => {}
            RemoteRepo::None => unreachable!(),
        }
        Ok(true)
    }

    pub async fn pull<T: MutTxnTExt + TxnTExt + GraphIter>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        channel: &mut ChannelRef<T>,
        to_apply: &[Hash],
        inodes: &HashSet<Position<Hash>>,
        do_apply: bool,
    ) -> Result<Vec<Hash>, anyhow::Error> {
        let mut pro = PROGRESS.borrow_mut().unwrap();
        let pro_a = pro.push(crate::progress::Cursor::Bar {
            i: 0,
            n: to_apply.len(),
            pre: "Downloading changes".into(),
        });
        let pro_b = if do_apply {
            Some(pro.push(crate::progress::Cursor::Bar {
                i: 0,
                n: to_apply.len(),
                pre: "Applying".into(),
            }))
        } else {
            None
        };
        std::mem::drop(pro);

        let (mut send, mut recv) = tokio::sync::mpsc::channel(100);

        let mut self_ = std::mem::replace(self, RemoteRepo::None);
        let (hash_send, mut hash_recv) = tokio::sync::mpsc::unbounded_channel();
        let mut change_path_ = repo.path.clone();
        change_path_.push(DOT_DIR);
        change_path_.push("changes");
        let t = tokio::spawn(async move {
            self_
                .download_changes(pro_a, &mut hash_recv, &mut send, &mut change_path_, false)
                .await?;
            Ok::<_, anyhow::Error>(self_)
        });

        let mut change_path_ = repo.changes_dir.clone();
        let mut to_download = HashSet::with_capacity(to_apply.len());
        for h in to_apply {
            libpijul::changestore::filesystem::push_filename(&mut change_path_, h);
            if std::fs::metadata(&change_path_).is_err() {
                hash_send.send(*h)?;
                to_download.insert(*h);
            }
            libpijul::changestore::filesystem::pop_filename(&mut change_path_);
        }
        std::mem::drop(hash_send);

        let mut ws = libpijul::ApplyWorkspace::new();
        let mut to_apply_inodes = Vec::new();
        for h in to_apply {
            debug!("to_apply: {:?}", h);
            while to_download.contains(&h) {
                debug!("waiting for {:?}", h);
                if let Some(h) = recv.recv().await {
                    debug!("recv {:?}", h);
                    to_download.remove(&h);
                } else {
                    break;
                }
            }
            let touches_inodes = inodes.is_empty()
                || {
                    debug!("inodes = {:?}", inodes);
                    use libpijul::changestore::ChangeStore;
                    let changes = repo.changes.get_changes(h)?;
                    changes.iter().any(|c| {
                        c.iter().any(|c| {
                            let inode = c.inode();
                            debug!("inode = {:?}", inode);
                            if let Some(h) = inode.change {
                                inodes.contains(&Position {
                                    change: h,
                                    pos: inode.pos,
                                })
                            } else {
                                false
                            }
                        })
                    })
                }
                || { inodes.iter().any(|i| i.change == *h) };

            if touches_inodes {
                to_apply_inodes.push(*h);
            } else {
                continue;
            }

            if let Some(pro_b) = pro_b {
                info!("Applying {:?}", h);
                PROGRESS.inner.lock().unwrap()[pro_b].incr();
                debug!("apply");
                let mut channel = channel.write().unwrap();
                txn.apply_change_ws(&repo.changes, &mut channel, h, &mut ws)?;
                debug!("applied");
            } else {
                debug!("not applying {:?}", h)
            }
        }
        debug!("finished");
        std::mem::drop(recv);
        debug!("waiting for spawned process");
        *self = t.await??;
        debug!("join");
        PROGRESS.join();
        Ok(to_apply_inodes)
    }

    pub async fn clone_tag<T: MutTxnTExt + TxnTExt + GraphIter>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        channel: &mut ChannelRef<T>,
        tag: &[Hash],
    ) -> Result<(), anyhow::Error> {
        let (mut send_signal, mut recv_signal) = tokio::sync::mpsc::channel(100);
        let (send_hash, mut recv_hash) = tokio::sync::mpsc::unbounded_channel();

        let mut change_path_ = repo.changes_dir.clone();
        let mut self_ = std::mem::replace(self, RemoteRepo::None);
        let pro_n = {
            let mut pro = PROGRESS.borrow_mut().unwrap();
            pro.push(crate::progress::Cursor::Bar {
                i: 0,
                n: tag.len(),
                pre: "Downloading changes".into(),
            })
        };

        let t = tokio::spawn(async move {
            self_
                .download_changes(
                    pro_n,
                    &mut recv_hash,
                    &mut send_signal,
                    &mut change_path_,
                    false,
                )
                .await?;
            Ok(self_)
        });

        for &h in tag.iter() {
            send_hash.send(h)?;
        }

        let mut change_path = repo.changes_dir.clone();
        let mut hashes = Vec::new();
        while let Some(hash) = recv_signal.recv().await {
            libpijul::changestore::filesystem::push_filename(&mut change_path, &hash);
            std::fs::create_dir_all(change_path.parent().unwrap())?;
            use libpijul::changestore::ChangeStore;
            hashes.push(hash);
            for dep in repo.changes.get_dependencies(&hash)? {
                let dep: libpijul::pristine::Hash = dep;
                send_hash.send(dep)?;
            }
            libpijul::changestore::filesystem::pop_filename(&mut change_path);
        }
        std::mem::drop(recv_signal);
        std::mem::drop(send_hash);
        let mut ws = libpijul::ApplyWorkspace::new();
        {
            let mut channel_ = channel.write().unwrap();
            for hash in hashes.iter() {
                txn.apply_change_ws(&repo.changes, &mut channel_, hash, &mut ws)?;
            }
        }
        let r: Result<_, anyhow::Error> = t.await?;
        *self = r?;
        self.complete_changes(repo, txn, channel, &hashes, false)
            .await?;
        Ok(())
    }

    pub async fn clone_state<T: MutTxnTExt + TxnTExt + GraphIter>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        channel: &mut ChannelRef<T>,
        state: Merkle,
    ) -> Result<(), anyhow::Error> {
        self.update_changelist(txn, &[]).await?;
        let name = self.name().unwrap();
        let remote = txn.open_or_create_remote(name).unwrap();
        let mut to_pull = Vec::new();
        let mut found = false;
        for x in txn.iter_remote(&remote.lock()?.remote, 0)? {
            let (n, p) = x?;
            debug!("{:?} {:?}", n, p);
            to_pull.push(p.a.into());
            if p.b == state {
                found = true;
                break;
            }
        }
        if !found {
            bail!("State not found: {:?}", state)
        }
        self.pull(repo, txn, channel, &to_pull, &HashSet::new(), true)
            .await?;
        self.complete_changes(repo, txn, channel, &to_pull, false)
            .await?;
        Ok(())
    }

    pub async fn complete_changes<T: MutTxnT + TxnTExt + GraphIter>(
        &mut self,
        repo: &crate::repository::Repository,
        txn: &T,
        local_channel: &mut ChannelRef<T>,
        changes: &[Hash],
        full: bool,
    ) -> Result<(), anyhow::Error> {
        debug!("complete changes {:?}", changes);
        use libpijul::changestore::ChangeStore;
        let (send_hash, mut recv_hash) = tokio::sync::mpsc::unbounded_channel();
        let (mut send_sig, mut recv_sig) = tokio::sync::mpsc::channel(100);
        let mut self_ = std::mem::replace(self, RemoteRepo::None);
        let mut changes_dir = repo.changes_dir.clone();
        let mut progress = PROGRESS.borrow_mut().unwrap();
        let pro_n = {
            progress.push(crate::progress::Cursor::Bar {
                i: 0,
                n: 0,
                pre: "Completing changes".into(),
            })
        };
        std::mem::drop(progress);
        let t = tokio::spawn(async move {
            self_
                .download_changes(pro_n, &mut recv_hash, &mut send_sig, &mut changes_dir, true)
                .await?;
            Ok::<_, anyhow::Error>(self_)
        });

        for c in changes {
            let sc = c.into();
            if repo
                .changes
                .has_contents(*c, txn.get_internal(&sc)?.cloned())
            {
                debug!("has contents {:?}", c);
                continue;
            }
            if full {
                debug!("sending send_hash");
                send_hash.send(*c)?;
                PROGRESS.borrow_mut().unwrap()[pro_n].incr_len();
                debug!("sent");
                continue;
            }
            let change = if let Some(&i) = txn.get_internal(&sc)? {
                i
            } else {
                debug!("could not find internal for {:?}", sc);
                continue;
            };
            // Check if at least one non-empty vertex from c is still alive.
            let v = libpijul::pristine::Vertex {
                change,
                start: libpijul::pristine::ChangePosition(0u64.into()),
                end: libpijul::pristine::ChangePosition(0u64.into()),
            };
            let channel = local_channel.read()?;
            let graph = txn.graph(&channel);
            let mut it = txn.iter_graph(graph, Some(&v))?;
            while let Some(x) = txn.next_graph(&graph, &mut it) {
                let (v, e) = x?;
                if v.change > change {
                    break;
                } else if e.flag().is_alive_parent() {
                    send_hash.send(*c)?;
                    PROGRESS.borrow_mut().unwrap()[pro_n].incr_len();
                    break;
                }
            }
        }
        debug!("dropping send_hash");
        std::mem::drop(send_hash);
        while recv_sig.recv().await.is_some() {}
        *self = t.await??;
        PROGRESS.join();
        Ok(())
    }

    pub async fn clone_channel<T: MutTxnTExt + TxnTExt + GraphIter>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        local_channel: &mut ChannelRef<T>,
        path: &[String],
    ) -> Result<(), anyhow::Error> {
        let (inodes, remote_changes) = self
            .update_changelist(txn, path)
            .await?
            .expect("Remote is not self");
        let mut pullable = Vec::new();
        for x in txn.iter_remote(&remote_changes.lock()?.remote, 0)? {
            let (_, p) = x?;
            pullable.push(p.a.into())
        }
        self.pull(repo, txn, local_channel, &pullable, &inodes, true)
            .await?;
        self.complete_changes(repo, txn, local_channel, &pullable, false)
            .await?;
        Ok(())
    }
}

use libpijul::pristine::{ChangePosition, Position};
use regex::Regex;

lazy_static! {
    static ref CHANGELIST_LINE: Regex =
        Regex::new(r#"(?P<num>[0-9]+)\.(?P<hash>[A-Za-z0-9]+)\.(?P<merkle>[A-Za-z0-9]+)"#).unwrap();
    static ref PATHS_LINE: Regex =
        Regex::new(r#"(?P<hash>[A-Za-z0-9]+)\.(?P<num>[0-9]+)"#).unwrap();
}

enum ListLine {
    Change { n: u64, h: Hash, m: Merkle },
    Position(Position<Hash>),
    Error(String),
}

fn parse_line(data: &str) -> Result<ListLine, anyhow::Error> {
    debug!("data = {:?}", data);
    if let Some(caps) = CHANGELIST_LINE.captures(data) {
        if let (Some(h), Some(m)) = (
            Hash::from_base32(caps.name("hash").unwrap().as_str().as_bytes()),
            Merkle::from_base32(caps.name("merkle").unwrap().as_str().as_bytes()),
        ) {
            return Ok(ListLine::Change {
                n: caps.name("num").unwrap().as_str().parse().unwrap(),
                h,
                m,
            });
        }
    }
    if data.starts_with("error:") {
        return Ok(ListLine::Error(data.split_at(6).1.to_string()));
    }
    if let Some(caps) = PATHS_LINE.captures(data) {
        return Ok(ListLine::Position(Position {
            change: Hash::from_base32(caps.name("hash").unwrap().as_str().as_bytes()).unwrap(),
            pos: ChangePosition(
                caps.name("num")
                    .unwrap()
                    .as_str()
                    .parse::<u64>()
                    .unwrap()
                    .into(),
            ),
        }));
    }
    debug!("offending line: {:?}", data);
    bail!("Protocol error")
}
