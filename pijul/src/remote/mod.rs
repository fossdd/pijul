use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use lazy_static::lazy_static;
use libpijul::pristine::{
    sanakirja::MutTxn, Base32, ChangeId, ChannelRef, GraphIter, Hash, Merkle, MutTxnT, RemoteRef,
    TxnT,
};
use libpijul::DOT_DIR;
use libpijul::{ChannelTxnT, DepsTxnT, GraphTxnT, MutTxnTExt, TxnTExt};
use log::{debug, info};

use crate::config::*;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CS {
    Change(Hash),
    State(Merkle),
}

impl Repository {
    pub async fn remote(
        &self,
        self_path: Option<&Path>,
        name: &str,
        channel: &str,
        direction: Direction,
        no_cert_check: bool,
        with_path: bool,
    ) -> Result<RemoteRepo, anyhow::Error> {
        if let Some(name) = self.config.remotes.get(name) {
            unknown_remote(
                self_path,
                name.with_dir(direction),
                channel,
                no_cert_check,
                with_path,
            )
            .await
        } else {
            unknown_remote(self_path, name, channel, no_cert_check, with_path).await
        }
    }
}

pub async fn unknown_remote(
    self_path: Option<&Path>,
    name: &str,
    channel: &str,
    no_cert_check: bool,
    with_path: bool,
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
            if let Some(mut ssh) = ssh_remote(name, with_path) {
                debug!("unknown_remote, ssh = {:?}", ssh);
                if let Some(c) = ssh.connect(name, channel).await? {
                    return Ok(RemoteRepo::Ssh(c));
                }
            }
            bail!("Remote not found: {:?}", name)
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
        match libpijul::pristine::sanakirja::Pristine::new(&dot_dir.join("db")) {
            Ok(pristine) => {
                debug!("pristine done");
                return Ok(RemoteRepo::Local(Local {
                    root: Path::new(name).to_path_buf(),
                    channel: channel.to_string(),
                    changes_dir,
                    pristine: Arc::new(pristine),
                    name: name.to_string(),
                }));
            }
            Err(libpijul::pristine::sanakirja::SanakirjaError::Sanakirja(
                sanakirja::Error::IO(e),
            )) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("repo not found")
            }
            Err(e) => return Err(e.into()),
        }
    }
    if let Some(mut ssh) = ssh_remote(name, with_path) {
        debug!("unknown_remote, ssh = {:?}", ssh);
        if let Some(c) = ssh.connect(name, channel).await? {
            return Ok(RemoteRepo::Ssh(c));
        }
    }
    bail!("Remote not found: {:?}", name)
}

// Extracting this saves a little bit of duplication.
fn get_local_inodes(
    txn: &mut MutTxn<()>,
    channel: &ChannelRef<MutTxn<()>>,
    repo: &Repository,
    path: &[String],
) -> Result<HashSet<Position<ChangeId>>, anyhow::Error> {
    let mut paths = HashSet::new();
    for path in path.iter() {
        let (p, ambiguous) = txn.follow_oldest_path(&repo.changes, &channel, path)?;
        if ambiguous {
            bail!("Ambiguous path: {:?}", path)
        }
        paths.insert(p);
        paths.extend(
            libpijul::fs::iter_graph_descendants(txn, &channel.read(), p)?.map(|x| x.unwrap()),
        );
    }
    Ok(paths)
}

/// Embellished [`RemoteDelta`] that has information specific
/// to a push operation. We want to know what our options are
/// for changes to upload, whether the remote has unrecorded relevant changes,
/// and whether the remote has changes we don't know about, since those might
/// effect whether or not we actually want to go through with the push.
pub(crate) struct PushDelta {
    pub to_upload: Vec<CS>,
    pub remote_unrecs: Vec<(u64, CS)>,
    pub unknown_changes: Vec<CS>,
}

/// For a [`RemoteRepo`] that's Local, Ssh, or Http
/// (anything other than a LocalChannel),
/// [`RemoteDelta`] contains data about the difference between
/// the "actual" state of the remote ('theirs') and the last version of it
/// that we cached ('ours'). The dichotomy is the last point at which the two
/// were the same. `remote_unrecs` is a list of changes which used to be
/// present in the remote, AND were present in the current channel we're
/// pulling to or pushing from. The significance of that is that if we knew
/// about a certain change but did not pull it, the user won't be notified
/// if it's unrecorded in the remote.
///
/// If the remote we're pulling from or pushing to is a LocalChannel,
/// (meaning it's just a different channel of the repo we're already in), then
/// `ours_ge_dichotomy`, `theirs_ge_dichotomy`, and `remote_unrecs` will be empty
/// since they have no meaning. If we're pulling from a LocalChannel,
/// there's no cache to have diverged from, and if we're pushing to a LocalChannel,
/// we're not going to suddenly be surprised by the presence of unknown changes.
///
/// This struct will be created by both a push and pull operation since both
/// need to update the changelist and will at least try to update the local
/// remote cache. For a push, this later gets turned into [`PushDelta`].
pub(crate) struct RemoteDelta<T: MutTxnTExt + TxnTExt> {
    pub inodes: HashSet<Position<Hash>>,
    pub to_download: Vec<CS>,
    pub remote_ref: Option<RemoteRef<T>>,
    pub ours_ge_dichotomy_set: HashSet<CS>,
    pub theirs_ge_dichotomy_set: HashSet<CS>,
    // Keep the Vec representation around as well so that notification
    // for unknown changes during shows the hashes in order.
    pub theirs_ge_dichotomy: Vec<(u64, Hash, Merkle, bool)>,
    pub remote_unrecs: Vec<(u64, CS)>,
}

impl RemoteDelta<MutTxn<()>> {
    /// Make a [`PushDelta`] from a [`RemoteDelta`]
    /// when the remote is a [`RemoteRepo::LocalChannel`].
    pub(crate) fn to_local_channel_push(
        self,
        remote_channel: &str,
        txn: &mut MutTxn<()>,
        path: &[String],
        channel: &ChannelRef<MutTxn<()>>,
        repo: &Repository,
    ) -> Result<PushDelta, anyhow::Error> {
        let mut to_upload = Vec::new();
        let inodes = get_local_inodes(txn, channel, repo, path)?;

        for x in txn.reverse_log(&*channel.read(), None)? {
            let (_, (h, _)) = x?;
            if let Some(channel) = txn.load_channel(remote_channel)? {
                let channel = channel.read();
                let h_int = txn.get_internal(h)?.unwrap();
                if txn.get_changeset(txn.changes(&channel), h_int)?.is_none() {
                    if inodes.is_empty() {
                        to_upload.push(CS::Change(h.into()))
                    } else {
                        for p in inodes.iter() {
                            if txn.get_touched_files(p, Some(h_int))?.is_some() {
                                to_upload.push(CS::Change(h.into()));
                                break;
                            }
                        }
                    }
                }
            }
        }
        assert!(self.ours_ge_dichotomy_set.is_empty());
        assert!(self.theirs_ge_dichotomy_set.is_empty());
        let d = PushDelta {
            to_upload: to_upload.into_iter().rev().collect(),
            remote_unrecs: self.remote_unrecs,
            unknown_changes: Vec::new(),
        };
        assert!(d.remote_unrecs.is_empty());
        Ok(d)
    }

    /// Make a [`PushDelta`] from a [`RemoteDelta`] when the remote
    /// is not a LocalChannel.
    pub(crate) fn to_remote_push(
        self,
        txn: &mut MutTxn<()>,
        path: &[String],
        channel: &ChannelRef<MutTxn<()>>,
        repo: &Repository,
    ) -> Result<PushDelta, anyhow::Error> {
        let mut to_upload = Vec::new();
        let inodes = get_local_inodes(txn, channel, repo, path)?;
        if let Some(ref remote_ref) = self.remote_ref {
            let mut tags: HashSet<Merkle> = HashSet::new();
            for x in txn.rev_iter_tags(&channel.read().tags, None)? {
                let (n, m) = x?;
                debug!("rev_iter_tags {:?} {:?}", n, m);
                // First, if the remote has exactly the same first n tags, break.
                if let Some((_, p)) = txn.get_remote_tag(&remote_ref.lock().tags, (*n).into())? {
                    if p.b == m.b {
                        debug!("the remote has tag {:?}", p.a);
                        break;
                    }
                    if p.a != m.a {
                        // What to do here?  It is possible that state
                        // `n` is a different state than `m.a` in the
                        // remote, and is also tagged.
                    }
                } else {
                    tags.insert(m.a.into());
                }
            }
            debug!("tags = {:?}", tags);
            for x in txn.reverse_log(&*channel.read(), None)? {
                let (_, (h, m)) = x?;
                let h_unrecorded = self
                    .remote_unrecs
                    .iter()
                    .any(|(_, hh)| hh == &CS::Change(h.into()));
                if !h_unrecorded {
                    if txn.remote_has_state(remote_ref, &m)?.is_some() {
                        debug!("remote_has_state: {:?}", m);
                        break;
                    }
                }
                let h_int = txn.get_internal(h)?.unwrap();
                let h_deser = Hash::from(h);
                // For elements that are in the uncached remote changes (theirs_ge_dichotomy),
                // don't put those in to_upload since the remote we're pushing to
                // already has those changes.
                if (!txn.remote_has_change(remote_ref, &h)? || h_unrecorded)
                    && !self.theirs_ge_dichotomy_set.contains(&CS::Change(h_deser))
                {
                    if inodes.is_empty() {
                        if tags.remove(&m.into()) {
                            to_upload.push(CS::State(m.into()));
                        }
                        to_upload.push(CS::Change(h_deser));
                    } else {
                        for p in inodes.iter() {
                            if txn.get_touched_files(p, Some(h_int))?.is_some() {
                                to_upload.push(CS::Change(h_deser));
                                if tags.remove(&m.into()) {
                                    to_upload.push(CS::State(m.into()));
                                }
                                break;
                            }
                        }
                    }
                }
            }
            for t in tags.iter() {
                if let Some(n) = txn.remote_has_state(&remote_ref, &t.into())? {
                    if !txn.is_tagged(&remote_ref.lock().tags, n)? {
                        to_upload.push(CS::State(*t));
                    }
                } else {
                    debug!("the remote doesn't have state {:?}", t);
                }
            }
        }

        // { h | h \in theirs_ge_dichotomy /\ ~(h \in ours_ge_dichotomy) }
        // The set of their changes >= dichotomy that aren't
        // already known to our set of changes after the dichotomy.
        let mut unknown_changes = Vec::new();
        for (_, h, m, is_tag) in self.theirs_ge_dichotomy.iter() {
            let h_is_known = txn.get_revchanges(&channel, h).unwrap().is_some();
            let change = CS::Change(*h);
            if !(self.ours_ge_dichotomy_set.contains(&change) || h_is_known) {
                unknown_changes.push(change)
            }
            if *is_tag {
                let m_is_known = if let Some(n) = txn
                    .channel_has_state(txn.states(&*channel.read()), &m.into())
                    .unwrap()
                {
                    txn.is_tagged(txn.tags(&*channel.read()), n.into()).unwrap()
                } else {
                    false
                };
                if !m_is_known {
                    unknown_changes.push(CS::State(*m))
                }
            }
        }

        Ok(PushDelta {
            to_upload: to_upload.into_iter().rev().collect(),
            remote_unrecs: self.remote_unrecs,
            unknown_changes,
        })
    }
}

/// Create a [`RemoteDelta`] for a [`RemoteRepo::LocalChannel`].
/// Since this case doesn't have a local remote cache to worry about,
/// mainly just calculates the `to_download` list of changes.
pub(crate) fn update_changelist_local_channel(
    remote_channel: &str,
    txn: &mut MutTxn<()>,
    path: &[String],
    current_channel: &ChannelRef<MutTxn<()>>,
    repo: &Repository,
    specific_changes: &[String],
) -> Result<RemoteDelta<MutTxn<()>>, anyhow::Error> {
    if !specific_changes.is_empty() {
        let mut to_download = Vec::new();
        for h in specific_changes {
            let h = txn.hash_from_prefix(h)?.0;
            if txn.get_revchanges(current_channel, &h)?.is_none() {
                to_download.push(CS::Change(h));
            }
        }
        Ok(RemoteDelta {
            inodes: HashSet::new(),
            to_download,
            remote_ref: None,
            ours_ge_dichotomy_set: HashSet::new(),
            theirs_ge_dichotomy: Vec::new(),
            theirs_ge_dichotomy_set: HashSet::new(),
            remote_unrecs: Vec::new(),
        })
    } else {
        let mut inodes = HashSet::new();
        let inodes_ = get_local_inodes(txn, current_channel, repo, path)?;
        let mut to_download = Vec::new();
        inodes.extend(inodes_.iter().map(|x| libpijul::pristine::Position {
            change: txn.get_external(&x.change).unwrap().unwrap().into(),
            pos: x.pos,
        }));
        if let Some(remote_channel) = txn.load_channel(remote_channel)? {
            let remote_channel = remote_channel.read();
            for x in txn.reverse_log(&remote_channel, None)? {
                let (_, (h, m)) = x?;
                if txn
                    .channel_has_state(txn.states(&*current_channel.read()), &m)?
                    .is_some()
                {
                    break;
                }
                let h_int = txn.get_internal(h)?.unwrap();
                if txn
                    .get_changeset(txn.changes(&*current_channel.read()), h_int)?
                    .is_none()
                {
                    if inodes_.is_empty()
                        || inodes_.iter().any(|&inode| {
                            txn.get_rev_touched_files(h_int, Some(&inode))
                                .unwrap()
                                .is_some()
                        })
                    {
                        to_download.push(CS::Change(h.into()));
                    }
                }
            }
        }
        Ok(RemoteDelta {
            inodes,
            to_download,
            remote_ref: None,
            ours_ge_dichotomy_set: HashSet::new(),
            theirs_ge_dichotomy: Vec::new(),
            theirs_ge_dichotomy_set: HashSet::new(),
            remote_unrecs: Vec::new(),
        })
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

    pub async fn update_changelist<T: MutTxnTExt + TxnTExt + 'static>(
        &mut self,
        txn: &mut T,
        path: &[String],
    ) -> Result<Option<(HashSet<Position<Hash>>, RemoteRef<T>)>, anyhow::Error> {
        debug!("update_changelist");
        let id = if let Some(id) = self.get_id(txn).await? {
            id
        } else {
            return Ok(None);
        };
        let mut remote = if let Some(name) = self.name() {
            txn.open_or_create_remote(id, name)?
        } else {
            return Ok(None);
        };
        let n = self.dichotomy_changelist(txn, &remote.lock()).await?;
        debug!("update changelist {:?}", n);
        let v: Vec<_> = txn
            .iter_remote(&remote.lock().remote, n)?
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
        let v: Vec<_> = txn
            .iter_tags(&remote.lock().tags, n)?
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
            txn.del_tags(&mut remote.lock().tags, k)?;
        }

        debug!("deleted");
        let paths = self.download_changelist(txn, &mut remote, n, path).await?;
        Ok(Some((paths, remote)))
    }

    async fn update_changelist_pushpull_from_scratch(
        &mut self,
        txn: &mut MutTxn<()>,
        path: &[String],
        current_channel: &ChannelRef<MutTxn<()>>,
    ) -> Result<RemoteDelta<MutTxn<()>>, anyhow::Error> {
        debug!("no id, starting from scratch");
        let (inodes, theirs_ge_dichotomy) = self.download_changelist_nocache(0, path).await?;
        let mut theirs_ge_dichotomy_set = HashSet::new();
        let mut to_download = Vec::new();
        for (_, h, m, is_tag) in theirs_ge_dichotomy.iter() {
            theirs_ge_dichotomy_set.insert(CS::Change(*h));
            if txn.get_revchanges(current_channel, h)?.is_none() {
                to_download.push(CS::Change(*h));
            }
            if *is_tag {
                let ch = current_channel.read();
                if let Some(n) = txn.channel_has_state(txn.states(&*ch), &m.into())? {
                    if !txn.is_tagged(txn.tags(&*ch), n.into())? {
                        to_download.push(CS::State(*m));
                    }
                } else {
                    to_download.push(CS::State(*m));
                }
            }
        }
        Ok(RemoteDelta {
            inodes,
            remote_ref: None,
            to_download,
            ours_ge_dichotomy_set: HashSet::new(),
            theirs_ge_dichotomy,
            theirs_ge_dichotomy_set,
            remote_unrecs: Vec::new(),
        })
    }

    /// Creates a [`RemoteDelta`].
    ///
    /// IF:
    ///    the RemoteRepo is a [`RemoteRepo::LocalChannel`], delegate to
    ///    the simpler method [`update_changelist_local_channel`], returning the
    ///    `to_download` list of changes.
    ///
    /// ELSE:
    ///    calculate the `to_download` list of changes. Additionally, if there are
    ///    no remote unrecords, update the local remote cache. If there are remote unrecords,
    ///    calculate and return information about the difference between our cached version
    ///    of the remote, and their version of the remote.
    pub(crate) async fn update_changelist_pushpull(
        &mut self,
        txn: &mut MutTxn<()>,
        path: &[String],
        current_channel: &ChannelRef<MutTxn<()>>,
        force_cache: Option<bool>,
        repo: &Repository,
        specific_changes: &[String],
        is_pull: bool,
    ) -> Result<RemoteDelta<MutTxn<()>>, anyhow::Error> {
        debug!("update_changelist_pushpull");
        if let RemoteRepo::LocalChannel(c) = self {
            return update_changelist_local_channel(
                c,
                txn,
                path,
                current_channel,
                repo,
                specific_changes,
            );
        }

        let id = if let Some(id) = self.get_id(txn).await? {
            debug!("id = {:?}", id);
            id
        } else {
            return self
                .update_changelist_pushpull_from_scratch(txn, path, current_channel)
                .await;
        };
        let mut remote_ref = txn.open_or_create_remote(id, self.name().unwrap()).unwrap();
        let dichotomy_n = self.dichotomy_changelist(txn, &remote_ref.lock()).await?;
        let ours_ge_dichotomy: Vec<(u64, CS)> = txn
            .iter_remote(&remote_ref.lock().remote, dichotomy_n)?
            .filter_map(|k| {
                debug!("filter_map {:?}", k);
                match k.unwrap() {
                    (k, libpijul::pristine::Pair { a: hash, .. }) => {
                        let (k, hash) = (u64::from(*k), Hash::from(*hash));
                        if k >= dichotomy_n {
                            Some((k, CS::Change(hash)))
                        } else {
                            None
                        }
                    }
                }
            })
            .collect();
        let (inodes, theirs_ge_dichotomy) =
            self.download_changelist_nocache(dichotomy_n, path).await?;
        debug!("theirs_ge_dichotomy = {:?}", theirs_ge_dichotomy);
        let ours_ge_dichotomy_set = ours_ge_dichotomy
            .iter()
            .map(|(_, h)| h)
            .copied()
            .collect::<HashSet<CS>>();
        let mut theirs_ge_dichotomy_set = HashSet::new();
        for (_, h, m, is_tag) in theirs_ge_dichotomy.iter() {
            theirs_ge_dichotomy_set.insert(CS::Change(*h));
            if *is_tag {
                theirs_ge_dichotomy_set.insert(CS::State(*m));
            }
        }

        // remote_unrecs = {x: (u64, Hash) | x \in ours_ge_dichot /\ ~(x \in theirs_ge_dichot) /\ x \in current_channel }
        let remote_unrecs = remote_unrecs(
            txn,
            current_channel,
            &ours_ge_dichotomy,
            &theirs_ge_dichotomy_set,
        )?;
        let should_cache = if let Some(true) = force_cache {
            true
        } else {
            remote_unrecs.is_empty()
        };
        debug!(
            "should_cache = {:?} {:?} {:?}",
            force_cache, remote_unrecs, should_cache
        );
        if should_cache {
            use libpijul::ChannelMutTxnT;
            for (k, t) in ours_ge_dichotomy.iter().copied() {
                match t {
                    CS::State(_) => txn.del_tags(&mut remote_ref.lock().tags, k)?,
                    CS::Change(_) => {
                        txn.del_remote(&mut remote_ref, k)?;
                    }
                }
            }
            for (n, h, m, is_tag) in theirs_ge_dichotomy.iter().copied() {
                debug!("theirs: {:?} {:?} {:?}", n, h, m);
                txn.put_remote(&mut remote_ref, n, (h, m))?;
                if is_tag {
                    txn.put_tags(&mut remote_ref.lock().tags, n, &m)?;
                }
            }
        }
        if !specific_changes.is_empty() {
            // Here, the user only wanted to push/pull specific changes
            let to_download = specific_changes
                .iter()
                .map(|h| {
                    if is_pull {
                        {
                            if let Ok(t) = txn.state_from_prefix(&remote_ref.lock().states, h) {
                                return Ok(CS::State(t.0));
                            }
                        }
                        Ok(CS::Change(txn.hash_from_prefix_remote(&remote_ref, h)?))
                    } else {
                        if let Ok(t) = txn.state_from_prefix(&current_channel.read().states, h) {
                            Ok(CS::State(t.0))
                        } else {
                            Ok(CS::Change(txn.hash_from_prefix(h)?.0))
                        }
                    }
                })
                .collect::<Result<Vec<_>, anyhow::Error>>();
            Ok(RemoteDelta {
                inodes,
                remote_ref: Some(remote_ref),
                to_download: to_download?,
                ours_ge_dichotomy_set,
                theirs_ge_dichotomy,
                theirs_ge_dichotomy_set,
                remote_unrecs,
            })
        } else {
            let mut to_download: Vec<CS> = Vec::new();
            let mut to_download_ = HashSet::new();
            for x in txn.iter_rev_remote(&remote_ref.lock().remote, None)? {
                let (_, p) = x?;
                let h: Hash = p.a.into();
                if txn
                    .channel_has_state(txn.states(&current_channel.read()), &p.b)
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                if txn.get_revchanges(&current_channel, &h).unwrap().is_none() {
                    let h = CS::Change(h);
                    if to_download_.insert(h.clone()) {
                        to_download.push(h);
                    }
                }
            }

            // The patches in theirs_ge_dichotomy are unknown to us,
            // download them.
            for (n, h, m, is_tag) in theirs_ge_dichotomy.iter() {
                // In all cases, add this new change/state/tag to `to_download`.
                let ch = CS::Change(*h);
                if txn.get_revchanges(&current_channel, h).unwrap().is_none() {
                    if to_download_.insert(ch.clone()) {
                        to_download.push(ch.clone());
                    }
                    if *is_tag {
                        to_download.push(CS::State(*m));
                    }
                } else if *is_tag {
                    let has_tag = if let Some(n) =
                        txn.channel_has_state(txn.states(&current_channel.read()), &m.into())?
                    {
                        txn.is_tagged(txn.tags(&current_channel.read()), n.into())?
                    } else {
                        false
                    };
                    if !has_tag {
                        to_download.push(CS::State(*m));
                    }
                }
                // Additionally, if there are no remote unrecords
                // (i.e. if `should_cache`), cache.
                if should_cache && ours_ge_dichotomy_set.get(&ch).is_none() {
                    use libpijul::ChannelMutTxnT;
                    txn.put_remote(&mut remote_ref, *n, (*h, *m))?;
                    if *is_tag {
                        let mut rem = remote_ref.lock();
                        txn.put_tags(&mut rem.tags, *n, m)?;
                    }
                }
            }
            Ok(RemoteDelta {
                inodes,
                remote_ref: Some(remote_ref),
                to_download,
                ours_ge_dichotomy_set,
                theirs_ge_dichotomy,
                theirs_ge_dichotomy_set,
                remote_unrecs,
            })
        }
    }

    /// Get the list of the remote's changes that come after `from: u64`.
    /// Instead of immediately updating the local cache of the remote, return
    /// the change info without changing the cache.
    pub async fn download_changelist_nocache(
        &mut self,
        from: u64,
        paths: &[String],
    ) -> Result<(HashSet<Position<Hash>>, Vec<(u64, Hash, Merkle, bool)>), anyhow::Error> {
        let mut v = Vec::new();
        let f = |v: &mut Vec<(u64, Hash, Merkle, bool)>, n, h, m, m2| {
            debug!("no cache: {:?}", h);
            Ok(v.push((n, h, m, m2)))
        };
        let r = match *self {
            RemoteRepo::Local(ref mut l) => l.download_changelist(f, &mut v, from, paths)?,
            RemoteRepo::Ssh(ref mut s) => s.download_changelist(f, &mut v, from, paths).await?,
            RemoteRepo::Http(ref h) => h.download_changelist(f, &mut v, from, paths).await?,
            RemoteRepo::LocalChannel(_) => HashSet::new(),
            RemoteRepo::None => unreachable!(),
        };
        Ok((r, v))
    }

    /// Uses a binary search to find the integer identifier of the last point
    /// at which our locally cached version of the remote was the same as the 'actual'
    /// state of the remote.
    async fn dichotomy_changelist<T: MutTxnT + TxnTExt>(
        &mut self,
        txn: &T,
        remote: &libpijul::pristine::Remote<T>,
    ) -> Result<u64, anyhow::Error> {
        let mut a = 0;
        let (mut b, state): (_, Merkle) = if let Some((u, v)) = txn.last_remote(&remote.remote)? {
            debug!("dichotomy_changelist: {:?} {:?}", u, v);
            (u, (&v.b).into())
        } else {
            debug!("the local copy of the remote has no changes");
            return Ok(0);
        };
        let last_statet = if let Some((_, _, v)) = txn.last_remote_tag(&remote.tags)? {
            v.into()
        } else {
            Merkle::zero()
        };
        debug!("last_state: {:?} {:?}", state, last_statet);
        if let Some((_, s, st)) = self.get_state(txn, Some(b)).await? {
            debug!("remote last_state: {:?} {:?}", s, st);
            if s == state && st == last_statet {
                // The local list is already up to date.
                return Ok(b + 1);
            }
        }
        // Else, find the last state we have in common with the
        // remote, it might be older than the last known state (if
        // changes were unrecorded on the remote).
        while a < b {
            let mid = (a + b) / 2;
            let (mid, state) = {
                let (a, b) = txn.get_remote_state(&remote.remote, mid)?.unwrap();
                (a, b.b)
            };
            let statet = if let Some((_, b)) = txn.get_remote_tag(&remote.tags, mid)? {
                // There's still a tag at position >= mid in the
                // sequence.
                b.b.into()
            } else {
                // No tag at or after mid, the last state, `statet`,
                // is the right answer in that case.
                last_statet
            };

            let remote_state = self.get_state(txn, Some(mid)).await?;
            debug!("dichotomy {:?} {:?} {:?}", mid, state, remote_state);
            if let Some((_, remote_state, remote_statet)) = remote_state {
                if remote_state == state && remote_statet == statet {
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
    ) -> Result<Option<(u64, Merkle, Merkle)>, anyhow::Error> {
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

    /// This method might return `Ok(None)` in some cases, for example
    /// if the remote wants to indicate not to store a cache. This is
    /// the case for Nest channels, for example.
    async fn get_id<T: libpijul::TxnTExt + 'static>(
        &mut self,
        txn: &T,
    ) -> Result<Option<libpijul::pristine::RemoteId>, anyhow::Error> {
        match *self {
            RemoteRepo::Local(ref l) => Ok(Some(l.get_id()?)),
            RemoteRepo::Ssh(ref mut s) => s.get_id().await,
            RemoteRepo::Http(ref h) => h.get_id().await,
            RemoteRepo::LocalChannel(ref channel) => {
                if let Some(channel) = txn.load_channel(&channel)? {
                    Ok(txn.id(&*channel.read()).cloned())
                } else {
                    Err(anyhow::anyhow!(
                        "Unable to retrieve RemoteId for LocalChannel remote"
                    ))
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
                let changes = libpijul::changestore::filesystem::FileSystem::from_root(
                    &l.root,
                    crate::repository::max_files(),
                );
                let mut tarball = libpijul::output::Tarball::new(w, prefix, umask);
                let conflicts = if let Some((state, extra)) = state {
                    let txn = l.pristine.arc_txn_begin()?;
                    let channel = {
                        let txn = txn.read();
                        txn.load_channel(&l.channel)?.unwrap()
                    };
                    txn.archive_with_state(&changes, &channel, &state, extra, &mut tarball, 0)?
                } else {
                    let txn = l.pristine.arc_txn_begin()?;
                    let channel = {
                        let txn = txn.read();
                        txn.load_channel(&l.channel)?.unwrap()
                    };
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
        let f = |a: &mut (&mut T, &mut RemoteRef<T>), n, h, m, is_tag| {
            let (ref mut txn, ref mut remote) = *a;
            txn.put_remote(remote, n, (h, m))?;
            if is_tag {
                txn.put_tags(&mut remote.lock().tags, n, &m.into())?;
            }
            Ok(())
        };
        match *self {
            RemoteRepo::Local(ref mut l) => {
                l.download_changelist(f, &mut (txn, remote), from, paths)
            }
            RemoteRepo::Ssh(ref mut s) => {
                s.download_changelist(f, &mut (txn, remote), from, paths)
                    .await
            }
            RemoteRepo::Http(ref h) => {
                h.download_changelist(f, &mut (txn, remote), from, paths)
                    .await
            }
            RemoteRepo::LocalChannel(_) => Ok(HashSet::new()),
            RemoteRepo::None => unreachable!(),
        }
    }

    pub async fn upload_changes<T: MutTxnTExt + 'static>(
        &mut self,
        txn: &mut T,
        local: PathBuf,
        to_channel: Option<&str>,
        changes: &[CS],
    ) -> Result<(), anyhow::Error> {
        let pro_n = {
            let mut pro = PROGRESS.borrow_mut().unwrap();
            pro.push(crate::progress::Cursor::Bar {
                i: 0,
                n: changes.len(),
                pre: "Uploading changes".into(),
            })
        };

        match self {
            RemoteRepo::Local(ref mut l) => l.upload_changes(pro_n, local, to_channel, changes)?,
            RemoteRepo::Ssh(ref mut s) => {
                s.upload_changes(pro_n, local, to_channel, changes).await?
            }
            RemoteRepo::Http(ref h) => h.upload_changes(pro_n, local, to_channel, changes).await?,
            RemoteRepo::LocalChannel(ref channel) => {
                let mut channel = txn.open_or_create_channel(channel)?;
                let store = libpijul::changestore::filesystem::FileSystem::from_changes(
                    local,
                    crate::repository::max_files(),
                );
                local::upload_changes(pro_n, &store, txn, &mut channel, changes)?
            }
            RemoteRepo::None => unreachable!(),
        }
        PROGRESS.join();
        Ok(())
    }

    /// Start (and possibly complete) the download of a change.
    pub async fn download_changes(
        &mut self,
        pro_n: usize,
        hashes: &mut tokio::sync::mpsc::UnboundedReceiver<CS>,
        send: &mut tokio::sync::mpsc::Sender<CS>,
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

    pub async fn update_identities<T: MutTxnTExt + TxnTExt + GraphIter>(
        &mut self,
        repo: &mut Repository,
        remote: &RemoteRef<T>,
    ) -> Result<(), anyhow::Error> {
        debug!("Downloading identities");
        let mut id_path = repo.path.clone();
        id_path.push(DOT_DIR);
        id_path.push("identities");
        let rev = None;
        let r = match *self {
            RemoteRepo::Local(ref mut l) => l.update_identities(rev, id_path).await?,
            RemoteRepo::Ssh(ref mut s) => s.update_identities(rev, id_path).await?,
            RemoteRepo::Http(ref mut h) => h.update_identities(rev, id_path).await?,
            RemoteRepo::LocalChannel(_) => 0,
            RemoteRepo::None => unreachable!(),
        };
        remote.set_id_revision(r);
        Ok(())
    }

    pub async fn pull<T: MutTxnTExt + TxnTExt + GraphIter + 'static>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        channel: &mut ChannelRef<T>,
        to_apply: &[CS],
        inodes: &HashSet<Position<Hash>>,
        do_apply: bool,
    ) -> Result<Vec<CS>, anyhow::Error> {
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
            match h {
                CS::Change(h) => {
                    libpijul::changestore::filesystem::push_filename(&mut change_path_, h);
                }
                CS::State(h) => {
                    libpijul::changestore::filesystem::push_tag_filename(&mut change_path_, h);
                }
            }
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
                    if let CS::Change(h) = h {
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
                    } else {
                        false
                    }
                }
                || { inodes.iter().any(|i| CS::Change(i.change) == *h) };

            if touches_inodes {
                to_apply_inodes.push(*h);
            } else {
                continue;
            }

            if let Some(pro_b) = pro_b {
                info!("Applying {:?}", h);
                PROGRESS.inner.lock().unwrap()[pro_b].incr();
                debug!("apply");
                if let CS::Change(h) = h {
                    let mut channel = channel.write();
                    txn.apply_change_ws(&repo.changes, &mut channel, h, &mut ws)?;
                }
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

    pub async fn clone_tag<T: MutTxnTExt + TxnTExt + GraphIter + 'static>(
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
            send_hash.send(CS::Change(h))?;
        }

        let mut change_path = repo.changes_dir.clone();
        let mut hashes = Vec::new();
        while let Some(hash) = recv_signal.recv().await {
            if let CS::Change(hash) = hash {
                libpijul::changestore::filesystem::push_filename(&mut change_path, &hash);
                std::fs::create_dir_all(change_path.parent().unwrap())?;
                use libpijul::changestore::ChangeStore;
                hashes.push(CS::Change(hash));
                for dep in repo.changes.get_dependencies(&hash)? {
                    let dep: libpijul::pristine::Hash = dep;
                    send_hash.send(CS::Change(dep))?;
                }
                libpijul::changestore::filesystem::pop_filename(&mut change_path);
            }
        }
        std::mem::drop(recv_signal);
        std::mem::drop(send_hash);
        let mut ws = libpijul::ApplyWorkspace::new();
        {
            let mut channel_ = channel.write();
            for hash in hashes.iter() {
                if let CS::Change(hash) = hash {
                    txn.apply_change_ws(&repo.changes, &mut channel_, hash, &mut ws)?;
                }
            }
        }
        let r: Result<_, anyhow::Error> = t.await?;
        *self = r?;
        self.complete_changes(repo, txn, channel, &hashes, false)
            .await?;
        Ok(())
    }

    pub async fn clone_state<T: MutTxnTExt + TxnTExt + GraphIter + 'static>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        channel: &mut ChannelRef<T>,
        state: Merkle,
    ) -> Result<(), anyhow::Error> {
        let id = if let Some(id) = self.get_id(txn).await? {
            id
        } else {
            return Ok(());
        };
        self.update_changelist(txn, &[]).await?;
        let remote = txn.open_or_create_remote(id, self.name().unwrap()).unwrap();
        let mut to_pull = Vec::new();
        let mut found = false;
        for x in txn.iter_remote(&remote.lock().remote, 0)? {
            let (n, p) = x?;
            debug!("{:?} {:?}", n, p);
            to_pull.push(CS::Change(p.a.into()));
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
        self.update_identities(repo, &remote).await?;

        self.complete_changes(repo, txn, channel, &to_pull, false)
            .await?;
        Ok(())
    }

    pub async fn complete_changes<T: MutTxnT + TxnTExt + GraphIter>(
        &mut self,
        repo: &crate::repository::Repository,
        txn: &T,
        local_channel: &mut ChannelRef<T>,
        changes: &[CS],
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
            let c = if let CS::Change(c) = c { c } else { continue };
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
                send_hash.send(CS::Change(*c))?;
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
            let channel = local_channel.read();
            let graph = txn.graph(&channel);
            for x in txn.iter_graph(graph, Some(&v))? {
                let (v, e) = x?;
                if v.change > change {
                    break;
                } else if e.flag().is_alive_parent() {
                    send_hash.send(CS::Change(*c))?;
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

    pub async fn clone_channel<T: MutTxnTExt + TxnTExt + GraphIter + 'static>(
        &mut self,
        repo: &mut Repository,
        txn: &mut T,
        local_channel: &mut ChannelRef<T>,
        path: &[String],
    ) -> Result<(), anyhow::Error> {
        let (inodes, remote_changes) = if let Some(x) = self.update_changelist(txn, path).await? {
            x
        } else {
            bail!("Channel not found")
        };
        let mut pullable = Vec::new();
        {
            let rem = remote_changes.lock();
            for x in txn.iter_remote(&rem.remote, 0)? {
                let (_, p) = x?;
                pullable.push(CS::Change(p.a.into()))
            }
        }
        self.pull(repo, txn, local_channel, &pullable, &inodes, true)
            .await?;
        self.update_identities(repo, &remote_changes).await?;

        self.complete_changes(repo, txn, local_channel, &pullable, false)
            .await?;
        Ok(())
    }
}

use libpijul::pristine::{ChangePosition, Position};
use regex::Regex;

lazy_static! {
    static ref CHANGELIST_LINE: Regex = Regex::new(
        r#"(?P<num>[0-9]+)\.(?P<hash>[A-Za-z0-9]+)\.(?P<merkle>[A-Za-z0-9]+)(?P<tag>\.)?"#
    )
    .unwrap();
    static ref PATHS_LINE: Regex =
        Regex::new(r#"(?P<hash>[A-Za-z0-9]+)\.(?P<num>[0-9]+)"#).unwrap();
}

enum ListLine {
    Change {
        n: u64,
        h: Hash,
        m: Merkle,
        tag: bool,
    },
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
                tag: caps.name("tag").is_some(),
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

/// Compare the remote set (theirs_ge_dichotomy) with our current
/// version of that (ours_ge_dichotomy) and return the changes in our
/// current version that are not in the remote anymore.
fn remote_unrecs<T: TxnTExt + ChannelTxnT>(
    txn: &T,
    current_channel: &ChannelRef<T>,
    ours_ge_dichotomy: &[(u64, CS)],
    theirs_ge_dichotomy_set: &HashSet<CS>,
) -> Result<Vec<(u64, CS)>, anyhow::Error> {
    let mut remote_unrecs = Vec::new();
    for (n, hash) in ours_ge_dichotomy {
        debug!("ours_ge_dichotomy: {:?} {:?}", n, hash);
        if theirs_ge_dichotomy_set.contains(hash) {
            // If this change is still present in the remote, skip
            debug!("still present");
            continue;
        } else {
            let has_it = match hash {
                CS::Change(hash) => txn.get_revchanges(&current_channel, &hash)?.is_some(),
                CS::State(state) => {
                    let ch = current_channel.read();
                    if let Some(n) = txn.channel_has_state(txn.states(&*ch), &state.into())? {
                        txn.is_tagged(txn.tags(&*ch), n.into())?
                    } else {
                        false
                    }
                }
            };
            if has_it {
                remote_unrecs.push((*n, *hash))
            } else {
                // If this unrecord wasn't in our current channel, skip
                continue;
            }
        }
    }
    Ok(remote_unrecs)
}
