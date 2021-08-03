use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use super::{make_changelist, parse_changelist};
use anyhow::bail;
use clap::Clap;
use lazy_static::lazy_static;
use libpijul::changestore::ChangeStore;
use libpijul::pristine::sanakirja::MutTxn;
use libpijul::*;
use log::debug;
use regex::Regex;

use crate::progress::PROGRESS;
use crate::remote::{PushDelta, RemoteDelta, RemoteRepo};
use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Remote {
    #[clap(subcommand)]
    subcmd: Option<SubRemote>,
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
}

#[derive(Clap, Debug)]
pub enum SubRemote {
    /// Deletes the remote
    #[clap(name = "delete")]
    Delete { remote: String },
}

impl Remote {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path)?;
        debug!("{:?}", repo.config);
        let mut stdout = std::io::stdout();
        match self.subcmd {
            None => {
                let txn = repo.pristine.txn_begin()?;
                for r in txn.iter_remotes(&libpijul::pristine::RemoteId::nil())? {
                    let r = r?;
                    writeln!(stdout, "  {}: {}", r.id(), r.lock().path.as_str())?;
                }
            }
            Some(SubRemote::Delete { remote }) => {
                let remote =
                    if let Some(r) = libpijul::pristine::RemoteId::from_base32(remote.as_bytes()) {
                        r
                    } else {
                        bail!("Could not parse identifier: {:?}", remote)
                    };
                let mut txn = repo.pristine.mut_txn_begin()?;
                if !txn.drop_named_remote(remote)? {
                    bail!("Remote not found: {:?}", remote)
                } else {
                    txn.commit()?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clap, Debug)]
pub struct Push {
    /// Path to the repository. Uses the current repository if the argument is omitted
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Push from this channel instead of the default channel
    #[clap(long = "from-channel")]
    from_channel: Option<String>,
    /// Push all changes
    #[clap(long = "all", short = 'a', conflicts_with = "changes")]
    all: bool,
    /// Force an update of the local remote cache. May effect some
    /// reporting of unrecords/concurrent changes in the remote.
    #[clap(long = "force-cache", short = 'f')]
    force_cache: bool,
    /// Do not check certificates (HTTPS remotes only, this option might be dangerous)
    #[clap(short = 'k')]
    no_cert_check: bool,
    /// Push changes only relating to these paths
    #[clap(long = "path")]
    path: Vec<String>,
    /// Push to this remote
    to: Option<String>,
    /// Push to this remote channel instead of the remote's default channel
    #[clap(long = "to-channel")]
    to_channel: Option<String>,
    /// Push only these changes
    #[clap(last = true)]
    changes: Vec<String>,
}

#[derive(Clap, Debug)]
pub struct Pull {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Pull into this channel instead of the current channel
    #[clap(long = "to-channel")]
    to_channel: Option<String>,
    /// Pull all changes
    #[clap(long = "all", short = 'a', conflicts_with = "changes")]
    all: bool,
    /// Force an update of the local remote cache. May effect some
    /// reporting of unrecords/concurrent changes in the remote.
    #[clap(long = "force-cache", short = 'f')]
    force_cache: bool,
    /// Do not check certificates (HTTPS remotes only, this option might be dangerous)
    #[clap(short = 'k')]
    no_cert_check: bool,
    /// Download full changes, even when not necessary
    #[clap(long = "full")]
    full: bool, // This can't be symmetric with push
    /// Only pull to these paths
    #[clap(long = "path")]
    path: Vec<String>,
    /// Pull from this remote
    from: Option<String>,
    /// Pull from this remote channel
    #[clap(long = "from-channel")]
    from_channel: Option<String>,
    /// Pull changes from the local repository, not necessarily from a channel
    #[clap(last = true)]
    changes: Vec<String>, // For local changes only, can't be symmetric.
}

lazy_static! {
    static ref CHANNEL: Regex = Regex::new(r#"([^:]*)(:(.*))?"#).unwrap();
}

impl Push {
    /// Gets the `to_upload` vector while trying to auto-update
    /// the local cache if possible. Also calculates whether the remote
    /// has any changes we don't know about.
    async fn to_upload(
        &self,
        txn: &mut MutTxn<()>,
        channel: &mut ChannelRef<MutTxn<()>>,
        repo: &Repository,
        remote: &mut RemoteRepo,
    ) -> Result<PushDelta<MutTxn<()>>, anyhow::Error> {
        let remote_delta = remote
            .update_changelist_pushpull(
                txn,
                &self.path,
                channel,
                Some(self.force_cache),
                repo,
                self.changes.as_slice(),
            )
            .await?;
        if let RemoteRepo::LocalChannel(ref remote_channel) = remote {
            remote_delta.to_local_channel_push(
                remote_channel,
                txn,
                self.path.as_slice(),
                channel,
                repo,
            )
        } else {
            remote_delta.to_remote_push(txn, self.path.as_slice(), channel, repo)
        }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut stderr = std::io::stderr();
        let repo = Repository::find_root(self.repo_path.clone())?;
        debug!("{:?}", repo.config);
        let txn = repo.pristine.arc_txn_begin()?;
        let cur = txn
            .read()
            .current_channel()
            .unwrap_or(crate::DEFAULT_CHANNEL)
            .to_string();
        let channel_name = if let Some(ref c) = self.from_channel {
            c
        } else {
            cur.as_str()
        };
        let remote_name = if let Some(ref rem) = self.to {
            rem
        } else if let Some(ref def) = repo.config.default_remote {
            def
        } else {
            bail!("Missing remote");
        };
        let mut push_channel = None;
        let remote_channel = if let Some(ref c) = self.to_channel {
            let c = CHANNEL.captures(c).unwrap();
            push_channel = c.get(3).map(|x| x.as_str());
            let c = c.get(1).unwrap().as_str();
            if c.is_empty() {
                channel_name
            } else {
                c
            }
        } else {
            channel_name
        };
        debug!("remote_channel = {:?} {:?}", remote_channel, push_channel);
        let mut remote = repo
            .remote(
                Some(&repo.path),
                &remote_name,
                remote_channel,
                self.no_cert_check,
                true,
            )
            .await?;

        let mut channel = txn.write().open_or_create_channel(&channel_name)?;

        let PushDelta {
            remote_ref,
            to_upload,
            remote_unrecs,
            unknown_changes,
        } = self
            .to_upload(&mut *txn.write(), &mut channel, &repo, &mut remote)
            .await?;

        debug!("to_upload = {:?}", to_upload);

        if to_upload.is_empty() {
            writeln!(stderr, "Nothing to push")?;
            txn.commit()?;
            return Ok(());
        }

        notify_remote_unrecords(&repo, remote_unrecs.as_slice());
        notify_unknown_changes(unknown_changes.as_slice());

        let to_upload = if !self.changes.is_empty() {
            let mut u: Vec<libpijul::Hash> = Vec::new();
            let mut not_found = Vec::new();
            let txn = txn.read();
            for change in self.changes.iter() {
                match txn.hash_from_prefix(change) {
                    Ok((hash, _)) => {
                        if to_upload.contains(&hash) {
                            u.push(hash);
                        }
                    }
                    Err(_) => {
                        if !not_found.contains(change) {
                            not_found.push(change.to_string());
                        }
                    }
                }
            }

            if !not_found.is_empty() {
                bail!("Changes not found: {:?}", not_found)
            }

            check_deps(&repo.changes, &to_upload, &u)?;
            u
        } else if self.all {
            to_upload
        } else {
            let mut o = make_changelist(&repo.changes, &to_upload, "push")?;
            loop {
                let d = parse_changelist(&edit::edit_bytes(&o[..])?);
                let comp = complete_deps(&*txn.read(), &remote_ref, &repo.changes, &to_upload, &d)?;
                if comp.len() == d.len() {
                    break comp;
                }
                o = make_changelist(&repo.changes, &comp, "push")?
            }
        };
        debug!("to_upload = {:?}", to_upload);

        if to_upload.is_empty() {
            writeln!(stderr, "Nothing to push")?;
            txn.commit()?;
            return Ok(());
        }

        remote
            .upload_changes(
                &mut *txn.write(),
                repo.changes_dir.clone(),
                push_channel,
                &to_upload,
            )
            .await?;
        txn.commit()?;

        remote.finish().await?;
        Ok(())
    }
}

impl Pull {
    /// Gets the `to_download` vec and calculates any remote unrecords.
    /// If the local remote cache can be auto-updated, it will be.
    async fn to_download(
        &self,
        txn: &mut MutTxn<()>,
        channel: &mut ChannelRef<MutTxn<()>>,
        repo: &mut Repository,
        remote: &mut RemoteRepo,
    ) -> Result<RemoteDelta<MutTxn<()>>, anyhow::Error> {
        let force_cache = if self.force_cache {
            Some(self.force_cache)
        } else {
            None
        };
        let delta = remote
            .update_changelist_pushpull(
                txn,
                &self.path,
                channel,
                force_cache,
                repo,
                self.changes.as_slice(),
            )
            .await?;
        let to_download = remote
            .pull(
                repo,
                txn,
                channel,
                delta.to_download.as_slice(),
                &delta.inodes,
                false,
            )
            .await?;

        Ok(RemoteDelta {
            to_download,
            ..delta
        })
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut repo = Repository::find_root(self.repo_path.clone())?;
        let txn = repo.pristine.arc_txn_begin()?;
        let cur = txn
            .read()
            .current_channel()
            .unwrap_or(crate::DEFAULT_CHANNEL)
            .to_string();
        let channel_name = if let Some(ref c) = self.to_channel {
            c
        } else {
            cur.as_str()
        };
        let is_current_channel = channel_name == cur;
        let mut channel = txn.write().open_or_create_channel(&channel_name)?;
        debug!("{:?}", repo.config);
        let remote_name = if let Some(ref rem) = self.from {
            rem
        } else if let Some(ref def) = repo.config.default_remote {
            def
        } else {
            bail!("Missing remote")
        };
        let from_channel = if let Some(ref c) = self.from_channel {
            c
        } else {
            crate::DEFAULT_CHANNEL
        };
        let mut remote = repo
            .remote(
                Some(&repo.path),
                &remote_name,
                from_channel,
                self.no_cert_check,
                true,
            )
            .await?;
        debug!("downloading");

        let RemoteDelta {
            inodes,
            remote_ref,
            mut to_download,
            remote_unrecs,
            ..
        } = self
            .to_download(&mut *txn.write(), &mut channel, &mut repo, &mut remote)
            .await?;

        let hash = super::pending(txn.clone(), &mut channel, &mut repo)?;

        if let Some(ref r) = remote_ref {
            remote.update_identities(&mut repo, r).await?;
        }

        notify_remote_unrecords(&repo, remote_unrecs.as_slice());

        if to_download.is_empty() {
            let mut stderr = std::io::stderr();
            writeln!(stderr, "Nothing to pull")?;
            if let Some(ref h) = hash {
                txn.write().unrecord(&repo.changes, &mut channel, h, 0)?;
            }
            txn.commit()?;
            return Ok(());
        }

        if !self.all {
            let mut o = make_changelist(&repo.changes, &to_download, "pull")?;
            to_download = loop {
                let d = parse_changelist(&edit::edit_bytes(&o[..])?);
                let comp = complete_deps(&*txn.read(), &None, &repo.changes, &to_download, &d)?;
                if comp.len() == d.len() {
                    break comp;
                }
                o = make_changelist(&repo.changes, &comp, "pull")?
            };
        }

        {
            // Now that .pull is always given `false` for `do_apply`...
            let mut ws = libpijul::ApplyWorkspace::new();
            debug!("to_download = {:#?}", to_download);
            let mut pro = PROGRESS.borrow_mut().unwrap();
            let n = pro.push(crate::progress::Cursor::Bar {
                i: 0,
                n: to_download.len(),
                pre: "Applying".into(),
            });
            std::mem::drop(pro);
            let mut channel = channel.write();
            let mut txn = txn.write();
            for h in to_download.iter() {
                txn.apply_change_rec_ws(&repo.changes, &mut channel, h, &mut ws)?;
                PROGRESS.borrow_mut().unwrap()[n].incr()
            }
        }

        debug!("completing changes");
        remote
            .complete_changes(&repo, &*txn.read(), &mut channel, &to_download, self.full)
            .await?;
        remote.finish().await?;

        debug!("inodes = {:?}", inodes);
        debug!("to_download: {:?}", to_download.len());
        let mut touched = HashSet::new();
        let txn_ = txn.read();
        for d in to_download.iter() {
            if let Some(int) = txn_.get_internal(&d.into())? {
                for inode in txn_.iter_rev_touched(int)? {
                    let (int_, inode) = inode?;
                    if int_ < int {
                        continue;
                    } else if int_ > int {
                        break;
                    }
                    let ext = libpijul::pristine::Position {
                        change: txn_.get_external(&inode.change)?.unwrap().into(),
                        pos: inode.pos,
                    };
                    if inodes.is_empty() || inodes.contains(&ext) {
                        touched.insert(*inode);
                    }
                }
            }
        }
        std::mem::drop(txn_);
        if is_current_channel {
            let mut touched_paths: Vec<_> = Vec::new();
            {
                let txn_ = txn.read();
                for &i in touched.iter() {
                    if let Some((path, _)) =
                        libpijul::fs::find_path(&repo.changes, &*txn_, &*channel.read(), false, i)?
                    {
                        touched_paths.push(path)
                    } else {
                        touched_paths.clear();
                        break;
                    }
                }
            }
            touched_paths.sort();
            let mut last = "";
            PROGRESS
                .borrow_mut()
                .unwrap()
                .push(crate::progress::Cursor::Spin {
                    i: 0,
                    pre: "Outputting repository".into(),
                });
            let mut conflicts = Vec::new();
            for path in touched_paths.iter() {
                if !last.is_empty() && path.starts_with(last) {
                    continue;
                }
                debug!("path = {:?}", path);
                conflicts.extend(
                    libpijul::output::output_repository_no_pending(
                        &repo.working_copy,
                        &repo.changes,
                        &txn,
                        &channel,
                        path,
                        true,
                        None,
                        num_cpus::get(),
                        0,
                    )?
                    .into_iter(),
                );
                last = path
            }
            if touched_paths.is_empty() {
                conflicts.extend(
                    libpijul::output::output_repository_no_pending(
                        &repo.working_copy,
                        &repo.changes,
                        &txn,
                        &channel,
                        "",
                        true,
                        None,
                        num_cpus::get(),
                        0,
                    )?
                    .into_iter(),
                );
            }
            PROGRESS.join();
        }
        if let Some(h) = hash {
            txn.write().unrecord(&repo.changes, &mut channel, &h, 0)?;
            repo.changes.del_change(&h)?;
        }

        txn.commit()?;
        Ok(())
    }
}

fn complete_deps<T: TxnT, C: ChangeStore>(
    txn: &T,
    remote_changes: &Option<libpijul::RemoteRef<T>>,
    c: &C,
    original: &[libpijul::Hash],
    now: &[libpijul::Hash],
) -> Result<Vec<libpijul::Hash>, anyhow::Error> {
    let original_: HashSet<_> = original.iter().collect();
    let mut now_ = HashSet::with_capacity(original.len());
    let mut result = Vec::with_capacity(original.len());
    for &h in now {
        now_.insert(h);
        result.push(h);
    }
    let mut stack = now.to_vec();
    stack.reverse();
    while let Some(n) = stack.pop() {
        // check that all of `now`'s deps are in now or not in original
        for d in c.get_dependencies(&n)? {
            if let Some(ref rem) = remote_changes {
                if txn.remote_has_change(rem, &d.into())? {
                    continue;
                }
            }
            if original_.get(&d).is_some() && now_.get(&d).is_none() {
                result.push(d);
                now_.insert(d);
                stack.push(d);
            }
        }
        if now_.insert(n) {
            result.push(n)
        }
    }
    Ok(result)
}

fn check_deps<C: ChangeStore>(
    c: &C,
    original: &[libpijul::Hash],
    now: &[libpijul::Hash],
) -> Result<(), anyhow::Error> {
    let original_: HashSet<_> = original.iter().collect();
    let now_: HashSet<_> = now.iter().collect();
    for n in now {
        // check that all of `now`'s deps are in now or not in original
        for d in c.get_dependencies(n)? {
            if original_.get(&d).is_some() && now_.get(&d).is_none() {
                bail!("Missing dependency: {:?}", n)
            }
        }
    }
    Ok(())
}

fn notify_remote_unrecords(repo: &Repository, remote_unrecs: &[(u64, Hash)]) {
    use std::fmt::Write;
    if !remote_unrecs.is_empty() {
        let mut s = format!(
            "# The following changes have been unrecorded in the remote.\n\
            # This buffer is only being used to inform you of the remote change;\n\
            # your push will continue when it is closed.\n"
        );
        for (_, hash) in remote_unrecs {
            let header = &repo.changes.get_change(hash).unwrap().header;
            s.push_str("#\n");
            writeln!(&mut s, "#    {}", header.message).expect("Infallible write to String");
            writeln!(&mut s, "#    {}", header.timestamp).expect("Infallible write to String");
            writeln!(&mut s, "#    {}", hash.to_base32()).expect("Infallible write to String");
        }
        if let Err(e) = edit::edit(s.as_str()) {
            log::error!(
                "Notification of remote unrecords experienced an error: {}",
                e
            );
        }
    }
}

fn notify_unknown_changes(unknown_changes: &[Hash]) {
    use std::fmt::Write;
    if unknown_changes.is_empty() {
        return;
    } else {
        let mut s = format!(
            "# The following changes are new in the remote\n# (and are not yet known to your local copy):\n#\n"
        );
        let rest_len = unknown_changes.len().saturating_sub(5);
        for hash in unknown_changes.iter().take(5) {
            writeln!(&mut s, "#     {}", hash.to_base32()).expect("Infallible write to String");
        }
        if rest_len > 0 {
            let plural = if rest_len == 1 { "" } else { "s" };
            writeln!(&mut s, "#     ... plus {} more change{}", rest_len, plural)
                .expect("Infallible write to String");
        }
        if let Err(e) = edit::edit(s.as_str()) {
            log::error!(
                "Notification of unknown changes experienced an error: {}",
                e
            );
        }
    }
}
