use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::{make_changelist, parse_changelist};
use anyhow::bail;
use clap::Clap;
use lazy_static::lazy_static;
use libpijul::changestore::ChangeStore;
use libpijul::*;
use log::debug;
use regex::Regex;

use crate::progress::PROGRESS;
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
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path).await?;
        debug!("{:?}", repo.config);
        let mut stdout = std::io::stdout();
        match self.subcmd {
            None => {
                let txn = repo.pristine.txn_begin()?;
                for r in txn.iter_remotes("")? {
                    let r = r?;
                    writeln!(stdout, "  {}", r.name())?;
                }
            }
            Some(SubRemote::Delete { remote }) => {
                let mut txn = repo.pristine.mut_txn_begin()?;
                if !txn.drop_named_remote(&remote)? {
                    writeln!(std::io::stderr(), "Remote not found: {:?}", remote)?
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
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut stderr = std::io::stderr();
        let repo = Repository::find_root(self.repo_path).await?;
        debug!("{:?}", repo.config);
        let (channel_name, _) = repo
            .config
            .get_current_channel(self.from_channel.as_deref());
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
            )
            .await?;
        let mut txn = repo.pristine.mut_txn_begin()?;
        let remote_changes = remote.update_changelist(&mut txn, &self.path).await?;
        let channel = txn.open_or_create_channel(channel_name)?;

        let mut paths = HashSet::new();
        for path in self.path.iter() {
            let (p, ambiguous) = txn.follow_oldest_path(&repo.changes, &channel, path)?;
            if ambiguous {
                bail!("Ambiguous path: {:?}", path)
            }
            paths.insert(p);
            paths.extend(
                libpijul::fs::iter_graph_descendants(&txn, &channel.read()?.graph, p)?
                    .map(|x| x.unwrap()),
            );
        }

        let mut to_upload: Vec<Hash> = Vec::new();
        for x in txn.reverse_log(&*channel.read()?, None)? {
            let (_, (h, m)) = x?;
            if let Some((_, ref remote_changes)) = remote_changes {
                if txn.remote_has_state(remote_changes, &m)? {
                    break;
                }
                let h_int = txn.get_internal(h)?.unwrap();
                if !txn.remote_has_change(&remote_changes, &h)? {
                    if paths.is_empty() {
                        to_upload.push(h.into())
                    } else {
                        for p in paths.iter() {
                            if txn.get_touched_files(p, Some(h_int))?.is_some() {
                                to_upload.push(h.into());
                                break;
                            }
                        }
                    }
                }
            } else if let crate::remote::RemoteRepo::LocalChannel(ref remote_channel) = remote {
                if let Some(channel) = txn.load_channel(remote_channel)? {
                    let channel = channel.read()?;
                    let h_int = txn.get_internal(h)?.unwrap();
                    if txn.get_changeset(txn.changes(&channel), h_int)?.is_none() {
                        if paths.is_empty() {
                            to_upload.push(h.into())
                        } else {
                            for p in paths.iter() {
                                if txn.get_touched_files(p, Some(h_int))?.is_some() {
                                    to_upload.push(h.into());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        debug!("to_upload = {:?}", to_upload);

        if to_upload.is_empty() {
            writeln!(stderr, "Nothing to push")?;
            return Ok(());
        }
        to_upload.reverse();

        let to_upload = if !self.changes.is_empty() {
            let mut u: Vec<libpijul::Hash> = Vec::new();
            let mut not_found = Vec::new();
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
            let remote_changes = remote_changes.map(|x| x.1);
            loop {
                let d = parse_changelist(&edit::edit_bytes(&o[..])?);
                let comp = complete_deps(&txn, &remote_changes, &repo.changes, &to_upload, &d)?;
                if comp.len() == d.len() {
                    break comp;
                }
                o = make_changelist(&repo.changes, &comp, "push")?
            }
        };
        debug!("to_upload = {:?}", to_upload);

        if to_upload.is_empty() {
            writeln!(stderr, "Nothing to push")?;
            return Ok(());
        }

        remote
            .upload_changes(&mut txn, repo.changes_dir.clone(), push_channel, &to_upload)
            .await?;
        txn.commit()?;

        remote.finish().await?;
        Ok(())
    }
}

impl Pull {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut repo = Repository::find_root(self.repo_path).await?;
        let mut txn = repo.pristine.mut_txn_begin()?;
        let (channel_name, is_current_channel) =
            repo.config.get_current_channel(self.to_channel.as_deref());
        let mut channel = txn.open_or_create_channel(channel_name)?;
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
            )
            .await?;
        debug!("downloading");

        let mut inodes: HashSet<libpijul::pristine::Position<libpijul::Hash>> = HashSet::new();
        let mut to_download = if self.changes.is_empty() {
            let remote_changes = remote.update_changelist(&mut txn, &self.path).await?;
            debug!("changelist done");
            let mut to_download: Vec<Hash> = Vec::new();
            if let Some((inodes_, remote_changes)) = remote_changes {
                inodes.extend(inodes_.into_iter());
                for x in txn.iter_remote(&remote_changes.lock()?.remote, 0)? {
                    let p = x?.1; // (h, m)
                    if txn
                        .channel_has_state(txn.states(&*channel.read()?), &p.b)?
                        .is_some()
                    {
                        break;
                    } else if txn.get_revchanges(&channel, &p.a.into())?.is_none() {
                        to_download.push(p.a.into())
                    }
                }
            } else if let crate::remote::RemoteRepo::LocalChannel(ref remote_channel) = remote {
                let mut inodes_ = HashSet::new();
                for path in self.path.iter() {
                    let (p, ambiguous) = txn.follow_oldest_path(&repo.changes, &channel, path)?;
                    if ambiguous {
                        bail!("Ambiguous path: {:?}", path)
                    }
                    inodes_.insert(p);
                    inodes_.extend(
                        libpijul::fs::iter_graph_descendants(&txn, &channel.read()?.graph, p)?
                            .map(|x| x.unwrap()),
                    );
                }
                inodes.extend(inodes_.iter().map(|x| libpijul::pristine::Position {
                    change: txn.get_external(&x.change).unwrap().unwrap().into(),
                    pos: x.pos,
                }));
                if let Some(remote_channel) = txn.load_channel(remote_channel)? {
                    let remote_channel = remote_channel.read()?;
                    for x in txn.reverse_log(&remote_channel, None)? {
                        let (h, m) = x?.1;
                        if txn
                            .channel_has_state(txn.states(&*channel.read()?), &m)?
                            .is_some()
                        {
                            break;
                        }
                        let h_int = txn.get_internal(h)?.unwrap();
                        if txn
                            .get_changeset(txn.changes(&*channel.read()?), h_int)?
                            .is_none()
                        {
                            if inodes_.is_empty()
                                || inodes_.iter().any(|&inode| {
                                    txn.get_rev_touched_files(h_int, Some(&inode))
                                        .unwrap()
                                        .is_some()
                                })
                            {
                                to_download.push(h.into())
                            }
                        }
                    }
                }
            }
            to_download
        } else {
            let r: Result<Vec<libpijul::Hash>, anyhow::Error> = self
                .changes
                .iter()
                .map(|h| Ok(txn.hash_from_prefix(h)?.0))
                .collect();
            r?
        };
        debug!("recording");
        let txn = Arc::new(RwLock::new(txn));
        let hash = super::pending(txn.clone(), &mut channel, &mut repo)?;
        let mut txn = if let Ok(txn) = Arc::try_unwrap(txn) {
            txn.into_inner().unwrap()
        } else {
            unreachable!()
        };
        let mut to_download = remote
            .pull(
                &mut repo,
                &mut txn,
                &mut channel,
                &mut to_download,
                &inodes,
                self.all,
            )
            .await?;

        if to_download.is_empty() {
            let mut stderr = std::io::stderr();
            writeln!(stderr, "Nothing to pull")?;
            return Ok(());
        }

        if !self.all {
            let mut o = make_changelist(&repo.changes, &to_download, "pull")?;
            to_download = loop {
                let d = parse_changelist(&edit::edit_bytes(&o[..])?);
                let comp = complete_deps(&txn, &None, &repo.changes, &to_download, &d)?;
                if comp.len() == d.len() {
                    break comp;
                }
                o = make_changelist(&repo.changes, &comp, "pull")?
            };
            let mut ws = libpijul::ApplyWorkspace::new();
            debug!("to_download = {:#?}", to_download);
            let mut pro = PROGRESS.borrow_mut().unwrap();
            let n = pro.push(crate::progress::Cursor::Bar {
                i: 0,
                n: to_download.len(),
                pre: "Applying".into(),
            });
            std::mem::drop(pro);
            let mut channel = channel.write().unwrap();
            for h in to_download.iter() {
                txn.apply_change_rec_ws(&repo.changes, &mut channel, h, &mut ws)?;
                PROGRESS.borrow_mut().unwrap()[n].incr()
            }
        }
        debug!("completing changes");
        remote
            .complete_changes(&repo, &txn, &mut channel, &to_download, self.full)
            .await?;
        remote.finish().await?;

        debug!("inodes = {:?}", inodes);
        debug!("to_download: {:?}", to_download.len());
        let mut touched = HashSet::new();
        for d in to_download.iter() {
            if let Some(int) = txn.get_internal(&d.into())? {
                for inode in txn.iter_rev_touched(int)? {
                    let (int_, inode) = inode?;
                    if int_ < int {
                        continue;
                    } else if int_ > int {
                        break;
                    }
                    let ext = libpijul::pristine::Position {
                        change: txn.get_external(&inode.change)?.unwrap().into(),
                        pos: inode.pos,
                    };
                    if inodes.is_empty() || inodes.contains(&ext) {
                        touched.insert(*inode);
                    }
                }
            }
        }
        let txn = Arc::new(RwLock::new(txn));
        if is_current_channel {
            let mut touched_paths: Vec<_> = Vec::new();
            for &i in touched.iter() {
                if let Some((path, _)) = libpijul::fs::find_path(
                    &repo.changes,
                    &*txn.read().unwrap(),
                    &*channel.read()?,
                    false,
                    i,
                )? {
                    touched_paths.push(path)
                } else {
                    touched_paths.clear();
                    break;
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
                        repo.working_copy.clone(),
                        &repo.changes,
                        txn.clone(),
                        channel.clone(),
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
                        repo.working_copy.clone(),
                        &repo.changes,
                        txn.clone(),
                        channel.clone(),
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
        let mut txn = if let Ok(txn) = Arc::try_unwrap(txn) {
            txn.into_inner().unwrap()
        } else {
            unreachable!()
        };
        if let Some(h) = hash {
            txn.unrecord(&repo.changes, &mut channel, &h, 0)?;
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
