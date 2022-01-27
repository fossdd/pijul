use super::*;
use crate::pristine::{ArcTxn, GraphTxnT, InodeMetadata, TreeErr, TreeTxnT, TxnErr};
use canonical_path::{CanonicalPath, CanonicalPathBuf};
use ignore::WalkBuilder;
use std::borrow::Cow;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct FileSystem {
    root: PathBuf,
}

/// Returns whether `path` is a child of `root_` (or `root_` itself).
pub fn filter_ignore(root_: &CanonicalPath, path: &CanonicalPath, is_dir: bool) -> bool {
    debug!("path = {:?} root = {:?}", path, root_);
    if let Ok(suffix) = path.as_path().strip_prefix(root_.as_path()) {
        debug!("suffix = {:?}", suffix);
        let mut root = root_.as_path().to_path_buf();
        let mut ignore = ignore::gitignore::GitignoreBuilder::new(&root);
        let mut add_root = |root: &mut PathBuf| {
            ignore.add_line(None, crate::DOT_DIR).unwrap();
            root.push(".ignore");
            ignore.add(&root);
            root.pop();
            root.push(".gitignore");
            ignore.add(&root);
            root.pop();
        };
        add_root(&mut root);
        for c in suffix.components() {
            root.push(c);
            add_root(&mut root);
        }
        if let Ok(ig) = ignore.build() {
            let m = ig.matched(suffix, is_dir);
            debug!("m = {:?}", m);
            return !m.is_ignore();
        }
    }
    false
}

/// From a path on the filesystem, return the canonical path (a `PathBuf`), and a
/// prefix relative to the root of the repository (a `String`).
pub fn get_prefix(
    repo_path: Option<&Path>,
    prefix: &Path,
) -> Result<(PathBuf, String), std::io::Error> {
    let mut p = String::new();
    let repo = if let Some(repo) = repo_path {
        Cow::Borrowed(repo.into())
    } else {
        Cow::Owned(std::env::current_dir()?)
    };
    debug!("get prefix {:?} {:?}", repo, prefix);
    let repo_prefix = &repo.join(&prefix);
    let prefix_ = if let Ok(x) = repo_prefix.canonicalize() {
        x
    } else {
        let mut p = PathBuf::new();
        for c in repo_prefix.components() {
            use std::path::Component;
            match c {
                Component::Prefix(_) => p.push(c.as_os_str()),
                Component::RootDir => p.push(c.as_os_str()),
                Component::CurDir => {}
                Component::ParentDir => {
                    p.pop();
                }
                Component::Normal(x) => p.push(x),
            }
        }
        p
    };
    debug!("get prefix {:?}", prefix_);
    if let Ok(prefix) = prefix_.as_path().strip_prefix(repo) {
        for c in prefix.components() {
            if !p.is_empty() {
                p.push('/');
            }
            let c: &std::path::Path = c.as_ref();
            p.push_str(&c.to_string_lossy())
        }
    }
    Ok((prefix_, p))
}

#[derive(Error)]
pub enum AddError<T: GraphTxnT + TreeTxnT> {
    #[error(transparent)]
    Ignore(#[from] ignore::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fs(#[from] crate::fs::FsError<T>),
}

impl<T: GraphTxnT + TreeTxnT> std::fmt::Debug for AddError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AddError::Ignore(e) => std::fmt::Debug::fmt(e, fmt),
            AddError::Io(e) => std::fmt::Debug::fmt(e, fmt),
            AddError::Fs(e) => std::fmt::Debug::fmt(e, fmt),
        }
    }
}

#[derive(Error)]
pub enum Error<C: std::error::Error + 'static, T: GraphTxnT + TreeTxnT> {
    #[error(transparent)]
    Add(#[from] AddError<T>),
    #[error(transparent)]
    Record(#[from] crate::record::RecordError<C, std::io::Error, T>),
    #[error(transparent)]
    Txn(#[from] TxnErr<T::GraphError>),
    #[error(transparent)]
    Tree(#[from] TreeErr<T::TreeError>),
}

impl<C: std::error::Error + 'static, T: GraphTxnT + TreeTxnT> std::fmt::Debug for Error<C, T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Add(e) => std::fmt::Debug::fmt(e, fmt),
            Error::Record(e) => std::fmt::Debug::fmt(e, fmt),
            Error::Txn(e) => std::fmt::Debug::fmt(e, fmt),
            Error::Tree(e) => std::fmt::Debug::fmt(e, fmt),
        }
    }
}

pub struct Untracked {
    join: Option<std::thread::JoinHandle<Result<(), std::io::Error>>>,
    receiver: std::sync::mpsc::Receiver<(PathBuf, bool)>,
}

impl Iterator for Untracked {
    type Item = Result<(PathBuf, bool), std::io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(x) = self.receiver.recv() {
            return Some(Ok(x));
        } else if let Some(j) = self.join.take() {
            if let Ok(Err(e)) = j.join() {
                return Some(Err(e));
            }
        }
        None
    }
}

impl FileSystem {
    pub fn from_root<P: AsRef<Path>>(root: P) -> Self {
        FileSystem {
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn record_prefixes<
        T: crate::MutTxnTExt + crate::TxnTExt + Send + Sync + 'static,
        C: crate::changestore::ChangeStore + Clone + Send + 'static,
        P: AsRef<Path>,
    >(
        &self,
        txn: ArcTxn<T>,
        channel: crate::pristine::ChannelRef<T>,
        changes: &C,
        state: &mut crate::RecordBuilder,
        repo_path: CanonicalPathBuf,
        prefixes: &[P],
        force: bool,
        threads: usize,
        salt: u64,
    ) -> Result<(), Error<C::Error, T>>
    where
        T::Channel: Send + Sync,
    {
        for prefix in prefixes.iter() {
            self.clone().record_prefix(
                txn.clone(),
                channel.clone(),
                changes,
                state,
                repo_path.clone(),
                prefix.as_ref(),
                force,
                threads,
                salt,
            )?
        }
        if prefixes.is_empty() {
            self.record_prefix(
                txn,
                channel,
                changes,
                state,
                repo_path.clone(),
                Path::new(""),
                force,
                threads,
                salt,
            )?
        }
        Ok(())
    }

    pub fn add_prefix_rec<T: crate::MutTxnTExt + crate::TxnTExt>(
        &self,
        txn: &ArcTxn<T>,
        repo_path: CanonicalPathBuf,
        full: CanonicalPathBuf,
        force: bool,
        threads: usize,
        salt: u64,
    ) -> Result<(), AddError<T>> {
        let mut txn = txn.write();
        for p in self.iterate_prefix_rec(repo_path.clone(), full.clone(), force, threads)? {
            let (path, is_dir) = p?;
            info!("Adding {:?}", path);
            use path_slash::PathExt;
            let path_str = path.to_slash_lossy();
            if path_str.is_empty() || path_str == "." {
                continue;
            }
            match txn.add(&path_str, is_dir, salt) {
                Ok(_) => {}
                Err(crate::fs::FsError::AlreadyInRepo(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub fn iterate_prefix_rec(
        &self,
        repo_path: CanonicalPathBuf,
        full: CanonicalPathBuf,
        force: bool,
        threads: usize,
    ) -> Result<Untracked, std::io::Error> {
        debug!("full = {:?}", full);
        let meta = std::fs::metadata(&full)?;
        debug!("meta = {:?}", meta);
        let (sender, receiver) = std::sync::mpsc::sync_channel(100);

        debug!("{:?}", full.as_path().strip_prefix(repo_path.as_path()));
        debug!("force = {:?}", force);
        if !force {
            if !filter_ignore(
                &repo_path.as_canonical_path(),
                &full.as_canonical_path(),
                meta.is_dir(),
            ) {
                return Ok(Untracked {
                    join: None,
                    receiver,
                });
            }
        }
        let t = std::thread::spawn(move || -> Result<(), std::io::Error> {
            if meta.is_dir() {
                let mut walk = WalkBuilder::new(&full);
                walk.ignore(!force)
                    .git_ignore(!force)
                    .hidden(false)
                    .filter_entry(|p| {
                        debug!("p.file_name = {:?}", p.file_name());
                        p.file_name() != crate::DOT_DIR
                    })
                    .threads((threads - 1).max(1));
                walk.build_parallel().run(|| {
                    Box::new(|entry| {
                        let entry: ignore::DirEntry = if let Ok(entry) = entry {
                            entry
                        } else {
                            return ignore::WalkState::Quit;
                        };
                        let p = entry.path();
                        if let Some(p) = p.file_name() {
                            if let Some(p) = p.to_str() {
                                if p.ends_with("~") || (p.starts_with("#") && p.ends_with("#")) {
                                    return ignore::WalkState::Skip;
                                }
                            }
                        }
                        debug!("entry path = {:?} {:?}", entry.path(), repo_path);
                        if let Ok(path) = entry.path().strip_prefix(&repo_path) {
                            let is_dir = entry.file_type().unwrap().is_dir();
                            if sender.send((path.to_path_buf(), is_dir)).is_err() {
                                return ignore::WalkState::Quit;
                            }
                        } else {
                            debug!("entry = {:?}", entry.path());
                        }
                        ignore::WalkState::Continue
                    })
                })
            } else {
                debug!("filter_ignore ok");
                let path = full.as_path().strip_prefix(&repo_path.as_path()).unwrap();
                sender.send((path.to_path_buf(), false)).unwrap();
            }
            Ok(())
        });
        Ok(Untracked {
            join: Some(t),
            receiver,
        })
    }

    pub fn record_prefix<
        T: crate::MutTxnTExt + crate::TxnTExt + Send + Sync + 'static,
        C: crate::changestore::ChangeStore + Clone + Send + 'static,
    >(
        &self,
        txn: ArcTxn<T>,
        channel: crate::pristine::ChannelRef<T>,
        changes: &C,
        state: &mut crate::RecordBuilder,
        repo_path: CanonicalPathBuf,
        prefix: &Path,
        force: bool,
        threads: usize,
        salt: u64,
    ) -> Result<(), Error<C::Error, T>>
    where
        T::Channel: Send + Sync,
    {
        let (full, prefix) = get_prefix(Some(repo_path.as_ref()), prefix).map_err(AddError::Io)?;
        if let Ok(full) = CanonicalPathBuf::canonicalize(&full) {
            if let Ok(path) = full.as_path().strip_prefix(&repo_path.as_path()) {
                use path_slash::PathExt;
                let path_str = path.to_slash_lossy();
                if !crate::fs::is_tracked(&*txn.read(), &path_str)? {
                    self.add_prefix_rec(&txn, repo_path, full, force, threads, salt)?;
                }
            }
        }
        debug!("recording from prefix {:?}", prefix);
        state.record(
            txn.clone(),
            crate::Algorithm::default(),
            false,
            &crate::diff::DEFAULT_SEPARATOR,
            channel,
            self,
            changes,
            &prefix,
            1,
        )?;
        debug!("recorded");
        Ok(())
    }

    fn path(&self, file: &str) -> PathBuf {
        let mut path = self.root.clone();
        path.extend(crate::path::components(file));
        path
    }
}

impl WorkingCopyRead for FileSystem {
    type Error = std::io::Error;
    fn file_metadata(&self, file: &str) -> Result<InodeMetadata, Self::Error> {
        debug!("metadata {:?}", file);
        let attr = std::fs::metadata(&self.path(file))?;
        let permissions = permissions(&attr).unwrap_or(0o700);
        debug!("permissions = {:?}", permissions);
        Ok(InodeMetadata::new(permissions & 0o100, attr.is_dir()))
    }
    fn read_file(&self, file: &str, buffer: &mut Vec<u8>) -> Result<(), Self::Error> {
        use std::io::Read;
        debug!("read_file {:?}", file);
        let mut f = std::fs::File::open(&self.path(file))?;
        f.read_to_end(buffer)?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn modified_time(&self, file: &str) -> Result<std::time::SystemTime, Self::Error> {
        debug!("modified_time {:?}", file);
        let attr = std::fs::metadata(&self.path(file))?;
        Ok(attr.modified()?)
    }

    #[cfg(unix)]
    fn modified_time(&self, file: &str) -> Result<std::time::SystemTime, Self::Error> {
        debug!("modified_time {:?}", file);
        use std::os::unix::fs::MetadataExt;
        let attr = std::fs::metadata(&self.path(file))?;
        let ctime = std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis(
                attr.ctime() as u64 * 1000 + attr.ctime_nsec() as u64 / 1_000_000,
            );
        Ok(attr.modified()?.min(ctime))
    }
}

impl WorkingCopy for FileSystem {
    fn create_dir_all(&self, file: &str) -> Result<(), Self::Error> {
        debug!("create_dir_all {:?}", file);
        Ok(std::fs::create_dir_all(&self.path(file))?)
    }

    fn remove_path(&self, path: &str, rec: bool) -> Result<(), Self::Error> {
        debug!("remove_path {:?}", path);
        let path = self.path(path);
        if let Ok(meta) = std::fs::metadata(&path) {
            if let Err(e) = if meta.is_dir() {
                if rec {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_dir(&path)
                }
            } else {
                std::fs::remove_file(&path)
            } {
                info!("while deleting {:?}: {:?}", path, e);
            }
        }
        Ok(())
    }
    fn rename(&self, former: &str, new: &str) -> Result<(), Self::Error> {
        debug!("rename {:?} {:?}", former, new);
        let former = self.path(former);
        let new = self.path(new);
        if let Some(p) = new.parent() {
            std::fs::create_dir_all(p)?
        }
        std::fs::rename(&former, &new)?;
        Ok(())
    }
    #[cfg(not(windows))]
    fn set_permissions(&self, name: &str, permissions: u16) -> Result<(), Self::Error> {
        use std::os::unix::fs::PermissionsExt;
        let name = self.path(name);
        debug!("set_permissions: {:?}", name);
        let metadata = std::fs::metadata(&name)?;
        let mut current = metadata.permissions();
        debug!(
            "setting mode for {:?} to {:?} (currently {:?})",
            name, permissions, current
        );
        if permissions & 0o100 != 0 {
            current.set_mode(current.mode() | 0o100);
        } else {
            current.set_mode(current.mode() & ((!0o777) | 0o666));
        }
        debug!("setting {:?}", current);
        std::fs::set_permissions(name, current)?;
        debug!("set");
        Ok(())
    }
    #[cfg(windows)]
    fn set_permissions(&self, _name: &str, _permissions: u16) -> Result<(), Self::Error> {
        Ok(())
    }

    type Writer = std::io::BufWriter<std::fs::File>;
    fn write_file(&self, file: &str, _: Inode) -> Result<Self::Writer, Self::Error> {
        let path = self.path(file);
        debug!("path = {:?}", path);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap_or(())
        }
        debug!("write_file: dir created");
        std::fs::remove_file(&path).unwrap_or(());
        let file = std::io::BufWriter::new(std::fs::File::create(&path)?);
        debug!("file");
        Ok(file)
    }
}

#[cfg(not(windows))]
fn permissions(attr: &std::fs::Metadata) -> Option<usize> {
    use std::os::unix::fs::PermissionsExt;
    Some(attr.permissions().mode() as usize)
}
#[cfg(windows)]
fn permissions(_: &std::fs::Metadata) -> Option<usize> {
    None
}
