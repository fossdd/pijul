use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use canonical_path::CanonicalPathBuf;
use clap::{ArgSettings, Clap};
use libpijul::{MutTxnT, MutTxnTExt, TxnTExt};
use log::{debug, info};

use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Move {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,

    #[clap(setting = ArgSettings::Hidden, long = "salt")]
    salt: Option<u64>,
    /// Paths which need to be moved
    ///
    /// The last argument to this option is considered the
    /// destination
    paths: Vec<PathBuf>,
}

impl Move {
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone()).await?;
        let to = if let Some(to) = self.paths.pop() {
            to
        } else {
            return Ok(());
        };
        let to = path(&self.repo_path, to);
        let is_dir = if let Ok(m) = std::fs::metadata(&to) {
            m.is_dir()
        } else {
            false
        };
        if !is_dir && self.paths.len() > 1 {
            return Ok(());
        }

        let mut txn = repo.pristine.mut_txn_begin()?;
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        for p in self.paths {
            debug!("p = {:?}", p);
            let source = std::fs::canonicalize(&path(&self.repo_path, p.clone()))?;
            let target = if is_dir { to.join(p) } else { to.clone() };
            debug!("target = {:?}", target);

            let r = Rename {
                source: &source,
                target: &target,
            };
            std::fs::rename(r.source, r.target)?;
            let target = std::fs::canonicalize(r.target)?;
            {
                let source = source.strip_prefix(&repo_path)?;
                use path_slash::PathExt;
                let source = source.to_slash_lossy();
                let target = target.strip_prefix(&repo_path)?;
                let target = target.to_slash_lossy();
                debug!("moving {:?} -> {:?}", source, target);
                txn.move_file(&source, &target, self.salt.unwrap_or(0))?;
            }
            std::mem::forget(r);
        }
        txn.commit()?;
        Ok(())
    }
}

struct Rename<'a> {
    source: &'a Path,
    target: &'a Path,
}

impl<'a> Drop for Rename<'a> {
    fn drop(&mut self) {
        std::fs::rename(self.target, self.source).unwrap_or(())
    }
}

fn path(root: &Option<PathBuf>, path: PathBuf) -> PathBuf {
    if let Some(ref p) = root {
        p.join(path)
    } else {
        path
    }
}

#[derive(Clap, Debug)]
pub struct List {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
}

impl List {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = unsafe { Repository::find_root(self.repo_path).await? };
        let txn = repo.pristine.txn_begin()?;
        let mut stdout = std::io::stdout();
        for p in txn.iter_working_copy() {
            let p = p?.1;
            writeln!(stdout, "{}", p)?;
        }
        Ok(())
    }
}

#[derive(Clap, Debug)]
pub struct Add {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    #[clap(short = 'r', long = "recursive")]
    recursive: bool,
    #[clap(short = 'f', long = "force")]
    force: bool,
    #[clap(setting = ArgSettings::Hidden, long = "salt")]
    salt: Option<u64>,
    /// Paths to add to the internal tree.
    paths: Vec<PathBuf>,
}

impl Add {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone()).await?;
        let txn = repo.pristine.mut_txn_begin()?;
        let threads = num_cpus::get();
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        let mut stderr = std::io::stderr();
        let txn = Arc::new(RwLock::new(txn));
        for path in self.paths.iter() {
            info!("Adding {:?}", path);
            let path = CanonicalPathBuf::canonicalize(&path)?;
            debug!("{:?}", path);
            let meta = std::fs::metadata(&path)?;
            debug!("{:?}", meta);
            if !self.force
                && !libpijul::working_copy::filesystem::filter_ignore(
                    repo_path.as_ref(),
                    path.as_ref(),
                    meta.is_dir(),
                )
            {
                continue;
            }
            if self.recursive {
                use libpijul::working_copy::filesystem::*;
                let (full, _) = get_prefix(Some(repo_path.as_ref()), path.as_path())?;
                repo.working_copy.add_prefix_rec(
                    txn.clone(),
                    repo_path.clone(),
                    full.clone(),
                    threads,
                    self.salt.unwrap_or(0),
                )?
            } else {
                let mut txn = txn.write().unwrap();
                let path = if let Ok(path) = path.as_path().strip_prefix(&repo_path.as_path()) {
                    path
                } else {
                    continue;
                };
                use path_slash::PathExt;
                let path_str = path.to_slash_lossy();
                if !txn.is_tracked(&path_str)? {
                    if let Err(e) = txn.add(&path_str, meta.is_dir(), self.salt.unwrap_or(0)) {
                        writeln!(stderr, "{}", e)?;
                    }
                }
            }
        }
        if let Ok(txn) = Arc::try_unwrap(txn) {
            txn.into_inner().unwrap().commit()?;
        } else {
            unreachable!()
        }
        Ok(())
    }
}

#[derive(Clap, Debug)]
pub struct Remove {
    #[clap(long = "repository")]
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    repo_path: Option<PathBuf>,
    /// The paths need to be removed
    paths: Vec<PathBuf>,
}

impl Remove {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone()).await?;
        let mut txn = repo.pristine.mut_txn_begin()?;
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        for path in self.paths.iter() {
            debug!("{:?}", path);

            if let Some(p) = path.file_name() {
                if let Some(p) = p.to_str() {
                    if p.ends_with('~') || (p.starts_with('#') && p.ends_with('#')) {
                        continue;
                    }
                }
            }

            let path = path.canonicalize()?;
            let path = if let Ok(path) = path.strip_prefix(&repo_path.as_path()) {
                path
            } else {
                continue;
            };
            use path_slash::PathExt;
            let path_str = path.to_slash_lossy();
            if txn.is_tracked(&path_str)? {
                txn.remove_file(&path_str)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
}
