use std::io::Write;
use std::path::{Path, PathBuf};

use canonical_path::CanonicalPathBuf;
use clap::{ArgSettings, Clap};
use libpijul::{MutTxnT, MutTxnTExt, TxnTExt};
use log::{debug, info};

use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Move {
    #[clap(setting = ArgSettings::Hidden, long = "salt")]
    salt: Option<u64>,
    /// Paths which need to be moved
    ///
    /// The last argument to this option is considered the
    /// destination
    paths: Vec<PathBuf>,
}

impl Move {
    pub fn run(mut self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(None)?;
        let to = if let Some(to) = self.paths.pop() {
            to
        } else {
            return Ok(());
        };
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
            let source = std::fs::canonicalize(&p.clone())?;
            debug!("source = {:?}", source);
            let target = if is_dir {
                to.join(source.file_name().unwrap())
            } else {
                to.clone()
            };
            debug!("target = {:?}", target);

            let r = Rename {
                source: &source,
                target: &target,
            };
            std::fs::rename(r.source, r.target)?;
            let target = std::fs::canonicalize(r.target)?;
            debug!("target = {:?}", target);
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

#[derive(Clap, Debug)]
pub struct List {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
}

impl List {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path)?;
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
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(None)?;
        let txn = repo.pristine.arc_txn_begin()?;
        let threads = num_cpus::get();
        let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
        let mut stderr = std::io::stderr();
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
                    &txn,
                    repo_path.clone(),
                    full.clone(),
                    threads,
                    self.salt.unwrap_or(0),
                )?
            } else {
                let mut txn = txn.write();
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
        txn.commit()?;
        Ok(())
    }
}

#[derive(Clap, Debug)]
pub struct Remove {
    /// The paths need to be removed
    paths: Vec<PathBuf>,
}

impl Remove {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(None)?;
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
