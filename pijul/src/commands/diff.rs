use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;

use canonical_path::CanonicalPathBuf;
use clap::Parser;
use libpijul::change::*;
use libpijul::{MutTxnT, TxnT, TxnTExt};
use serde_derive::Serialize;

use crate::repository::*;

#[derive(Parser, Debug)]
pub struct Diff {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    pub repo_path: Option<PathBuf>,
    /// Output the diff in JSON format instead of the default change text format.
    #[clap(long = "json")]
    pub json: bool,
    /// Compare with this channel.
    #[clap(long = "channel")]
    pub channel: Option<String>,
    /// Add all the changes of this channel as dependencies (except changes implied transitively), instead of the minimal dependencies.
    #[clap(long = "tag")]
    pub tag: bool,
    /// Show a short version of the diff.
    #[clap(short = 's', long = "short")]
    pub short: bool,
    /// Include the untracked files
    #[clap(short = 'u', long = "untracked")]
    pub untracked: bool,
    /// Only diff those paths (files or directories). If missing, diff the entire repository.
    pub prefixes: Vec<PathBuf>,
}

impl Diff {
    pub fn run(mut self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone())?;
        let txn = repo.pristine.arc_txn_begin()?;
        let mut stdout = std::io::stdout();

        if self.untracked && self.json {
            let txn = txn.read();
            serde_json::to_writer_pretty(
                &mut std::io::stdout(),
                &untracked(&repo, &*txn)?.collect::<Vec<_>>(),
            )?;
            writeln!(stdout)?;
            return Ok(());
        }

        let cur = txn
            .read()
            .current_channel()
            .unwrap_or(crate::DEFAULT_CHANNEL)
            .to_string();
        let channel = if let Some(ref c) = self.channel {
            c
        } else {
            cur.as_str()
        };
        let channel = txn.write().open_or_create_channel(&channel)?;

        let mut state = libpijul::RecordBuilder::new();
        if self.prefixes.is_empty() {
            state.record(
                txn.clone(),
                libpijul::Algorithm::default(),
                self.short,
                &libpijul::DEFAULT_SEPARATOR,
                channel.clone(),
                &repo.working_copy,
                &repo.changes,
                "",
                num_cpus::get(),
            )?
        } else {
            self.fill_relative_prefixes()?;
            repo.working_copy.record_prefixes(
                txn.clone(),
                channel.clone(),
                &repo.changes,
                &mut state,
                CanonicalPathBuf::canonicalize(&repo.path)?,
                &self.prefixes,
                false,
                num_cpus::get(),
                0,
            )?;
        }
        let rec = state.finish();
        if rec.actions.is_empty() {
            let txn = txn.read();
            if self.short && self.untracked {
                for path in untracked(&repo, &*txn)? {
                    writeln!(stdout, "U {}", path.to_str().unwrap())?;
                }
            } else if self.untracked {
                for path in untracked(&repo, &*txn)? {
                    writeln!(stdout, "{}", path.to_str().unwrap())?;
                }
            }
            return Ok(());
        }
        let mut txn_ = txn.write();
        let actions: Vec<_> = rec
            .actions
            .into_iter()
            .map(|rec| rec.globalize(&*txn_).unwrap())
            .collect();
        let actions_is_empty = actions.is_empty();
        let contents = if let Ok(cont) = std::sync::Arc::try_unwrap(rec.contents) {
            cont.into_inner()
        } else {
            unreachable!()
        };
        let mut change = LocalChange::make_change(
            &*txn_,
            &channel,
            actions,
            contents,
            ChangeHeader::default(),
            Vec::new(),
        )?;

        let (dependencies, extra_known) = if self.tag {
            full_dependencies(&*txn_, &channel)?
        } else {
            dependencies(&*txn_, &*channel.read(), change.changes.iter())?
        };
        change.dependencies = dependencies;
        change.extra_known = extra_known;

        let colors = is_colored();
        if self.json {
            let mut changes = BTreeMap::new();
            for ch in change.changes.iter() {
                changes
                    .entry(ch.path())
                    .or_insert_with(Vec::new)
                    .push(Status {
                        operation: match ch {
                            Hunk::FileMove { .. } => "file move",
                            Hunk::FileDel { .. } => "file del",
                            Hunk::FileUndel { .. } => "file undel",
                            Hunk::SolveNameConflict { .. } => "solve name conflict",
                            Hunk::UnsolveNameConflict { .. } => "unsolve name conflict",
                            Hunk::FileAdd { .. } => "file add",
                            Hunk::Edit { .. } => "edit",
                            Hunk::Replacement { .. } => "replacement",
                            Hunk::SolveOrderConflict { .. } => "solve order conflict",
                            Hunk::UnsolveOrderConflict { .. } => "unsolve order conflict",
                            Hunk::ResurrectZombies { .. } => "resurrect zombies",
                            Hunk::AddRoot { .. } => "root",
                            Hunk::DelRoot { .. } => "unroot",
                        },
                        line: ch.line(),
                    });
            }
            serde_json::to_writer_pretty(&mut std::io::stdout(), &changes)?;
            writeln!(stdout)?;
        } else if self.short {
            let mut changes = BTreeMap::new();
            for ch in change.changes.iter() {
                match ch {
                    Hunk::FileMove { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("MV")
                    }
                    Hunk::FileDel { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("D")
                    }
                    Hunk::FileUndel { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("UD")
                    }
                    Hunk::FileAdd { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("A")
                    }
                    Hunk::SolveNameConflict { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("SC")
                    }
                    Hunk::UnsolveNameConflict { path, .. } => {
                        changes.entry(path).or_insert(BTreeSet::new()).insert("UC")
                    }
                    Hunk::Edit {
                        local: Local { path, .. },
                        ..
                    } => changes.entry(path).or_insert(BTreeSet::new()).insert("M"),
                    Hunk::Replacement {
                        local: Local { path, .. },
                        ..
                    } => changes.entry(path).or_insert(BTreeSet::new()).insert("R"),
                    Hunk::SolveOrderConflict {
                        local: Local { path, .. },
                        ..
                    } => changes.entry(path).or_insert(BTreeSet::new()).insert("SC"),
                    Hunk::UnsolveOrderConflict {
                        local: Local { path, .. },
                        ..
                    } => changes.entry(path).or_insert(BTreeSet::new()).insert("SC"),
                    Hunk::ResurrectZombies {
                        local: Local { path, .. },
                        ..
                    } => changes.entry(path).or_insert(BTreeSet::new()).insert("RZ"),
                    Hunk::AddRoot { .. } | Hunk::DelRoot { .. } => true,
                };
            }
            let al = changes
                .iter()
                .map(|(_, v)| v.iter().map(|x| x.len()).sum::<usize>() + v.len() - 1)
                .max()
                .unwrap_or(0);
            let spaces: String = std::iter::repeat(' ').take(al).collect();
            for (k, v) in changes.iter() {
                let mut is_first = true;
                for v in v.iter() {
                    if is_first {
                        write!(stdout, "{}", v)?;
                    } else {
                        write!(stdout, ",{}", v)?;
                    }
                    is_first = false;
                }
                let (sp, _) = spaces.split_at(al - v.len());
                writeln!(stdout, "{} {}", sp, k)?;
            }
            if self.untracked {
                for path in untracked(&repo, &*txn_)? {
                    writeln!(stdout, "U {}", path.to_str().unwrap())?;
                }
            }
        } else if self.untracked {
            for path in untracked(&repo, &*txn_)? {
                writeln!(stdout, "{}", path.to_str().unwrap())?;
            }
        } else {
            match change.write(
                &repo.changes,
                None,
                true,
                Colored {
                    w: termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto),
                    colors,
                },
            ) {
                Ok(()) => {}
                Err(libpijul::change::TextSerError::Io(e))
                    if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                Err(e) => return Err(e.into()),
            }
        }
        if actions_is_empty && self.prefixes.is_empty() {
            use libpijul::ChannelMutTxnT;
            txn_.touch_channel(&mut *channel.write(), None);
            std::mem::drop(txn_);
            txn.commit()?;
        }
        Ok(())
    }

    fn fill_relative_prefixes(&mut self) -> Result<(), anyhow::Error> {
        let cwd = std::env::current_dir()?;
        for p in self.prefixes.iter_mut() {
            if p.is_relative() {
                *p = cwd.join(&p);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct Status {
    operation: &'static str,
    line: Option<usize>,
}

pub struct Colored<W> {
    pub w: W,
    pub colors: bool,
}

impl<W: std::io::Write> std::io::Write for Colored<W> {
    fn write(&mut self, s: &[u8]) -> Result<usize, std::io::Error> {
        self.w.write(s)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.w.flush()
    }
}

use termcolor::*;

impl<W: termcolor::WriteColor> libpijul::change::WriteChangeLine for Colored<W> {
    fn write_change_line(&mut self, pref: &str, contents: &str) -> Result<(), std::io::Error> {
        if self.colors {
            let col = if pref == "+" {
                Color::Green
            } else {
                Color::Red
            };
            self.w.set_color(ColorSpec::new().set_fg(Some(col)))?;
            writeln!(self.w, "{} {}", pref, contents)?;
            self.w.reset()
        } else {
            writeln!(self.w, "{} {}", pref, contents)
        }
    }
    fn write_change_line_binary(
        &mut self,
        pref: &str,
        contents: &[u8],
    ) -> Result<(), std::io::Error> {
        if self.colors {
            let col = if pref == "+" {
                Color::Green
            } else {
                Color::Red
            };
            self.w.set_color(ColorSpec::new().set_fg(Some(col)))?;
            write!(
                self.w,
                "{}b{}",
                pref,
                data_encoding::BASE64.encode(contents)
            )?;
            self.w.reset()
        } else {
            write!(
                self.w,
                "{}b{}",
                pref,
                data_encoding::BASE64.encode(contents)
            )
        }
    }
}

pub fn is_colored() -> bool {
    let mut colors = atty::is(atty::Stream::Stdout);
    if let Ok((global, _)) = crate::config::Global::load() {
        match global.colors {
            Some(crate::config::Choice::Always) => colors = true,
            Some(crate::config::Choice::Never) => colors = false,
            _ => {}
        }
        match global.pager {
            Some(crate::config::Choice::Never) => colors = false,
            _ => {
                super::pager();
            }
        }
    } else {
        colors &= super::pager();
    }
    colors
}

fn untracked<'a, T: TxnTExt>(
    repo: &Repository,
    txn: &'a T,
) -> Result<impl Iterator<Item = PathBuf> + 'a, anyhow::Error> {
    let repo_path = CanonicalPathBuf::canonicalize(&repo.path)?;
    let threads = num_cpus::get();
    Ok(repo
        .working_copy
        .iterate_prefix_rec(repo_path.clone(), repo_path.clone(), false, threads)?
        .filter_map(move |x| {
            let (path, _) = x.unwrap();
            use path_slash::PathExt;
            let path_str = path.to_slash_lossy();
            if !txn.is_tracked(&path_str).unwrap() {
                Some(path)
            } else {
                None
            }
        }))
}
