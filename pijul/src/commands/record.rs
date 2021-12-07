use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use canonical_path::{CanonicalPath, CanonicalPathBuf};
use chrono::Utc;
use clap::Parser;
use libpijul::change::*;
use libpijul::changestore::*;
use libpijul::{
    ArcTxn, Base32, ChannelMutTxnT, ChannelRef, ChannelTxnT, MutTxnTExt, TxnT, TxnTExt,
};
use libpijul::{HashMap, HashSet};
use log::debug;

use crate::repository::*;

#[derive(Parser, Debug)]
pub struct Record {
    /// Record all paths that have changed
    #[clap(short = 'a', long = "all")]
    pub all: bool,
    /// Set the change message
    #[clap(short = 'm', long = "message")]
    pub message: Option<String>,
    /// Set the author field
    #[clap(long = "author")]
    pub author: Option<String>,
    /// Record the change in this channel instead of the current channel
    #[clap(long = "channel")]
    pub channel: Option<String>,
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    pub repo_path: Option<PathBuf>,
    /// Set the timestamp field
    #[clap(long = "timestamp", validator = timestamp_validator)]
    pub timestamp: Option<i64>,
    /// Ignore missing (deleted) files
    #[clap(long = "ignore-missing")]
    pub ignore_missing: bool,
    #[clap(long = "working-copy")]
    pub working_copy: Option<String>,
    /// Amend this change instead of creating a new change
    #[clap(long = "amend")]
    #[allow(clippy::option_option)]
    pub amend: Option<Option<String>>,
    /// Paths in which to record the changes
    pub prefixes: Vec<PathBuf>,
}

pub(crate) fn timestamp_validator(s: &str) -> Result<(), &'static str> {
    if let Ok(t) = s.parse() {
        if chrono::NaiveDateTime::from_timestamp_opt(t, 0).is_some() {
            return Ok(());
        }
    }
    Err("Could not parse timestamp")
}

impl Record {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone())?;
        let mut stdout = std::io::stdout();
        let mut stderr = std::io::stderr();

        for h in repo.config.hooks.record.iter() {
            h.run()?
        }
        let txn = repo.pristine.arc_txn_begin()?;
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
        let mut channel = if let Some(channel) = txn.read().load_channel(&channel)? {
            channel
        } else {
            bail!("Channel {:?} not found", channel);
        };

        let mut extra = Vec::new();
        for h in repo.config.extra_dependencies.iter() {
            let (h, c) = txn.read().hash_from_prefix(h)?;
            if txn
                .read()
                .get_changeset(txn.read().changes(&*channel.read()), &c)?
                .is_none()
            {
                bail!(
                    "Change {:?} (from .pijul/config) is not on channel {:?}",
                    h,
                    channel.read().name
                )
            }
            extra.push(h)
        }

        let header = if let Some(ref amend) = self.amend {
            let h = if let Some(ref hash) = amend {
                txn.read().hash_from_prefix(hash)?.0
            } else if let Some(h) = txn.read().reverse_log(&*channel.read(), None)?.next() {
                (h?.1).0.into()
            } else {
                return Ok(());
            };
            let header = if let Some(message) = self.message.clone() {
                ChangeHeader {
                    message,
                    ..repo.changes.get_header(&h)?
                }
            } else {
                repo.changes.get_header(&h)?
            };

            txn.write().unrecord(
                &repo.changes,
                &mut channel,
                &h,
                self.timestamp.unwrap_or(0) as u64,
            )?;
            header
        } else {
            self.header()?
        };
        let no_prefixes =
            self.prefixes.is_empty() && !self.ignore_missing && self.working_copy.is_none();
        let (repo_path, working_copy) = if let Some(ref w) = self.working_copy {
            (
                CanonicalPathBuf::canonicalize(w)?,
                Some(libpijul::working_copy::filesystem::FileSystem::from_root(w)),
            )
        } else {
            (CanonicalPathBuf::canonicalize(&repo.path)?, None)
        };

        let key = super::load_key()?;

        txn.write()
            .apply_root_change_if_needed(&repo.changes, &channel, rand::thread_rng())?;

        let result = self.record(
            txn,
            channel.clone(),
            working_copy.as_ref().unwrap_or(&repo.working_copy),
            &repo.changes,
            repo_path,
            header,
            &extra,
        )?;
        match result {
            Either::A((txn, mut change, updates, oldest)) => {
                let hash = repo.changes.save_change(&mut change, |change, hash| {
                    change.unhashed = Some(serde_json::json!({
                        "signature": key.sign_raw(&hash.to_bytes()).unwrap(),
                    }));
                    Ok::<_, anyhow::Error>(())
                })?;

                let mut txn_ = txn.write();
                txn_.apply_local_change(&mut channel, &change, &hash, &updates)?;
                let mut path = repo.path.join(libpijul::DOT_DIR);
                path.push("identities");
                std::fs::create_dir_all(&path)?;
                path.push("publickey.json");
                std::fs::File::create(&path)?;

                writeln!(stdout, "Hash: {}", hash.to_base32())?;
                debug!("oldest = {:?}", oldest);
                if no_prefixes {
                    let mut oldest = oldest
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as u64;
                    if oldest == 0 {
                        // If no diff was done at all, it means that no
                        // existing file changed since last time (some
                        // files may have been added, deleted or moved,
                        // but `touch` isn't about those).
                        oldest = std::time::SystemTime::now()
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as u64;
                    }
                    txn_.touch_channel(&mut *channel.write(), Some(oldest));
                }
                std::mem::drop(txn_);
                txn.commit()?;
            }
            Either::B(txn) => {
                if no_prefixes {
                    txn.write().touch_channel(&mut *channel.write(), None);
                    txn.commit()?;
                }
                writeln!(stderr, "Nothing to record")?;
            }
        }
        Ok(())
    }

    fn header(&self) -> Result<ChangeHeader, anyhow::Error> {
        let config = crate::config::Global::load();
        let mut authors = Vec::new();
        let mut b = std::collections::BTreeMap::new();
        if let Some(ref a) = self.author {
            b.insert("name".to_string(), a.clone());
        } else if let Some(mut dir) = crate::config::global_config_dir() {
            dir.push("publickey.json");
            if let Ok(key) = std::fs::File::open(&dir) {
                let k: libpijul::key::PublicKey = serde_json::from_reader(key).unwrap();
                b.insert("key".to_string(), k.key);
            } else {
                bail!("No identity configured yet. Please use `pijul key` to create one")
            }
        }
        authors.push(Author(b));
        let templates = config
            .as_ref()
            .ok()
            .and_then(|(cfg, _)| cfg.template.as_ref());
        let message = if let Some(message) = &self.message {
            message.clone()
        } else if let Some(message_file) = templates.and_then(|t| t.message.as_ref()) {
            match std::fs::read_to_string(message_file) {
                Ok(m) => m,
                Err(e) => bail!("Could not read message template: {:?}: {}", message_file, e),
            }
        } else {
            String::new()
        };
        let description = if let Some(descr_file) = templates.and_then(|t| t.description.as_ref()) {
            match std::fs::read_to_string(descr_file) {
                Ok(d) => Some(d),
                Err(e) => bail!(
                    "Could not read description template: {:?}: {}",
                    descr_file,
                    e
                ),
            }
        } else {
            None
        };
        let header = ChangeHeader {
            message,
            authors,
            description,
            timestamp: if let Some(t) = self.timestamp {
                chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(t, 0), chrono::Utc)
            } else {
                Utc::now()
            },
        };
        Ok(header)
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

    fn record<
        T: TxnTExt + MutTxnTExt + Sync + Send + 'static,
        C: ChangeStore + Send + Clone + 'static,
    >(
        mut self,
        txn: ArcTxn<T>,
        channel: ChannelRef<T>,
        working_copy: &libpijul::working_copy::FileSystem,
        changes: &C,
        repo_path: CanonicalPathBuf,
        header: ChangeHeader,
        extra_deps: &[libpijul::Hash],
    ) -> Result<
        Either<
            (
                ArcTxn<T>,
                Change,
                HashMap<usize, libpijul::InodeUpdate>,
                std::time::SystemTime,
            ),
            ArcTxn<T>,
        >,
        anyhow::Error,
    > {
        let mut state = libpijul::RecordBuilder::new();
        if self.ignore_missing {
            state.ignore_missing = true;
        }
        if self.prefixes.is_empty() {
            if self.ignore_missing {
                for f in ignore::Walk::new(&repo_path) {
                    let f = f?;
                    if f.metadata()?.is_file() {
                        let p = CanonicalPath::new(f.path())?;
                        let p = p.as_path().strip_prefix(&repo_path).unwrap();
                        state.record(
                            txn.clone(),
                            libpijul::Algorithm::default(),
                            &libpijul::DEFAULT_SEPARATOR,
                            channel.clone(),
                            working_copy,
                            changes,
                            p.to_str().unwrap(),
                            1, // num_cpus::get(),
                        )?
                    }
                }
            } else {
                state.record(
                    txn.clone(),
                    libpijul::Algorithm::default(),
                    &libpijul::DEFAULT_SEPARATOR,
                    channel.clone(),
                    working_copy,
                    changes,
                    "",
                    1, // num_cpus::get(),
                )?
            }
        } else {
            self.fill_relative_prefixes()?;
            working_copy.record_prefixes(
                txn.clone(),
                channel.clone(),
                changes,
                &mut state,
                repo_path,
                &self.prefixes,
                1, // num_cpus::get(),
                self.timestamp.unwrap_or(0) as u64,
            )?;
        }

        let mut rec = state.finish();
        if rec.actions.is_empty() {
            return Ok(Either::B(txn));
        }
        debug!("TAKING LOCK {}", line!());
        let txn_ = txn.write();
        let actions = rec
            .actions
            .into_iter()
            .map(|rec| rec.globalize(&*txn_).unwrap())
            .collect();
        let contents = if let Ok(c) = Arc::try_unwrap(rec.contents) {
            c.into_inner()
        } else {
            unreachable!()
        };
        let mut change =
            LocalChange::make_change(&*txn_, &channel, actions, contents, header, Vec::new())?;

        let current: HashSet<_> = change.dependencies.iter().cloned().collect();
        for dep in extra_deps.iter() {
            if !current.contains(dep) {
                change.dependencies.push(*dep)
            }
        }

        debug!("has_binary = {:?}", rec.has_binary_files);
        let mut change = if self.all {
            change
        } else if rec.has_binary_files {
            bail!("Cannot record a binary change interactively. Please use -a.")
        } else {
            let mut o = Vec::new();
            debug!("write change");
            change.write(changes, None, true, &mut o)?;
            debug!("write change done");

            let mut with_errors: Option<Vec<u8>> = None;
            let change = loop {
                let mut bytes = if let Some(ref o) = with_errors {
                    edit::edit_bytes(&o[..])?
                } else {
                    edit::edit_bytes(&o[..])?
                };
                if bytes.iter().all(|c| (*c as char).is_whitespace()) {
                    bail!("Empty change")
                }
                let mut change = std::io::BufReader::new(std::io::Cursor::new(&bytes));
                if let Ok(change) =
                    Change::read_and_deps(&mut change, &mut rec.updatables, &*txn_, &channel)
                {
                    break change;
                }

                let mut err = SYNTAX_ERROR.as_bytes().to_vec();
                err.append(&mut bytes);
                with_errors = Some(err)
            };
            if change.changes.is_empty() {
                bail!("Empty change")
            }
            change
        };

        let current: HashSet<_> = change.dependencies.iter().cloned().collect();
        for dep in extra_deps.iter() {
            if !current.contains(dep) {
                change.dependencies.push(*dep)
            }
        }

        if change.header.message.trim().is_empty() {
            bail!("No change message")
        }
        debug!("saving change");
        std::mem::drop(txn_);
        Ok(Either::A((txn, change, rec.updatables, rec.oldest_change)))
    }
}

enum Either<A, B> {
    A(A),
    B(B),
}

const SYNTAX_ERROR: &str = "# Syntax errors, please try again.
# Alternatively, you may delete the entire file (including this
# comment) to abort.
";
