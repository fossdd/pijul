use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::repository::Repository;
use anyhow::bail;
use clap::Parser;
use libpijul::changestore::*;
use libpijul::pristine::{sanakirja::Txn, ChannelRef, DepsTxnT, GraphTxnT, TreeTxnT, TxnErr};
use libpijul::{Base32, TxnT, TxnTExt};
use serde::ser::{SerializeSeq, Serializer};
use serde::Serialize;
use thiserror::*;

/// A struct containing user-input assembled by Parser.
#[derive(Parser, Debug)]
pub struct Log {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Show logs for this channel instead of the current channel
    #[clap(long = "channel")]
    channel: Option<String>,
    /// Only show the change hashes
    #[clap(long = "hash-only")]
    hash_only: bool,
    /// Include state identifiers in the output
    #[clap(long = "state")]
    states: bool,
    /// Include full change description in the output
    #[clap(long = "description")]
    descriptions: bool,
    /// Start after this many changes
    #[clap(long = "offset")]
    offset: Option<usize>,
    /// Output at most this many changes
    #[clap(long = "limit")]
    limit: Option<usize>,
    #[clap(long = "output-format")]
    output_format: Option<String>,
    /// Filter log output, showing only log entries that touched the specified
    /// files. Accepted as a list of paths relative to your current directory.
    /// Currently, filters can only be applied when logging the channel that's
    /// in use.
    #[clap(last = true)]
    filters: Vec<String>,
}

impl TryFrom<Log> for LogIterator {
    type Error = anyhow::Error;
    fn try_from(cmd: Log) -> Result<LogIterator, Self::Error> {
        let repo = Repository::find_root(cmd.repo_path.clone())?;
        let repo_path = repo.path.clone();
        let txn = repo.pristine.txn_begin()?;
        let channel_name = if let Some(ref c) = cmd.channel {
            c
        } else {
            txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL)
        };
        // The only situation that's disallowed is if the user's trying to apply
        // path filters AND get the logs for a channel other than the one they're
        // currently using (where using means the one that comprises the working copy)
        if !cmd.filters.is_empty()
            && !(channel_name == txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL))
        {
            bail!("Currently, log filters can only be applied to the channel currently in use.")
        }

        let channel_ref = if let Some(channel) = txn.load_channel(channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };
        let changes = repo.changes;
        let limit = cmd.limit.unwrap_or(std::usize::MAX);
        let offset = cmd.offset.unwrap_or(0);

        let mut id_path = repo.path.join(libpijul::DOT_DIR);
        id_path.push("identities");
        Ok(Self {
            txn,
            cmd,
            changes,
            repo_path,
            id_path,
            channel_ref,
            limit,
            offset,
        })
    }
}

#[derive(Debug, Error)]
pub enum Error<E: std::error::Error> {
    #[error("pijul log couldn't find a file or directory corresponding to `{}`", 0)]
    NotFound(String),
    #[error(transparent)]
    Txn(#[from] libpijul::pristine::sanakirja::SanakirjaError),
    #[error(transparent)]
    TxnErr(#[from] TxnErr<libpijul::pristine::sanakirja::SanakirjaError>),
    #[error(transparent)]
    Fs(#[from] libpijul::FsError<libpijul::pristine::sanakirja::SanakirjaError>),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("pijul log couldn't assemble file prefix for pattern `{}`: {} was not a file in the repository at {}", pat, canon_path.display(), repo_path.display())]
    FilterPath {
        pat: String,
        canon_path: PathBuf,
        repo_path: PathBuf,
    },
    #[error("pijul log couldn't assemble file prefix for pattern `{}`: the path contained invalid UTF-8", 0)]
    InvalidUtf8(String),
    #[error(transparent)]
    E(E),
    #[error(transparent)]
    Filesystem(#[from] libpijul::changestore::filesystem::Error),
}

// A lot of error-handling noise here, but since we're dealing with
// a user-command and a bunch of file-IO/path manipulation it's
// probably necessary for the feedback to be good.
fn get_inodes<E: std::error::Error>(
    txn: &Txn,
    repo_path: &Path,
    pats: &[String],
) -> Result<Vec<libpijul::Inode>, Error<E>> {
    let mut inodes = Vec::new();
    for pat in pats {
        let canon_path = match Path::new(pat).canonicalize() {
            Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound) => {
                return Err(Error::NotFound(pat.to_string()))
            }
            Err(e) => return Err(e.into()),
            Ok(p) => p,
        };

        match canon_path.strip_prefix(repo_path).map(|p| p.to_str()) {
            // strip_prefix error is if repo_path is not a prefix of canon_path,
            // which would only happen if they pased in a filter path that's not
            // in the repository.
            Err(_) => {
                return Err(Error::FilterPath {
                    pat: pat.to_string(),
                    canon_path,
                    repo_path: repo_path.to_path_buf(),
                })
            }
            // PathBuf.to_str() returns none iff the path contains invalid UTF-8.
            Ok(None) => return Err(Error::InvalidUtf8(pat.to_string())),
            Ok(Some(s)) => inodes.push(libpijul::fs::find_inode(txn, s)?),
        };
    }
    log::debug!("log filters: {:#?}\n", pats);
    Ok(inodes)
}

/// Given a list of path filters which represent the files/directories for which
/// the user wants to see the logs, find the subset of relevant change hashes.
fn filtered_hashes<E: std::error::Error>(
    txn: &Txn,
    path: &Path,
    filters: &[String],
) -> Result<HashSet<libpijul::Hash>, Error<E>> {
    let inodes = get_inodes(txn, path, filters)?;
    let mut hashes = HashSet::<libpijul::Hash>::new();
    for inode in inodes {
        // The Position<ChangeId> for the file Inode.
        let inode_position = match txn.get_inodes(&inode, None)? {
            None => continue,
            Some(p) => p,
        };
        for pair in txn.iter_touched(inode_position)? {
            let (position, touched_change_id) = pair?;
            // Push iff the file ChangeId for this element matches that of the file Inode
            if &position.change == &inode_position.change {
                let ser_h = txn.get_external(touched_change_id)?.unwrap();
                hashes.insert(libpijul::Hash::from(*ser_h));
            } else {
                // We've gone past the relevant subset of changes in the iterator.
                break;
            }
        }
    }
    Ok(hashes)
}

/// A single log entry created by [`LogIterator`]. The fields are
/// all `Option<T>` so that users can more precisely choose what
/// data they want.
///
/// The implementaiton of [`std::fmt::Display`] is the standard method
/// of pretty-printing a `LogEntry` to the terminal.
#[derive(Serialize)]
#[serde(untagged)]
enum LogEntry {
    Full {
        #[serde(skip_serializing_if = "Option::is_none")]
        hash: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        authors: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timestamp: Option<chrono::DateTime<chrono::offset::Utc>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Hash(libpijul::Hash),
}

/// The standard pretty-print
impl std::fmt::Display for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            LogEntry::Full {
                hash,
                state,
                authors,
                timestamp,
                message,
                description,
            } => {
                if let Some(ref h) = hash {
                    writeln!(f, "Change {}", h)?;
                }
                if let Some(ref authors) = authors {
                    write!(f, "Author: ")?;
                    let mut is_first = true;
                    for a in authors.iter() {
                        if is_first {
                            is_first = false;
                            write!(f, "{}", a)?;
                        } else {
                            write!(f, ", {}", a)?;
                        }
                    }
                    // Write a linebreak after finishing the list of authors.
                    writeln!(f)?;
                }

                if let Some(ref timestamp) = timestamp {
                    writeln!(f, "Date: {}", timestamp)?;
                }
                if let Some(ref mrk) = state {
                    writeln!(f, "State: {}", mrk)?;
                }
                if let Some(ref message) = message {
                    writeln!(f, "\n    {}\n", message)?;
                }
                if let Some(ref description) = description {
                    writeln!(f, "\n    {}\n", description)?;
                }
            }
            LogEntry::Hash(h) => {
                writeln!(f, "{}", h.to_base32())?;
            }
        }
        Ok(())
    }
}

/// Contains state needed to produce the sequence of [`LogEntry`] items
/// that are to be logged. The implementation of `TryFrom<Log>` provides
/// a fallible way of creating one of these from the CLI's [`Log`] structure.
///
/// The two main things this provides are an efficient/streaming implementation
/// of [`serde::Serialize`], and an implementation of [`std::fmt::Display`] that
/// does the standard pretty-printing to stdout.
///
/// The [`LogIterator::for_each`] method lets us reuse the most code while providing both
/// pretty-printing and efficient serialization; we can't easily do this with
/// a full implementation of Iterator because serde's serialize method requires
/// self to be an immutable reference.
struct LogIterator {
    txn: Txn,
    changes: libpijul::changestore::filesystem::FileSystem,
    cmd: Log,
    repo_path: PathBuf,
    id_path: PathBuf,
    channel_ref: ChannelRef<Txn>,
    limit: usize,
    offset: usize,
}

/// This implementation of Serialize is hand-rolled in order
/// to allow for greater re-use and efficiency.
impl Serialize for LogIterator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        match self.for_each(|entry| seq.serialize_element(&entry)) {
            Ok(_) => seq.end(),
            Err(anyhow_err) => Err(serde::ser::Error::custom(anyhow_err)),
        }
    }
}

/// Pretty-prints all of the requested log entries in the standard
/// user-facing format.
impl std::fmt::Display for LogIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.for_each(|entry| write!(f, "{}", entry)) {
            Err(e) => {
                log::error!("LogIterator::Display: {}", e);
                Err(std::fmt::Error)
            }
            _ => Ok(()),
        }
    }
}

impl LogIterator {
    /// Call `f` on each [`LogEntry`] in a [`LogIterator`].
    ///
    /// The purpose of this is to let us execute a function over the log entries
    /// without having to duplicate the iteration/filtering logic or
    /// having to collect all of the elements first.
    fn for_each<A, E: std::error::Error>(
        &self,
        mut f: impl FnMut(LogEntry) -> Result<A, E>,
    ) -> Result<(), Error<E>> {
        // A cache of authors to keys. Prevents us from having to do
        // a lot of file-io for looking up the same author multiple times.
        let mut authors = HashMap::new();
        let mut id_path = self.id_path.clone();
        // If the user applied path filters, figure out what change hashes
        // are to be logged.
        let mut requested_hashes = filtered_hashes(
            &self.txn,
            self.repo_path.as_ref(),
            self.cmd.filters.as_slice(),
        )?;

        // Get the (Hash, Merkle) pairs for the portion of reverse_log
        // that are between offset and limit.
        let hs = self
            .txn
            .reverse_log(&*self.channel_ref.read(), None)?
            .skip(self.offset)
            .take(self.limit)
            .map(|res| {
                res.map(|(_, (ser_h, ser_m))| {
                    (libpijul::Hash::from(ser_h), libpijul::Merkle::from(ser_m))
                })
            });

        for pr in hs {
            let (h, mrk) = pr?;
            if (self.cmd.filters.is_empty()) || requested_hashes.remove(&h) {
                // If there were no path filters applied, OR is this was one of the hashes
                // marked by the file filters that were applied
                let entry = self.mk_log_entry(&mut authors, &mut id_path, h, Some(mrk))?;
                f(entry).map_err(Error::E)?;
            } else if requested_hashes.is_empty() {
                // If the user applied path filters, but the relevant change hashes
                // have been exhausted, we can break early.
                break;
            } else {
                // The user applied path filters; this wasn't a hit, but
                // there are still hits to be logged.
                continue;
            }
        }

        Ok(())
    }

    /// Create a [`LogEntry`] for a given hash.
    ///
    /// Most of this is just getting the right key information from either the cache
    /// or from the relevant file.
    fn mk_log_entry<'x, E: std::error::Error>(
        &self,
        author_kvs: &'x mut HashMap<String, String>,
        id_path: &mut PathBuf,
        h: libpijul::Hash,
        m: Option<libpijul::Merkle>,
    ) -> Result<LogEntry, Error<E>> {
        if self.cmd.hash_only {
            return Ok(LogEntry::Hash(h));
        }
        let header = self.changes.get_header(&h.into())?;
        let authors = header
            .authors
            .into_iter()
            .map(|mut auth| {
                let auth = if let Some(k) = auth.0.remove("key") {
                    match author_kvs.entry(k) {
                        Entry::Occupied(e) => e.into_mut(),
                        Entry::Vacant(e) => {
                            let mut id = None;
                            id_path.push(e.key());
                            if let Ok(f) = std::fs::File::open(&self.id_path) {
                                if let Ok(id_) = serde_json::from_reader::<_, super::Identity>(f) {
                                    id = Some(id_)
                                }
                            }
                            id_path.pop();
                            if let Some(id) = id {
                                e.insert(id.login)
                            } else {
                                let k = e.key().to_string();
                                e.insert(k)
                            }
                        }
                    }
                } else {
                    auth.0.get("name").unwrap()
                };
                auth.to_owned()
            })
            .collect();
        Ok(LogEntry::Full {
            hash: Some(h.to_base32()),
            state: m.map(|mm| mm.to_base32()).filter(|_| self.cmd.states),
            authors: Some(authors),
            timestamp: Some(header.timestamp),
            message: Some(header.message.clone()),
            description: header.description,
        })
    }
}

impl Log {
    // In order to accommodate both pretty-printing and efficient
    // serialization to a serde target format, this now delegates
    // mostly to [`LogIterator`].
    pub fn run(self) -> Result<(), anyhow::Error> {
        let mut stdout = std::io::stdout();
        match self.output_format.as_ref().map(|s| s.as_str()) {
            Some(s) if s.eq_ignore_ascii_case("json") => {
                serde_json::to_writer_pretty(&mut stdout, &LogIterator::try_from(self)?)?
            }
            _ => {
                super::pager();
                LogIterator::try_from(self)?.for_each(|entry| {
                    match write!(&mut stdout, "{}", entry) {
                        Ok(_) => Ok(()),
                        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
                        Err(e) => Err(e),
                    }
                })?
            }
        }
        Ok(())
    }
}
