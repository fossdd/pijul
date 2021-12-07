#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate pijul_macros;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate lazy_static;
#[cfg(test)]
#[macro_use]
extern crate quickcheck;

pub mod alive;
mod apply;
pub mod change;
pub mod changestore;
mod diff;
pub mod find_alive;
pub mod fs;
mod missing_context;
pub mod output;
pub mod path;
pub mod pristine;
pub mod record;
pub mod small_string;
mod text_encoding;
mod unrecord;
mod vector2;
pub mod vertex_buffer;
pub mod working_copy;

pub mod key;
pub mod tag;

mod chardetng;

#[cfg(test)]
mod tests;

pub const DOT_DIR: &str = ".pijul";

#[derive(Debug, Error)]
#[error("Parse error: {:?}", s)]
pub struct ParseError {
    s: String,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum RemoteError {
    #[error("Repository not found: {}", url)]
    RepositoryNotFound { url: String },
    #[error("Channel {} not found for repository {}", channel, url)]
    ChannelNotFound { channel: String, url: String },
    #[error("Ambiguous path: {}", path)]
    AmbiguousPath { path: String },
    #[error("Path not found: {}", path)]
    PathNotFound { path: String },
    #[error("Change not found: {}", change)]
    ChangeNotFound { change: String },
}

pub use crate::apply::Workspace as ApplyWorkspace;
pub use crate::apply::{apply_change_arc, ApplyError, LocalApplyError};
pub use crate::diff::DEFAULT_SEPARATOR;
pub use crate::fs::{FsError, WorkingCopyIterator};
pub use crate::output::{Archive, Conflict};
pub use crate::pristine::{
    ArcTxn, Base32, ChangeId, ChannelMutTxnT, ChannelRef, ChannelTxnT, DepsTxnT, EdgeFlags,
    GraphTxnT, Hash, Inode, Merkle, MutTxnT, OwnedPathId, RemoteRef, TreeTxnT, TxnT, Vertex,
};
pub use crate::record::Builder as RecordBuilder;
pub use crate::record::{Algorithm, InodeUpdate};
pub use crate::unrecord::UnrecordError;

// Making hashmaps deterministic (for testing)
#[cfg(feature = "deterministic_hash")]
pub type Hasher = std::hash::BuildHasherDefault<twox_hash::XxHash64>;
#[cfg(not(feature = "deterministic_hash"))]
pub type Hasher = std::collections::hash_map::RandomState;

pub type HashMap<K, V> = std::collections::HashMap<K, V, Hasher>;
pub type HashSet<K> = std::collections::HashSet<K, Hasher>;

impl MutTxnTExt for pristine::sanakirja::MutTxn<()> {}
impl TxnTExt for pristine::sanakirja::MutTxn<()> {}
impl TxnTExt for pristine::sanakirja::Txn {}

pub fn commit<T: pristine::MutTxnT>(
    txn: std::sync::Arc<std::sync::RwLock<T>>,
) -> Result<(), T::GraphError> {
    let txn = if let Ok(txn) = std::sync::Arc::try_unwrap(txn) {
        txn.into_inner().unwrap()
    } else {
        unreachable!()
    };
    txn.commit()
}

pub trait MutTxnTExt: pristine::MutTxnT {
    fn apply_root_change_if_needed<C: changestore::ChangeStore, R: rand::Rng>(
        &mut self,
        changes: &C,
        channel: &ChannelRef<Self>,
        rng: R,
    ) -> Result<
        Option<(pristine::Hash, u64, pristine::Merkle)>,
        crate::apply::ApplyError<C::Error, Self::GraphError>,
    > {
        crate::apply::apply_root_change(self, channel, changes, rng)
    }

    fn apply_change_ws<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &mut Self::Channel,
        hash: &crate::pristine::Hash,
        workspace: &mut ApplyWorkspace,
    ) -> Result<(u64, pristine::Merkle), crate::apply::ApplyError<C::Error, Self::GraphError>> {
        crate::apply::apply_change_ws(changes, self, channel, hash, workspace)
    }

    fn apply_change_rec_ws<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &mut Self::Channel,
        hash: &crate::pristine::Hash,
        workspace: &mut ApplyWorkspace,
    ) -> Result<(), crate::apply::ApplyError<C::Error, Self::GraphError>> {
        crate::apply::apply_change_rec_ws(changes, self, channel, hash, workspace, false)
    }

    fn apply_change<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &mut Self::Channel,
        hash: &pristine::Hash,
    ) -> Result<(u64, pristine::Merkle), crate::apply::ApplyError<C::Error, Self::GraphError>> {
        crate::apply::apply_change(changes, self, channel, hash)
    }

    fn apply_change_rec<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &mut Self::Channel,
        hash: &pristine::Hash,
    ) -> Result<(), crate::apply::ApplyError<C::Error, Self::GraphError>> {
        crate::apply::apply_change_rec(changes, self, channel, hash, false)
    }

    fn apply_deps_rec<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &mut Self::Channel,
        hash: &pristine::Hash,
    ) -> Result<(), crate::apply::ApplyError<C::Error, Self::GraphError>> {
        crate::apply::apply_change_rec(changes, self, channel, hash, true)
    }

    fn apply_local_change_ws(
        &mut self,
        channel: &pristine::ChannelRef<Self>,
        change: &change::Change,
        hash: &pristine::Hash,
        inode_updates: &HashMap<usize, InodeUpdate>,
        workspace: &mut ApplyWorkspace,
    ) -> Result<(u64, pristine::Merkle), crate::apply::LocalApplyError<Self::GraphError>> {
        crate::apply::apply_local_change_ws(self, channel, change, hash, inode_updates, workspace)
    }

    fn apply_local_change(
        &mut self,
        channel: &crate::pristine::ChannelRef<Self>,
        change: &crate::change::Change,
        hash: &pristine::Hash,
        inode_updates: &HashMap<usize, InodeUpdate>,
    ) -> Result<(u64, pristine::Merkle), crate::apply::LocalApplyError<Self::GraphError>> {
        crate::apply::apply_local_change(self, channel, change, hash, inode_updates)
    }

    fn apply_recorded<C: changestore::ChangeStore>(
        &mut self,
        channel: &mut pristine::ChannelRef<Self>,
        recorded: record::Recorded,
        changestore: &C,
    ) -> Result<pristine::Hash, crate::apply::ApplyError<C::Error, Self::GraphError>> {
        let contents_hash = {
            let mut hasher = pristine::Hasher::default();
            hasher.update(&recorded.contents.lock()[..]);
            hasher.finish()
        };
        let mut change = change::LocalChange {
            offsets: change::Offsets::default(),
            hashed: change::Hashed {
                version: change::VERSION,
                contents_hash,
                changes: recorded
                    .actions
                    .into_iter()
                    .map(|rec| rec.globalize(self).unwrap())
                    .collect(),
                metadata: Vec::new(),
                dependencies: Vec::new(),
                extra_known: Vec::new(),
                header: change::ChangeHeader::default(),
            },
            unhashed: None,
            contents: std::sync::Arc::try_unwrap(recorded.contents)
                .unwrap()
                .into_inner(),
        };
        let hash = changestore
            .save_change(&mut change, |_, _| Ok(()))
            .map_err(apply::ApplyError::Changestore)?;
        apply::apply_local_change(self, channel, &change, &hash, &recorded.updatables)?;
        Ok(hash)
    }

    fn unrecord<C: changestore::ChangeStore>(
        &mut self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        hash: &pristine::Hash,
        salt: u64,
    ) -> Result<bool, unrecord::UnrecordError<C::Error, Self::GraphError>> {
        unrecord::unrecord(self, channel, changes, hash, salt)
    }

    /// Register a file in the working copy, where the file is given by
    /// its path from the root of the repository, where the components of
    /// the path are separated by `/` (example path: `a/b/c`).
    fn add_file(&mut self, path: &str, salt: u64) -> Result<Inode, fs::FsError<Self::GraphError>> {
        fs::add_inode(self, None, path, false, salt)
    }

    /// Register a directory in the working copy, where the directory is
    /// given by its path from the root of the repository, where the
    /// components of the path are separated by `/` (example path:
    /// `a/b/c`).
    fn add_dir(&mut self, path: &str, salt: u64) -> Result<Inode, fs::FsError<Self::GraphError>> {
        fs::add_inode(self, None, path, true, salt)
    }

    /// Register a file or directory in the working copy, given by its
    /// path from the root of the repository, where the components of the
    /// path are separated by `/` (example path: `a/b/c`).
    fn add(
        &mut self,
        path: &str,
        is_dir: bool,
        salt: u64,
    ) -> Result<Inode, fs::FsError<Self::GraphError>> {
        fs::add_inode(self, None, path, is_dir, salt)
    }

    fn move_file(
        &mut self,
        a: &str,
        b: &str,
        salt: u64,
    ) -> Result<(), fs::FsError<Self::GraphError>> {
        fs::move_file(self, a, b, salt)
    }

    fn remove_file(&mut self, a: &str) -> Result<(), fs::FsError<Self::GraphError>> {
        fs::remove_file(self, a)
    }

    fn archive_with_state<P: changestore::ChangeStore, A: Archive>(
        &mut self,
        changes: &P,
        channel: &mut pristine::ChannelRef<Self>,
        state: &pristine::Merkle,
        extra: &[pristine::Hash],
        arch: &mut A,
        salt: u64,
    ) -> Result<Vec<output::Conflict>, output::ArchiveError<P::Error, Self::GraphError, A::Error>>
    {
        self.archive_prefix_with_state(
            changes,
            channel,
            state,
            extra,
            &mut std::iter::empty(),
            arch,
            salt,
        )
    }

    /// Warning: this method unrecords changes until finding the
    /// state. If this is not wanted, please fork the channel before
    /// calling.
    fn archive_prefix_with_state<
        'a,
        P: changestore::ChangeStore,
        A: Archive,
        I: Iterator<Item = &'a str>,
    >(
        &mut self,
        changes: &P,
        channel: &mut pristine::ChannelRef<Self>,
        state: &pristine::Merkle,
        extra: &[pristine::Hash],
        prefix: &mut I,
        arch: &mut A,
        salt: u64,
    ) -> Result<Vec<output::Conflict>, output::ArchiveError<P::Error, Self::GraphError, A::Error>>
    {
        let mut unrecord = Vec::new();
        let mut found = false;
        for x in pristine::changeid_rev_log(self, &channel.read(), None)? {
            let (_, p) = x?;
            let m: Merkle = (&p.b).into();
            if &m == state {
                found = true;
                break;
            } else {
                unrecord.push(p.a.into())
            }
        }
        debug!("unrecord = {:?}", unrecord);
        if found {
            for h in unrecord.iter() {
                let h = self.get_external(h)?.unwrap().into();
                self.unrecord(changes, channel, &h, salt)?;
            }
            {
                let mut channel_ = channel.write();
                for app in extra.iter() {
                    self.apply_change_rec(changes, &mut channel_, app)?
                }
            }
            output::archive(changes, self, channel, prefix, arch)
        } else {
            Err(output::ArchiveError::StateNotFound { state: *state })
        }
    }
}

pub trait TxnTExt: pristine::TxnT {
    fn is_directory(&self, inode: pristine::Inode) -> Result<bool, Self::TreeError> {
        fs::is_directory(self, inode).map_err(|e| e.0)
    }

    fn is_tracked(&self, path: &str) -> Result<bool, Self::TreeError> {
        fs::is_tracked(self, path).map_err(|e| e.0)
    }

    fn iter_working_copy(&self) -> WorkingCopyIterator<Self> {
        fs::iter_working_copy(self, pristine::Inode::ROOT)
    }

    fn iter_graph_children<'txn, 'changes, P>(
        &'txn self,
        changes: &'changes P,
        channel: &'txn Self::Channel,
        key: pristine::Position<ChangeId>,
    ) -> Result<fs::GraphChildren<'txn, 'changes, Self, P>, Self::GraphError>
    where
        P: changestore::ChangeStore,
    {
        fs::iter_graph_children(self, changes, &self.graph(channel), key)
    }

    fn has_change(
        &self,
        channel: &pristine::ChannelRef<Self>,
        hash: &pristine::Hash,
    ) -> Result<Option<u64>, Self::GraphError> {
        if let Some(cid) = pristine::GraphTxnT::get_internal(self, &hash.into()).map_err(|e| e.0)? {
            self.get_changeset(self.changes(&channel.read()), cid)
                .map_err(|e| e.0)
                .map(|x| x.map(|x| u64::from_le(x.0)))
        } else {
            Ok(None)
        }
    }

    fn is_alive(
        &self,
        channel: &Self::Channel,
        a: &pristine::Vertex<pristine::ChangeId>,
    ) -> Result<bool, Self::GraphError> {
        pristine::is_alive(self, self.graph(channel), a).map_err(|e| e.0)
    }

    fn current_state(&self, channel: &Self::Channel) -> Result<pristine::Merkle, Self::GraphError> {
        pristine::current_state(self, channel).map_err(|e| e.0)
    }

    fn log<'channel, 'txn>(
        &'txn self,
        channel: &'channel Self::Channel,
        from: u64,
    ) -> Result<Log<'txn, Self>, Self::GraphError> {
        Ok(Log {
            txn: self,
            iter: pristine::changeid_log(self, channel, pristine::L64(from.to_le()))
                .map_err(|e| e.0)?,
        })
    }

    fn log_for_path<'channel, 'txn>(
        &'txn self,
        channel: &'channel Self::Channel,
        pos: pristine::Position<pristine::ChangeId>,
        from: u64,
    ) -> Result<pristine::PathChangeset<'channel, 'txn, Self>, Self::GraphError> {
        pristine::log_for_path(self, channel, pos, from).map_err(|e| e.0)
    }

    fn rev_log_for_path<'channel, 'txn>(
        &'txn self,
        channel: &'channel Self::Channel,
        pos: pristine::Position<pristine::ChangeId>,
        from: u64,
    ) -> Result<pristine::RevPathChangeset<'channel, 'txn, Self>, Self::DepsError> {
        pristine::rev_log_for_path(self, channel, pos, from).map_err(|e| e.0)
    }

    fn reverse_log<'channel, 'txn>(
        &'txn self,
        channel: &'channel Self::Channel,
        from: Option<u64>,
    ) -> Result<RevLog<'txn, Self>, Self::GraphError> {
        Ok(RevLog {
            txn: self,
            iter: pristine::changeid_rev_log(self, channel, from.map(|x| pristine::L64(x.to_le())))
                .map_err(|e| e.0)?,
        })
    }

    fn changeid_reverse_log<'txn>(
        &'txn self,
        channel: &Self::Channel,
        from: Option<pristine::L64>,
    ) -> Result<
        pristine::RevCursor<
            Self,
            &'txn Self,
            Self::RevchangesetCursor,
            pristine::L64,
            pristine::Pair<pristine::ChangeId, pristine::SerializedMerkle>,
        >,
        Self::GraphError,
    > {
        pristine::changeid_rev_log(self, channel, from).map_err(|e| e.0)
    }

    fn get_changes(
        &self,
        channel: &pristine::ChannelRef<Self>,
        n: u64,
    ) -> Result<Option<(pristine::Hash, pristine::Merkle)>, Self::GraphError> {
        if let Some(p) = self
            .get_revchangeset(self.rev_changes(&channel.read()), &pristine::L64(n.to_le()))
            .map_err(|e| e.0)?
        {
            Ok(Some((
                self.get_external(&p.a.into())
                    .map_err(|e| e.0)?
                    .unwrap()
                    .into(),
                (&p.b).into(),
            )))
        } else {
            Ok(None)
        }
    }

    fn get_revchanges(
        &self,
        channel: &pristine::ChannelRef<Self>,
        h: &pristine::Hash,
    ) -> Result<Option<u64>, Self::GraphError> {
        if let Some(h) = pristine::GraphTxnT::get_internal(self, &h.into()).map_err(|e| e.0)? {
            self.get_changeset(self.changes(&channel.read()), h)
                .map_err(|e| e.0)
                .map(|x| x.map(|x| u64::from_le(x.0)))
        } else {
            Ok(None)
        }
    }

    fn touched_files(&self, h: &pristine::Hash) -> Result<Option<Touched<Self>>, Self::DepsError> {
        if let Some(id) = pristine::GraphTxnT::get_internal(self, &h.into()).map_err(|e| e.0)? {
            Ok(Some(Touched {
                txn: self,
                iter: self.iter_rev_touched_files(id, None).map_err(|e| e.0)?,
                id: *id,
            }))
        } else {
            Ok(None)
        }
    }

    fn find_oldest_path<C: changestore::ChangeStore>(
        &self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        position: &pristine::Position<pristine::Hash>,
    ) -> Result<Option<(String, bool)>, output::FileError<C::Error, Self::GraphError>> {
        let position = pristine::Position {
            change: *pristine::GraphTxnT::get_internal(self, &position.change.into())?.unwrap(),
            pos: position.pos,
        };
        fs::find_path(changes, self, &channel.read(), false, position)
    }

    fn find_youngest_path<C: changestore::ChangeStore>(
        &self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        position: pristine::Position<pristine::Hash>,
    ) -> Result<Option<(String, bool)>, output::FileError<C::Error, Self::GraphError>> {
        let position = pristine::Position {
            change: *pristine::GraphTxnT::get_internal(self, &position.change.into())?.unwrap(),
            pos: position.pos,
        };
        fs::find_path(changes, self, &channel.read(), true, position)
    }

    fn follow_oldest_path<C: changestore::ChangeStore>(
        &self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        path: &str,
    ) -> Result<
        (pristine::Position<pristine::ChangeId>, bool),
        fs::FsErrorC<C::Error, Self::GraphError>,
    > {
        fs::follow_oldest_path(changes, self, &channel.read(), path)
    }

    fn archive<C: changestore::ChangeStore, A: Archive>(
        &self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        arch: &mut A,
    ) -> Result<Vec<output::Conflict>, output::ArchiveError<C::Error, Self::GraphError, A::Error>>
    {
        output::archive(changes, self, channel, &mut std::iter::empty(), arch)
    }

    fn archive_prefix<'a, C: changestore::ChangeStore, I: Iterator<Item = &'a str>, A: Archive>(
        &self,
        changes: &C,
        channel: &pristine::ChannelRef<Self>,
        prefix: &mut I,
        arch: &mut A,
    ) -> Result<Vec<output::Conflict>, output::ArchiveError<C::Error, Self::GraphError, A::Error>>
    {
        output::archive(changes, self, channel, prefix, arch)
    }

    fn iter_adjacent<'txn>(
        &'txn self,
        graph: &'txn Self::Channel,
        key: Vertex<pristine::ChangeId>,
        min_flag: pristine::EdgeFlags,
        max_flag: pristine::EdgeFlags,
    ) -> Result<pristine::AdjacentIterator<'txn, Self>, pristine::TxnErr<Self::GraphError>> {
        pristine::iter_adjacent(self, self.graph(graph), key, min_flag, max_flag)
    }
}

pub struct Log<'txn, T: pristine::ChannelTxnT> {
    txn: &'txn T,
    iter: pristine::Cursor<
        T,
        &'txn T,
        T::RevchangesetCursor,
        pristine::L64,
        pristine::Pair<pristine::ChangeId, pristine::SerializedMerkle>,
    >,
}

impl<'txn, T: pristine::ChannelTxnT> Iterator for Log<'txn, T> {
    type Item = Result<
        (
            u64,
            (
                &'txn pristine::SerializedHash,
                &'txn pristine::SerializedMerkle,
            ),
        ),
        T::GraphError,
    >;
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((n, p))) => {
                let ext = match self.txn.get_external(&p.a) {
                    Err(pristine::TxnErr(e)) => return Some(Err(e)),
                    Ok(Some(ext)) => ext,
                    Ok(None) => panic!("Unknown change {:?}", p),
                };
                Some(Ok((u64::from_le(n.0), (ext, &p.b))))
            }
            None => None,
            Some(Err(e)) => Some(Err(e.0)),
        }
    }
}

pub struct RevLog<'txn, T: pristine::ChannelTxnT> {
    txn: &'txn T,
    iter: pristine::RevCursor<
        T,
        &'txn T,
        T::RevchangesetCursor,
        pristine::L64,
        pristine::Pair<pristine::ChangeId, pristine::SerializedMerkle>,
    >,
}

impl<'txn, T: pristine::ChannelTxnT> Iterator for RevLog<'txn, T> {
    type Item = Result<
        (
            u64,
            (
                &'txn pristine::SerializedHash,
                &'txn pristine::SerializedMerkle,
            ),
        ),
        T::GraphError,
    >;
    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((n, p))) => match self.txn.get_external(&p.a.into()) {
                Ok(Some(ext)) => Some(Ok((u64::from_le(n.0), (ext, &p.b)))),
                Err(e) => Some(Err(e.0)),
                Ok(None) => panic!("Unknown change {:?}", p),
            },
            None => None,
            Some(Err(e)) => Some(Err(e.0)),
        }
    }
}

pub struct Touched<'txn, T: pristine::DepsTxnT> {
    txn: &'txn T,
    iter: pristine::Cursor<
        T,
        &'txn T,
        T::Rev_touched_filesCursor,
        pristine::ChangeId,
        pristine::Position<pristine::ChangeId>,
    >,
    id: pristine::ChangeId,
}

impl<
        'txn,
        T: pristine::DepsTxnT + pristine::GraphTxnT<GraphError = <T as pristine::DepsTxnT>::DepsError>,
    > Iterator for Touched<'txn, T>
{
    type Item = Result<pristine::Position<pristine::Hash>, T::DepsError>;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(x) = self.iter.next() {
            let (cid, file) = match x {
                Ok(x) => x,
                Err(e) => return Some(Err(e.0)),
            };
            if *cid > self.id {
                return None;
            } else if *cid == self.id {
                let change = match self.txn.get_external(&file.change) {
                    Ok(ext) => ext.unwrap(),
                    Err(e) => return Some(Err(e.0)),
                };
                return Some(Ok(pristine::Position {
                    change: change.into(),
                    pos: file.pos,
                }));
            }
        }
        None
    }
}

#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub struct Timers {
    pub alive_output: std::time::Duration,
    pub alive_graph: std::time::Duration,
    pub alive_retrieve: std::time::Duration,
    pub alive_contents: std::time::Duration,
    pub alive_write: std::time::Duration,
    pub record: std::time::Duration,
    pub apply: std::time::Duration,
    pub repair_context: std::time::Duration,
    pub check_cyclic_paths: std::time::Duration,
    pub find_alive: std::time::Duration,
}
use std::sync::Mutex;
lazy_static! {
    pub static ref TIMERS: Mutex<Timers> = Mutex::new(Timers {
        alive_output: std::time::Duration::from_secs(0),
        alive_graph: std::time::Duration::from_secs(0),
        alive_retrieve: std::time::Duration::from_secs(0),
        alive_contents: std::time::Duration::from_secs(0),
        alive_write: std::time::Duration::from_secs(0),
        record: std::time::Duration::from_secs(0),
        apply: std::time::Duration::from_secs(0),
        repair_context: std::time::Duration::from_secs(0),
        check_cyclic_paths: std::time::Duration::from_secs(0),
        find_alive: std::time::Duration::from_secs(0),
    });
}
#[doc(hidden)]
pub fn reset_timers() {
    *TIMERS.lock().unwrap() = Timers::default();
}
#[doc(hidden)]
pub fn get_timers() -> Timers {
    TIMERS.lock().unwrap().clone()
}
