use crate::change::*;
use crate::small_string::*;
use crate::{HashMap, HashSet};
use parking_lot::{Mutex, RwLock};
use std::io::Write;
use std::sync::Arc;

mod change_id;
pub use change_id::*;
mod vertex;
pub use vertex::*;
mod edge;
pub use edge::*;
mod hash;
pub use hash::*;
mod inode;
pub use inode::*;
mod inode_metadata;
pub use inode_metadata::*;
mod path_id;
pub use path_id::*;
mod merkle;
pub use merkle::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct L64(pub u64);

impl From<usize> for L64 {
    fn from(u: usize) -> Self {
        L64((u as u64).to_le())
    }
}

impl From<u64> for L64 {
    fn from(u: u64) -> Self {
        L64(u.to_le())
    }
}

impl From<L64> for u64 {
    fn from(u: L64) -> Self {
        u64::from_le(u.0)
    }
}

impl From<L64> for usize {
    fn from(u: L64) -> Self {
        u64::from_le(u.0) as usize
    }
}

impl L64 {
    pub fn as_u64(&self) -> u64 {
        u64::from_le(self.0)
    }
    pub fn as_usize(&self) -> usize {
        u64::from_le(self.0) as usize
    }
}

impl std::fmt::Display for L64 {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(fmt)
    }
}

impl Ord for L64 {
    fn cmp(&self, x: &Self) -> std::cmp::Ordering {
        u64::from_le(self.0).cmp(&u64::from_le(x.0))
    }
}

impl PartialOrd for L64 {
    fn partial_cmp(&self, x: &Self) -> Option<std::cmp::Ordering> {
        Some(u64::from_le(self.0).cmp(&u64::from_le(x.0)))
    }
}

impl std::ops::Add<L64> for L64 {
    type Output = Self;
    fn add(self, x: L64) -> L64 {
        L64((u64::from_le(self.0) + u64::from_le(x.0)).to_le())
    }
}

impl std::ops::Add<usize> for L64 {
    type Output = Self;
    fn add(self, x: usize) -> L64 {
        L64((u64::from_le(self.0) + x as u64).to_le())
    }
}

impl std::ops::SubAssign<usize> for L64 {
    fn sub_assign(&mut self, x: usize) {
        self.0 = ((u64::from_le(self.0)) - x as u64).to_le()
    }
}

impl L64 {
    pub fn from_slice_le(s: &[u8]) -> Self {
        let mut u = 0u64;
        assert!(s.len() >= 8);
        unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), &mut u as *mut u64 as *mut u8, 8) }
        L64(u)
    }
    pub fn to_slice_le(&self, s: &mut [u8]) {
        assert!(s.len() >= 8);
        unsafe {
            std::ptr::copy_nonoverlapping(&self.0 as *const u64 as *const u8, s.as_mut_ptr(), 8)
        }
    }
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
#[repr(C)]
pub struct SerializedRemote {
    remote: L64,
    rev: L64,
    states: L64,
    id_rev: L64,
    tags: L64,
    path: SmallStr,
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
#[repr(C)]
pub struct SerializedChannel {
    graph: L64,
    changes: L64,
    revchanges: L64,
    states: L64,
    tags: L64,
    apply_counter: L64,
    last_modified: L64,
    id: RemoteId,
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
#[repr(C)]
pub struct Pair<A, B> {
    pub a: A,
    pub b: B,
}

pub trait Base32: Sized {
    fn to_base32(&self) -> String;
    fn from_base32(b: &[u8]) -> Option<Self>;
}

pub mod sanakirja;

pub type ApplyTimestamp = u64;

pub struct ChannelRef<T: ChannelTxnT> {
    pub(crate) r: Arc<RwLock<T::Channel>>,
}

impl<T: ChannelTxnT> ChannelRef<T> {
    pub fn new(t: T::Channel) -> Self {
        ChannelRef {
            r: Arc::new(RwLock::new(t)),
        }
    }
}

pub struct ArcTxn<T>(pub Arc<RwLock<T>>);

impl<T> ArcTxn<T> {
    pub fn new(t: T) -> Self {
        ArcTxn(Arc::new(RwLock::new(t)))
    }
}

impl<T> Clone for ArcTxn<T> {
    fn clone(&self) -> Self {
        ArcTxn(self.0.clone())
    }
}

impl<T: MutTxnT> ArcTxn<T> {
    pub fn commit(self) -> Result<(), T::GraphError> {
        if let Ok(txn) = Arc::try_unwrap(self.0) {
            txn.into_inner().commit()
        } else {
            panic!("Tried to commit an ArcTxn without dropping its references")
        }
    }
}

impl<T> std::ops::Deref for ArcTxn<T> {
    type Target = RwLock<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Error)]
#[error("Mutex poison error")]
pub struct PoisonError {}

impl<T: ChannelTxnT> ChannelRef<T> {
    pub fn read(&self) -> parking_lot::RwLockReadGuard<T::Channel> {
        self.r.read()
    }
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<T::Channel> {
        self.r.write()
    }
}

impl<T: ChannelTxnT> Clone for ChannelRef<T> {
    fn clone(&self) -> Self {
        ChannelRef { r: self.r.clone() }
    }
}

impl<T: TxnT> RemoteRef<T> {
    pub fn id(&self) -> &RemoteId {
        &self.id
    }

    pub fn lock(&self) -> parking_lot::MutexGuard<Remote<T>> {
        self.db.lock()
    }

    pub fn id_revision(&self) -> u64 {
        self.lock().id_rev.into()
    }

    pub fn set_id_revision(&self, rev: u64) -> () {
        self.lock().id_rev = rev.into()
    }
}

pub struct Remote<T: TxnT> {
    pub remote: T::Remote,
    pub rev: T::Revremote,
    pub states: T::Remotestates,
    pub id_rev: L64,
    pub tags: T::Tags,
    pub path: SmallString,
}

pub struct RemoteRef<T: TxnT> {
    db: Arc<Mutex<Remote<T>>>,
    id: RemoteId,
}

impl<T: TxnT> Clone for RemoteRef<T> {
    fn clone(&self) -> Self {
        RemoteRef {
            db: self.db.clone(),
            id: self.id.clone(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RemoteId(pub(crate) [u8; 16]);

impl RemoteId {
    pub fn nil() -> Self {
        RemoteId([0; 16])
    }
    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        if b.len() < 16 {
            return None;
        }
        let mut x = RemoteId([0; 16]);
        unsafe {
            std::ptr::copy_nonoverlapping(b.as_ptr(), x.0.as_mut_ptr(), 16);
        }
        Some(x)
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub fn from_base32(b: &[u8]) -> Option<Self> {
        let mut bb = RemoteId([0; 16]);
        if b.len() != data_encoding::BASE32_NOPAD.encode_len(16) {
            return None;
        }
        if data_encoding::BASE32_NOPAD.decode_mut(b, &mut bb.0).is_ok() {
            Some(bb)
        } else {
            None
        }
    }
}

impl std::fmt::Display for RemoteId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{}", data_encoding::BASE32_NOPAD.encode(&self.0))
    }
}

impl std::fmt::Debug for RemoteId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{}", data_encoding::BASE32_NOPAD.encode(&self.0))
    }
}

#[derive(Debug, Error)]
pub enum HashPrefixError<T: std::error::Error + 'static> {
    #[error("Failed to parse hash prefix: {0}")]
    Parse(String),
    #[error("Ambiguous hash prefix: {0}")]
    Ambiguous(String),
    #[error("Change not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Txn(T),
}

#[derive(Debug, Error)]
pub enum ForkError<T: std::error::Error + 'static> {
    #[error("Channel name already exists: {0}")]
    ChannelNameExists(String),
    #[error(transparent)]
    Txn(T),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct TxnErr<E: std::error::Error + std::fmt::Debug + 'static>(pub E);

pub trait GraphTxnT: Sized {
    type GraphError: std::error::Error + std::fmt::Debug + Send + Sync + 'static;
    table!(graph);
    get!(graph, Vertex<ChangeId>, SerializedEdge, GraphError);
    /// Returns the external hash of an internal change identifier, if
    /// the change is known.
    fn get_external(
        &self,
        p: &ChangeId,
    ) -> Result<Option<&SerializedHash>, TxnErr<Self::GraphError>>;

    /// Returns the internal change identifier of change with external
    /// hash `hash`, if the change is known.
    fn get_internal(
        &self,
        p: &SerializedHash,
    ) -> Result<Option<&ChangeId>, TxnErr<Self::GraphError>>;

    type Adj;
    fn init_adj(
        &self,
        g: &Self::Graph,
        v: Vertex<ChangeId>,
        dest: Position<ChangeId>,
        min: EdgeFlags,
        max: EdgeFlags,
    ) -> Result<Self::Adj, TxnErr<Self::GraphError>>;
    fn next_adj<'a>(
        &'a self,
        g: &Self::Graph,
        a: &mut Self::Adj,
    ) -> Option<Result<&'a SerializedEdge, TxnErr<Self::GraphError>>>;

    fn find_block<'a>(
        &'a self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&'a Vertex<ChangeId>, BlockError<Self::GraphError>>;

    fn find_block_end<'a>(
        &'a self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&'a Vertex<ChangeId>, BlockError<Self::GraphError>>;
}

pub trait ChannelTxnT: GraphTxnT {
    type Channel: Sync + Send;

    fn name<'a>(&self, channel: &'a Self::Channel) -> &'a str;
    fn id<'a>(&self, c: &'a Self::Channel) -> Option<&'a RemoteId>;
    fn graph<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Graph;
    fn apply_counter(&self, channel: &Self::Channel) -> u64;
    fn last_modified(&self, channel: &Self::Channel) -> u64;
    fn changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Changeset;
    fn rev_changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::RevChangeset;
    fn tags<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Tags;
    fn states<'a>(&self, channel: &'a Self::Channel) -> &'a Self::States;

    type Changeset;
    type RevChangeset;
    fn get_changeset(
        &self,
        channel: &Self::Changeset,
        c: &ChangeId,
    ) -> Result<Option<&L64>, TxnErr<Self::GraphError>>;
    fn get_revchangeset(
        &self,
        channel: &Self::RevChangeset,
        c: &L64,
    ) -> Result<Option<&Pair<ChangeId, SerializedMerkle>>, TxnErr<Self::GraphError>>;

    type ChangesetCursor;
    fn cursor_changeset<'txn>(
        &'txn self,
        channel: &Self::Changeset,
        pos: Option<ChangeId>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::ChangesetCursor, ChangeId, L64>,
        TxnErr<Self::GraphError>,
    >;
    fn cursor_changeset_next(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<Self::GraphError>>;

    fn cursor_changeset_prev(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<Self::GraphError>>;

    type RevchangesetCursor;
    fn cursor_revchangeset_ref<RT: std::ops::Deref<Target = Self>>(
        txn: RT,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        Cursor<Self, RT, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<Self::GraphError>,
    >;
    fn rev_cursor_revchangeset<'txn>(
        &'txn self,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        RevCursor<
            Self,
            &'txn Self,
            Self::RevchangesetCursor,
            L64,
            Pair<ChangeId, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    >;
    fn cursor_revchangeset_next(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    fn cursor_revchangeset_prev(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    type States;
    fn channel_has_state(
        &self,
        channel: &Self::States,
        hash: &SerializedMerkle,
    ) -> Result<Option<L64>, TxnErr<Self::GraphError>>;

    type Tags;
    fn is_tagged(&self, tags: &Self::Tags, t: u64) -> Result<bool, TxnErr<Self::GraphError>>;

    type TagsCursor;
    fn cursor_tags<'txn>(
        &'txn self,
        channel: &Self::Tags,
        pos: Option<L64>,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &'txn Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    >;

    fn cursor_tags_next(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    fn cursor_tags_prev(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    fn iter_tags(
        &self,
        channel: &Self::Tags,
        from: u64,
    ) -> Result<
        Cursor<Self, &Self, Self::TagsCursor, L64, Pair<SerializedMerkle, SerializedMerkle>>,
        TxnErr<Self::GraphError>,
    >;

    fn rev_iter_tags(
        &self,
        channel: &Self::Tags,
        from: Option<u64>,
    ) -> Result<
        RevCursor<Self, &Self, Self::TagsCursor, L64, Pair<SerializedMerkle, SerializedMerkle>>,
        TxnErr<Self::GraphError>,
    >;
}

pub trait GraphIter: GraphTxnT {
    type GraphCursor;
    fn graph_cursor(
        &self,
        g: &Self::Graph,
        s: Option<&Vertex<ChangeId>>,
    ) -> Result<Self::GraphCursor, TxnErr<Self::GraphError>>;
    fn next_graph<'txn>(
        &'txn self,
        g: &Self::Graph,
        a: &mut Self::GraphCursor,
    ) -> Option<Result<(&'txn Vertex<ChangeId>, &'txn SerializedEdge), TxnErr<Self::GraphError>>>;

    fn iter_graph<'a>(
        &'a self,
        g: &'a Self::Graph,
        s: Option<&Vertex<ChangeId>>,
    ) -> Result<GraphIterator<'a, Self>, TxnErr<Self::GraphError>> {
        Ok(GraphIterator {
            cursor: self.graph_cursor(g, s)?,
            txn: self,
            g: g,
        })
    }
}

pub struct GraphIterator<'a, T: GraphIter> {
    txn: &'a T,
    g: &'a T::Graph,
    cursor: T::GraphCursor,
}

impl<'a, T: GraphIter> Iterator for GraphIterator<'a, T> {
    type Item = Result<(&'a Vertex<ChangeId>, &'a SerializedEdge), TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.txn.next_graph(self.g, &mut self.cursor)
    }
}

#[derive(Debug, Error)]
pub enum BlockError<T: std::error::Error + 'static> {
    #[error(transparent)]
    Txn(T),
    #[error("Block error: {:?}", block)]
    Block { block: Position<ChangeId> },
}

impl<T: std::error::Error + 'static> std::convert::From<TxnErr<T>> for BlockError<T> {
    fn from(e: TxnErr<T>) -> Self {
        BlockError::Txn(e.0)
    }
}

pub trait DepsTxnT: Sized {
    type DepsError: std::error::Error + Send + Sync + 'static;
    table!(revdep);
    table!(dep);
    table_get!(dep, ChangeId, ChangeId, DepsError);
    cursor_ref!(dep, ChangeId, ChangeId, DepsError);
    table_get!(revdep, ChangeId, ChangeId, DepsError);
    fn iter_revdep(
        &self,
        p: &ChangeId,
    ) -> Result<Cursor<Self, &Self, Self::DepCursor, ChangeId, ChangeId>, TxnErr<Self::DepsError>>;
    fn iter_dep(
        &self,
        p: &ChangeId,
    ) -> Result<Cursor<Self, &Self, Self::DepCursor, ChangeId, ChangeId>, TxnErr<Self::DepsError>>;
    fn iter_dep_ref<RT: std::ops::Deref<Target = Self> + Clone>(
        txn: RT,
        p: &ChangeId,
    ) -> Result<Cursor<Self, RT, Self::DepCursor, ChangeId, ChangeId>, TxnErr<Self::DepsError>>;
    fn iter_touched(
        &self,
        p: &Position<ChangeId>,
    ) -> Result<
        Cursor<Self, &Self, Self::Touched_filesCursor, Position<ChangeId>, ChangeId>,
        TxnErr<Self::DepsError>,
    >;
    fn iter_rev_touched(
        &self,
        p: &ChangeId,
    ) -> Result<
        Cursor<Self, &Self, Self::Rev_touched_filesCursor, ChangeId, Position<ChangeId>>,
        TxnErr<Self::DepsError>,
    >;
    table!(touched_files);
    table!(rev_touched_files);
    table_get!(touched_files, Position<ChangeId>, ChangeId, DepsError);
    table_get!(rev_touched_files, ChangeId, Position<ChangeId>, DepsError);
    iter!(touched_files, Position<ChangeId>, ChangeId, DepsError);
    iter!(rev_touched_files, ChangeId, Position<ChangeId>, DepsError);
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct TreeErr<E: std::error::Error + std::fmt::Debug + 'static>(pub E);

pub trait TreeTxnT: Sized {
    type TreeError: std::error::Error + std::fmt::Debug + Send + Sync + 'static;
    table!(tree);
    table_get!(tree, PathId, Inode, TreeError, TreeErr);
    iter!(tree, PathId, Inode, TreeError, TreeErr);

    table!(revtree);
    table_get!(revtree, Inode, PathId, TreeError, TreeErr);
    iter!(revtree, Inode, PathId, TreeError, TreeErr);

    table!(inodes);
    table!(revinodes);
    table_get!(inodes, Inode, Position<ChangeId>, TreeError, TreeErr);
    table_get!(revinodes, Position<ChangeId>, Inode, TreeError, TreeErr);

    table!(partials);
    cursor!(partials, SmallStr, Position<ChangeId>, TreeError, TreeErr);
    cursor!(inodes, Inode, Position<ChangeId>, TreeError, TreeErr);
    fn iter_inodes(
        &self,
    ) -> Result<
        Cursor<Self, &Self, Self::InodesCursor, Inode, Position<ChangeId>>,
        TreeErr<Self::TreeError>,
    >;

    // #[cfg(debug_assertions)]
    cursor!(revinodes, Position<ChangeId>, Inode, TreeError, TreeErr);
    // #[cfg(debug_assertions)]
    fn iter_revinodes(
        &self,
    ) -> Result<
        Cursor<Self, &Self, Self::RevinodesCursor, Position<ChangeId>, Inode>,
        TreeErr<Self::TreeError>,
    >;
    fn iter_partials<'txn>(
        &'txn self,
        channel: &str,
    ) -> Result<
        Cursor<Self, &'txn Self, Self::PartialsCursor, SmallStr, Position<ChangeId>>,
        TreeErr<Self::TreeError>,
    >;
}

/// The trait of immutable transactions.
pub trait TxnT:
    GraphTxnT
    + ChannelTxnT
    + DepsTxnT<DepsError = <Self as GraphTxnT>::GraphError>
    + TreeTxnT<TreeError = <Self as GraphTxnT>::GraphError>
{
    table!(channels);
    cursor!(channels, SmallStr, SerializedChannel);

    fn hash_from_prefix(
        &self,
        prefix: &str,
    ) -> Result<(Hash, ChangeId), HashPrefixError<Self::GraphError>>;

    fn state_from_prefix(
        &self,
        channel: &Self::States,
        s: &str,
    ) -> Result<(Merkle, L64), HashPrefixError<Self::GraphError>>;

    fn hash_from_prefix_remote(
        &self,
        remote: &RemoteRef<Self>,
        prefix: &str,
    ) -> Result<Hash, HashPrefixError<Self::GraphError>>;

    fn load_channel(
        &self,
        name: &str,
    ) -> Result<Option<ChannelRef<Self>>, TxnErr<Self::GraphError>>;

    fn load_remote(
        &self,
        name: &RemoteId,
    ) -> Result<Option<RemoteRef<Self>>, TxnErr<Self::GraphError>>;

    /// Iterate a function over all channels. The loop stops the first
    /// time `f` returns `false`.
    fn channels<'txn>(
        &'txn self,
        start: &str,
    ) -> Result<Vec<ChannelRef<Self>>, TxnErr<Self::GraphError>>;

    fn iter_remotes<'txn>(
        &'txn self,
        start: &RemoteId,
    ) -> Result<RemotesIterator<'txn, Self>, TxnErr<Self::GraphError>>;

    table!(remotes);
    cursor!(remotes, RemoteId, SerializedRemote);
    table!(remote);
    table!(remotetags);
    table!(revremote);
    table!(remotestates);
    cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);
    rev_cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);

    fn iter_remote<'txn>(
        &'txn self,
        remote: &Self::Remote,
        k: u64,
    ) -> Result<
        Cursor<Self, &'txn Self, Self::RemoteCursor, L64, Pair<SerializedHash, SerializedMerkle>>,
        TxnErr<Self::GraphError>,
    >;

    fn iter_rev_remote<'txn>(
        &'txn self,
        remote: &Self::Remote,
        k: Option<L64>,
    ) -> Result<
        RevCursor<
            Self,
            &'txn Self,
            Self::RemoteCursor,
            L64,
            Pair<SerializedHash, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    >;

    fn get_remote(
        &mut self,
        name: RemoteId,
    ) -> Result<Option<RemoteRef<Self>>, TxnErr<Self::GraphError>>;

    fn last_remote(
        &self,
        remote: &Self::Remote,
    ) -> Result<Option<(u64, &Pair<SerializedHash, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    fn last_remote_tag(
        &self,
        remote: &Self::Tags,
    ) -> Result<Option<(u64, &SerializedMerkle, &SerializedMerkle)>, TxnErr<Self::GraphError>>;

    /// Find the last state greater than or equal to n.
    fn get_remote_state(
        &self,
        remote: &Self::Remote,
        n: u64,
    ) -> Result<Option<(u64, &Pair<SerializedHash, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    /// Find the last tag less than or equal to n (opposite of get_remote_state).
    fn get_remote_tag(
        &self,
        remote: &Self::Tags,
        n: u64,
    ) -> Result<Option<(u64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>;

    fn remote_has_change(
        &self,
        remote: &RemoteRef<Self>,
        hash: &SerializedHash,
    ) -> Result<bool, TxnErr<Self::GraphError>>;
    fn remote_has_state(
        &self,
        remote: &RemoteRef<Self>,
        hash: &SerializedMerkle,
    ) -> Result<Option<u64>, TxnErr<Self::GraphError>>;

    fn current_channel(&self) -> Result<&str, Self::GraphError>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedPublicKey {
    algorithm: crate::key::Algorithm,
    key: [u8; 32],
}

/// Iterate the graph between `(key, min_flag)` and `(key,
/// max_flag)`, where both bounds are included.
pub(crate) fn iter_adjacent<'txn, T: GraphTxnT>(
    txn: &'txn T,
    graph: &'txn T::Graph,
    key: Vertex<ChangeId>,
    min_flag: EdgeFlags,
    max_flag: EdgeFlags,
) -> Result<AdjacentIterator<'txn, T>, TxnErr<T::GraphError>> {
    Ok(AdjacentIterator {
        it: txn.init_adj(graph, key, Position::ROOT, min_flag, max_flag)?,
        graph,
        txn,
    })
}

pub(crate) fn iter_alive_children<'txn, T: GraphTxnT>(
    txn: &'txn T,
    graph: &'txn T::Graph,
    key: Vertex<ChangeId>,
) -> Result<AdjacentIterator<'txn, T>, TxnErr<T::GraphError>> {
    iter_adjacent(
        txn,
        graph,
        key,
        EdgeFlags::empty(),
        EdgeFlags::alive_children(),
    )
}

pub(crate) fn iter_deleted_parents<'txn, T: GraphTxnT>(
    txn: &'txn T,
    graph: &'txn T::Graph,
    key: Vertex<ChangeId>,
) -> Result<AdjacentIterator<'txn, T>, TxnErr<T::GraphError>> {
    iter_adjacent(
        txn,
        graph,
        key,
        EdgeFlags::DELETED | EdgeFlags::PARENT,
        EdgeFlags::all(),
    )
}

pub fn iter_adj_all<'txn, T: GraphTxnT>(
    txn: &'txn T,
    graph: &'txn T::Graph,
    key: Vertex<ChangeId>,
) -> Result<AdjacentIterator<'txn, T>, TxnErr<T::GraphError>> {
    iter_adjacent(txn, graph, key, EdgeFlags::empty(), EdgeFlags::all())
}

pub(crate) fn tree_path<T: TreeTxnT>(
    txn: &T,
    v: &Position<ChangeId>,
) -> Result<Option<String>, TreeErr<T::TreeError>> {
    if let Some(mut inode) = txn.get_revinodes(v, None)? {
        let mut components = Vec::new();
        while !inode.is_root() {
            if let Some(next) = txn.get_revtree(inode, None)? {
                components.push(next.basename.as_str().to_string());
                inode = &next.parent_inode;
            } else {
                assert!(components.is_empty());
                return Ok(None);
            }
        }
        if let Some(mut result) = components.pop() {
            while let Some(c) = components.pop() {
                result = result + "/" + c.as_str()
            }
            return Ok(Some(result));
        }
    }
    Ok(None)
}

pub(crate) fn internal<T: GraphTxnT>(
    txn: &T,
    h: &Option<Hash>,
    p: ChangeId,
) -> Result<Option<ChangeId>, TxnErr<T::GraphError>> {
    match *h {
        Some(Hash::None) => Ok(Some(ChangeId::ROOT)),
        Some(ref h) => Ok(txn.get_internal(&h.into())?.map(|x| *x)),
        None => Ok(Some(p)),
    }
}

#[derive(Error, Debug)]
pub enum InconsistentChange<T: std::error::Error + 'static> {
    #[error("Undeclared dependency")]
    UndeclaredDep,
    #[error(transparent)]
    Txn(T),
}

impl<T: std::error::Error + 'static> std::convert::From<TxnErr<T>> for InconsistentChange<T> {
    fn from(e: TxnErr<T>) -> Self {
        InconsistentChange::Txn(e.0)
    }
}

pub fn internal_pos<T: GraphTxnT>(
    txn: &T,
    pos: &Position<Option<Hash>>,
    change_id: ChangeId,
) -> Result<Position<ChangeId>, InconsistentChange<T::GraphError>> {
    let change = if let Some(p) = pos.change {
        if let Some(&p) = txn.get_internal(&p.into())? {
            p
        } else {
            return Err(InconsistentChange::UndeclaredDep);
        }
    } else {
        change_id
    };

    Ok(Position {
        change,
        pos: pos.pos,
    })
}

pub fn internal_vertex<T: GraphTxnT>(
    txn: &T,
    v: &Vertex<Option<Hash>>,
    change_id: ChangeId,
) -> Result<Vertex<ChangeId>, InconsistentChange<T::GraphError>> {
    let change = if let Some(p) = v.change {
        if let Some(&p) = txn.get_internal(&p.into())? {
            p
        } else {
            return Err(InconsistentChange::UndeclaredDep);
        }
    } else {
        change_id
    };

    Ok(Vertex {
        change,
        start: v.start,
        end: v.end,
    })
}

pub fn changeid_log<'db, 'txn: 'db, T: ChannelTxnT>(
    txn: &'txn T,
    channel: &'db T::Channel,
    from: L64,
) -> Result<
    Cursor<T, &'txn T, T::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
    TxnErr<T::GraphError>,
> {
    T::cursor_revchangeset_ref(txn, txn.rev_changes(&channel), Some(from))
}

pub fn current_state<'db, 'txn: 'db, T: ChannelTxnT>(
    txn: &'txn T,
    channel: &'db T::Channel,
) -> Result<Merkle, TxnErr<T::GraphError>> {
    if let Some(e) = txn
        .rev_cursor_revchangeset(txn.rev_changes(&channel), None)?
        .next()
    {
        Ok((&(e?.1).b).into())
    } else {
        Ok(Merkle::zero())
    }
}

pub(crate) fn changeid_rev_log<'db, 'txn: 'db, T: ChannelTxnT>(
    txn: &'txn T,
    channel: &'db T::Channel,
    from: Option<L64>,
) -> Result<
    RevCursor<T, &'txn T, T::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
    TxnErr<T::GraphError>,
> {
    Ok(txn.rev_cursor_revchangeset(txn.rev_changes(&channel), from)?)
}

pub(crate) fn log_for_path<
    'txn,
    'channel,
    T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
>(
    txn: &'txn T,
    channel: &'channel T::Channel,
    key: Position<ChangeId>,
    from_timestamp: u64,
) -> Result<PathChangeset<'channel, 'txn, T>, TxnErr<T::GraphError>> {
    Ok(PathChangeset {
        iter: T::cursor_revchangeset_ref(
            txn,
            txn.rev_changes(&channel),
            Some(from_timestamp.into()),
        )?,
        txn,
        channel,
        key,
    })
}

pub(crate) fn rev_log_for_path<
    'txn,
    'channel,
    T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
>(
    txn: &'txn T,
    channel: &'channel T::Channel,
    key: Position<ChangeId>,
    from_timestamp: u64,
) -> Result<RevPathChangeset<'channel, 'txn, T>, TxnErr<T::GraphError>> {
    Ok(RevPathChangeset {
        iter: txn
            .rev_cursor_revchangeset(txn.rev_changes(&channel), Some(from_timestamp.into()))?,
        txn,
        channel,
        key,
    })
}

/// Is there an alive/pseudo edge from `a` to `b`.
pub(crate) fn test_edge<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    a: Position<ChangeId>,
    b: Position<ChangeId>,
    min: EdgeFlags,
    max: EdgeFlags,
) -> Result<bool, TxnErr<T::GraphError>> {
    debug!("is_connected {:?} {:?}", a, b);
    let mut adj = txn.init_adj(channel, a.inode_vertex(), b, min, max)?;
    match txn.next_adj(channel, &mut adj) {
        Some(Ok(dest)) => Ok(dest.dest() == b),
        Some(Err(e)) => Err(e.into()),
        None => Ok(false),
    }
}

/// Is there an alive/pseudo edge to `a`.
pub(crate) fn is_alive<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    a: &Vertex<ChangeId>,
) -> Result<bool, TxnErr<T::GraphError>> {
    if a.is_root() {
        return Ok(true);
    }
    for e in iter_adjacent(
        txn,
        channel,
        *a,
        EdgeFlags::PARENT,
        EdgeFlags::all() - EdgeFlags::DELETED,
    )? {
        let e = e?;
        if !e.flag().contains(EdgeFlags::PSEUDO)
            && (e.flag().contains(EdgeFlags::BLOCK) || a.is_empty())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn make_changeid<T: GraphTxnT>(
    txn: &T,
    h: &Hash,
) -> Result<ChangeId, TxnErr<T::GraphError>> {
    if let Some(h_) = txn.get_internal(&h.into())? {
        debug!("make_changeid, found = {:?} {:?}", h, h_);
        return Ok(*h_);
    }
    use byteorder::{ByteOrder, LittleEndian};
    let mut p = match h {
        Hash::None => return Ok(ChangeId::ROOT),
        Hash::Blake3(ref s) => LittleEndian::read_u64(&s[..]),
    };
    let mut pp = ChangeId(L64(p));
    while let Some(ext) = txn.get_external(&pp)? {
        debug!("ext = {:?}", ext);
        p = u64::from_le(p) + 1;
        pp = ChangeId(L64(p.to_le()));
    }
    Ok(pp)
}

// #[cfg(debug_assertions)]
pub fn debug_tree<P: AsRef<std::path::Path>, T: TreeTxnT>(
    txn: &T,
    file: P,
) -> Result<(), std::io::Error> {
    let root = OwnedPathId {
        parent_inode: Inode::ROOT,
        basename: SmallString::from_str(""),
    };
    let mut f = std::fs::File::create(file)?;
    for t in txn.iter_tree(&root, None).unwrap() {
        writeln!(f, "{:?}", t.unwrap())?
    }
    Ok(())
}

// #[cfg(debug_assertions)]
pub fn debug_tree_print<T: TreeTxnT>(txn: &T) {
    let root = OwnedPathId {
        parent_inode: Inode::ROOT,
        basename: SmallString::from_str(""),
    };
    for t in txn.iter_tree(&root, None).unwrap() {
        debug!("{:?}", t.unwrap())
    }
}

// #[cfg(debug_assertions)]
pub fn debug_remotes<T: TxnT>(txn: &T) {
    for t in txn.iter_remotes(&RemoteId([0; 16])).unwrap() {
        let rem = t.unwrap();
        debug!("{:?}", rem.id());
        for x in txn.iter_remote(&rem.lock().remote, 0).unwrap() {
            debug!("    {:?}", x.unwrap());
        }
    }
}

/// Write the graph of a channel to file `f` in graphviz
/// format. **Warning:** this can be really large on old channels.
// #[cfg(debug_assertions)]
pub fn debug_to_file<P: AsRef<std::path::Path>, T: GraphIter + ChannelTxnT>(
    txn: &T,
    channel: &ChannelRef<T>,
    f: P,
) -> Result<bool, std::io::Error> {
    info!("debug {:?}", f.as_ref());
    let mut f = std::fs::File::create(f)?;
    let channel = channel.r.read();
    let done = debug(txn, txn.graph(&*channel), &mut f)?;
    f.flush()?;
    info!("done debugging {:?}", done);
    Ok(done)
}

// #[cfg(debug_assertions)]
pub fn debug_revtree<P: AsRef<std::path::Path>, T: TreeTxnT>(
    txn: &T,
    file: P,
) -> Result<(), std::io::Error> {
    let mut f = std::fs::File::create(file)?;
    for t in txn.iter_revtree(&Inode::ROOT, None).unwrap() {
        writeln!(f, "{:?}", t.unwrap())?
    }
    Ok(())
}

// #[cfg(debug_assertions)]
pub fn debug_revtree_print<T: TreeTxnT>(txn: &T) {
    for t in txn.iter_revtree(&Inode::ROOT, None).unwrap() {
        debug!("{:?}", t.unwrap())
    }
}

// #[cfg(debug_assertions)]
pub fn debug_inodes<T: TreeTxnT>(txn: &T) {
    debug!("debug_inodes");
    for t in txn.iter_inodes().unwrap() {
        debug!("debug_inodes = {:?}", t.unwrap())
    }
    debug!("/debug_inodes");
}

// #[cfg(debug_assertions)]
pub fn debug_revinodes<T: TreeTxnT>(txn: &T) {
    debug!("debug_revinodes");
    for t in txn.iter_revinodes().unwrap() {
        debug!("debug_revinodes = {:?}", t.unwrap())
    }
    debug!("/debug_revinodes");
}

pub fn debug_dep<T: DepsTxnT>(txn: &T) {
    debug!("debug_dep");
    for t in txn.iter_dep(&ChangeId::ROOT).unwrap() {
        debug!("debug_dep = {:?}", t.unwrap())
    }
    debug!("/debug_dep");
}

pub fn debug_revdep<T: DepsTxnT>(txn: &T) {
    debug!("debug_revdep");
    for t in txn.iter_revdep(&ChangeId::ROOT).unwrap() {
        debug!("debug_revdep = {:?}", t.unwrap())
    }
    debug!("/debug_revdep");
}

/// Write the graph of a channel to write `W` in graphviz
/// format. **Warning:** this can be really large on old channels.
// #[cfg(debug_assertions)]
pub fn debug<W: Write, T: GraphIter>(
    txn: &T,
    channel: &T::Graph,
    mut f: W,
) -> Result<bool, std::io::Error> {
    let mut cursor = txn.graph_cursor(&channel, None).unwrap();
    writeln!(f, "digraph {{")?;
    let mut keys = std::collections::HashSet::new();
    let mut at_least_one = false;
    while let Some(x) = txn.next_graph(&channel, &mut cursor) {
        let (k, v) = x.unwrap();
        at_least_one = true;
        debug!("debug {:?} {:?}", k, v);
        if keys.insert(k) {
            debug_vertex(&mut f, *k)?
        }
        debug_edge(txn, channel, &mut f, *k, *v)?
    }
    writeln!(f, "}}")?;
    Ok(at_least_one)
}

pub fn check_alive<T: ChannelTxnT + GraphIter>(
    txn: &T,
    channel: &T::Graph,
) -> (
    HashMap<Vertex<ChangeId>, Option<Vertex<ChangeId>>>,
    Vec<(Vertex<ChangeId>, Option<Vertex<ChangeId>>)>,
) {
    // Find the reachable with a DFS.
    let mut reachable = HashSet::default();
    let mut stack = vec![Vertex::ROOT];
    while let Some(v) = stack.pop() {
        if !reachable.insert(v) {
            continue;
        }
        for e in iter_adjacent(
            txn,
            &channel,
            v,
            EdgeFlags::empty(),
            EdgeFlags::all() - EdgeFlags::DELETED - EdgeFlags::PARENT,
        )
        .unwrap()
        {
            let e = e.unwrap();
            stack.push(*txn.find_block(&channel, e.dest()).unwrap());
        }
    }
    debug!("reachable = {:#?}", reachable);

    // Find the alive
    let mut alive_unreachable = HashMap::default();
    let mut cursor = txn.graph_cursor(&channel, None).unwrap();

    let mut visited = HashSet::default();
    let mut k0 = Vertex::ROOT;
    let mut k0_has_pseudo_parents = false;
    let mut k0_has_regular_parents = false;
    let mut reachable_pseudo = Vec::new();
    while let Some(x) = txn.next_graph(&channel, &mut cursor) {
        let (&k, &v) = x.unwrap();
        debug!("check_alive, k = {:?}, v = {:?}", k, v);
        if k0 != k {
            if k0_has_pseudo_parents && !k0_has_regular_parents {
                reachable_pseudo.push((
                    k0,
                    find_file(txn, &channel, k0, &mut stack, &mut visited).unwrap(),
                ))
            }
            k0 = k;
            k0_has_pseudo_parents = false;
            k0_has_regular_parents = false;
        }
        if v.flag().contains(EdgeFlags::PARENT)
            && !v.flag().contains(EdgeFlags::FOLDER)
            && !v.flag().contains(EdgeFlags::DELETED)
        {
            if v.flag().contains(EdgeFlags::PSEUDO) {
                k0_has_pseudo_parents = true
            } else {
                k0_has_regular_parents = true
            }
        }

        if v.flag().contains(EdgeFlags::PARENT)
            && (v.flag().contains(EdgeFlags::BLOCK) || k.is_empty())
            && !v.flag().contains(EdgeFlags::DELETED)
            && !reachable.contains(&k)
        {
            let file = find_file(txn, &channel, k, &mut stack, &mut visited).unwrap();
            alive_unreachable.insert(k, file);
        }
    }
    if !k0.is_root() && k0_has_pseudo_parents && !k0_has_regular_parents {
        reachable_pseudo.push((
            k0,
            find_file(txn, &channel, k0, &mut stack, &mut visited).unwrap(),
        ));
    }

    (alive_unreachable, reachable_pseudo)
}

fn find_file<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    k: Vertex<ChangeId>,
    stack: &mut Vec<Vertex<ChangeId>>,
    visited: &mut HashSet<Vertex<ChangeId>>,
) -> Result<Option<Vertex<ChangeId>>, TxnErr<T::GraphError>> {
    let mut file = None;
    stack.clear();
    stack.push(k);
    visited.clear();
    'outer: while let Some(kk) = stack.pop() {
        if !visited.insert(kk) {
            continue;
        }
        for e in iter_adjacent(txn, &channel, kk, EdgeFlags::PARENT, EdgeFlags::all())? {
            let e = e?;
            if e.flag().contains(EdgeFlags::PARENT) {
                if e.flag().contains(EdgeFlags::FOLDER) {
                    file = Some(kk);
                    break 'outer;
                }
                stack.push(*txn.find_block_end(&channel, e.dest()).unwrap());
            }
        }
    }
    Ok(file)
}

pub fn debug_root<W: Write, T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    root: Vertex<ChangeId>,
    mut f: W,
    down: bool,
) -> Result<(), std::io::Error> {
    writeln!(f, "digraph {{")?;
    let mut visited = HashSet::default();
    let mut stack = vec![root];
    while let Some(v) = stack.pop() {
        if !visited.insert(v) {
            continue;
        }
        debug_vertex(&mut f, v)?;
        for e in iter_adj_all(txn, &channel, v).unwrap() {
            let e = e.unwrap();
            if e.flag().contains(EdgeFlags::PARENT) ^ down {
                debug_edge(txn, &channel, &mut f, v, *e)?;
                let v = if e.flag().contains(EdgeFlags::PARENT) {
                    txn.find_block_end(&channel, e.dest()).unwrap()
                } else {
                    txn.find_block(&channel, e.dest()).unwrap()
                };
                stack.push(*v);
            }
        }
    }
    writeln!(f, "}}")?;
    Ok(())
}

fn debug_vertex<W: std::io::Write>(mut f: W, k: Vertex<ChangeId>) -> Result<(), std::io::Error> {
    writeln!(
        f,
        "node_{}_{}_{}[label=\"{} [{};{}[\"];",
        k.change.to_base32(),
        k.start.0,
        k.end.0,
        k.change.to_base32(),
        k.start.0,
        k.end.0,
    )
}

fn debug_edge<T: GraphTxnT, W: std::io::Write>(
    txn: &T,
    channel: &T::Graph,
    mut f: W,
    k: Vertex<ChangeId>,
    v: SerializedEdge,
) -> Result<(), std::io::Error> {
    let style = if v.flag().contains(EdgeFlags::DELETED) {
        ", style=dashed"
    } else if v.flag().contains(EdgeFlags::PSEUDO) {
        ", style=dotted"
    } else {
        ""
    };
    let color = if v.flag().contains(EdgeFlags::PARENT) {
        if v.flag().contains(EdgeFlags::FOLDER) {
            "orange"
        } else {
            "red"
        }
    } else if v.flag().contains(EdgeFlags::FOLDER) {
        "royalblue"
    } else {
        "forestgreen"
    };

    if v.flag().contains(EdgeFlags::PARENT) {
        let dest = if v.dest().change.is_root() {
            Vertex::ROOT
        } else if let Ok(&dest) = txn.find_block_end(channel, v.dest()) {
            dest
        } else {
            return Ok(());
        };
        writeln!(
            f,
            "node_{}_{}_{} -> node_{}_{}_{} [label=\"{}{}{}\", color=\"{}\"{}];",
            k.change.to_base32(),
            k.start.0,
            k.end.0,
            dest.change.to_base32(),
            dest.start.0,
            dest.end.0,
            if v.flag().contains(EdgeFlags::BLOCK) {
                "["
            } else {
                ""
            },
            v.introduced_by().to_base32(),
            if v.flag().contains(EdgeFlags::BLOCK) {
                "]"
            } else {
                ""
            },
            color,
            style
        )?;
    } else if let Ok(dest) = txn.find_block(&channel, v.dest()) {
        writeln!(
            f,
            "node_{}_{}_{} -> node_{}_{}_{} [label=\"{}{}{}\", color=\"{}\"{}];",
            k.change.to_base32(),
            k.start.0,
            k.end.0,
            dest.change.to_base32(),
            dest.start.0,
            dest.end.0,
            if v.flag().contains(EdgeFlags::BLOCK) {
                "["
            } else {
                ""
            },
            v.introduced_by().to_base32(),
            if v.flag().contains(EdgeFlags::BLOCK) {
                "]"
            } else {
                ""
            },
            color,
            style
        )?;
    } else {
        writeln!(
            f,
            "node_{}_{}_{} -> node_{}_{} [label=\"{}{}{}\", color=\"{}\"{}];",
            k.change.to_base32(),
            k.start.0,
            k.end.0,
            v.dest().change.to_base32(),
            v.dest().pos.0,
            if v.flag().contains(EdgeFlags::BLOCK) {
                "["
            } else {
                ""
            },
            v.introduced_by().to_base32(),
            if v.flag().contains(EdgeFlags::BLOCK) {
                "]"
            } else {
                ""
            },
            color,
            style
        )?;
    }
    Ok(())
}

/// A cursor over a table, initialised at a certain value.
pub struct Cursor<T: Sized, RT: std::ops::Deref<Target = T>, Cursor, K: ?Sized, V: ?Sized> {
    pub cursor: Cursor,
    pub txn: RT,
    pub t: std::marker::PhantomData<T>,
    pub k: std::marker::PhantomData<K>,
    pub v: std::marker::PhantomData<V>,
}

pub struct RevCursor<T: Sized, RT: std::ops::Deref<Target = T>, Cursor, K: ?Sized, V: ?Sized> {
    pub cursor: Cursor,
    pub txn: RT,
    pub t: std::marker::PhantomData<T>,
    pub k: std::marker::PhantomData<K>,
    pub v: std::marker::PhantomData<V>,
}

initialized_cursor!(changeset, ChangeId, L64, ChannelTxnT, GraphError);
initialized_cursor!(
    revchangeset,
    L64,
    Pair<ChangeId, SerializedMerkle>,
    ChannelTxnT,
    GraphError
);
initialized_rev_cursor!(
    revchangeset,
    L64,
    Pair<ChangeId, SerializedMerkle>,
    ChannelTxnT,
    GraphError
);
initialized_cursor!(tags, L64, Pair<SerializedMerkle, SerializedMerkle>, ChannelTxnT, GraphError);
initialized_rev_cursor!(tags, L64, Pair<SerializedMerkle, SerializedMerkle>, ChannelTxnT, GraphError);
initialized_cursor!(tree, PathId, Inode, TreeTxnT, TreeError, TreeErr);
initialized_cursor!(revtree, Inode, PathId, TreeTxnT, TreeError, TreeErr);
initialized_cursor!(dep, ChangeId, ChangeId, DepsTxnT, DepsError);
initialized_cursor!(
    partials,
    SmallStr,
    Position<ChangeId>,
    TreeTxnT,
    TreeError,
    TreeErr
);
initialized_cursor!(
    rev_touched_files,
    ChangeId,
    Position<ChangeId>,
    DepsTxnT,
    DepsError
);
initialized_cursor!(
    touched_files,
    Position<ChangeId>,
    ChangeId,
    DepsTxnT,
    DepsError
);
initialized_cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);
initialized_rev_cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);
initialized_cursor!(
    inodes,
    Inode,
    Position<ChangeId>,
    TreeTxnT,
    TreeError,
    TreeErr
);
initialized_cursor!(
    revinodes,
    Position<ChangeId>,
    Inode,
    TreeTxnT,
    TreeError,
    TreeErr
);

/// An iterator for nodes adjacent to `key` through an edge with flags smaller than `max_flag`.
pub struct AdjacentIterator<'txn, T: GraphTxnT> {
    it: T::Adj,
    graph: &'txn T::Graph,
    txn: &'txn T,
}

impl<'txn, T: GraphTxnT> Iterator for AdjacentIterator<'txn, T> {
    type Item = Result<&'txn SerializedEdge, TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.txn.next_adj(self.graph, &mut self.it)
    }
}

pub struct PathChangeset<'channel, 'txn: 'channel, T: ChannelTxnT + DepsTxnT> {
    txn: &'txn T,
    channel: &'channel T::Channel,
    iter: Cursor<T, &'txn T, T::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
    key: Position<ChangeId>,
}

pub struct RevPathChangeset<'channel, 'txn: 'channel, T: ChannelTxnT + DepsTxnT> {
    txn: &'txn T,
    channel: &'channel T::Channel,
    iter: RevCursor<T, &'txn T, T::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
    key: Position<ChangeId>,
}

impl<
        'channel,
        'txn: 'channel,
        T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
    > Iterator for PathChangeset<'channel, 'txn, T>
{
    type Item = Result<Hash, TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(x) = self.iter.next() {
            let changeid = match x {
                Ok(x) => (x.1).a,
                Err(e) => return Some(Err(e)),
            };
            let iter = match self.txn.iter_rev_touched_files(&changeid, None) {
                Ok(iter) => iter,
                Err(e) => return Some(Err(e)),
            };
            for x in iter {
                let (p, touched) = match x {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e)),
                };
                if *p > changeid {
                    break;
                } else if *p < changeid {
                    continue;
                }
                match is_ancestor_of(self.txn, self.txn.graph(&self.channel), self.key, *touched) {
                    Ok(true) => {
                        return self
                            .txn
                            .get_external(&changeid)
                            .transpose()
                            .map(|x| x.map(|x| x.into()))
                    }
                    Err(e) => return Some(Err(e)),
                    Ok(false) => {}
                }
            }
        }
        None
    }
}

impl<
        'channel,
        'txn: 'channel,
        T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
    > Iterator for RevPathChangeset<'channel, 'txn, T>
{
    type Item = Result<Hash, TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let changeid = match self.iter.next()? {
                Err(e) => return Some(Err(e)),
                Ok((_, p)) => p.a,
            };
            let iter = match self.txn.iter_rev_touched_files(&changeid, None) {
                Ok(iter) => iter,
                Err(e) => return Some(Err(e)),
            };
            for x in iter {
                let (p, touched) = match x {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e)),
                };
                if *p > changeid {
                    break;
                } else if *p < changeid {
                    continue;
                }
                match is_ancestor_of(self.txn, self.txn.graph(&self.channel), self.key, *touched) {
                    Ok(true) => {
                        return self
                            .txn
                            .get_external(&changeid)
                            .transpose()
                            .map(|x| x.map(From::from))
                    }
                    Err(e) => return Some(Err(e)),
                    Ok(false) => {}
                }
            }
        }
    }
}

fn is_ancestor_of<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    a: Position<ChangeId>,
    b: Position<ChangeId>,
) -> Result<bool, TxnErr<T::GraphError>> {
    let mut stack = vec![b];
    let mut visited = std::collections::HashSet::new();
    debug!("a = {:?}", a);
    while let Some(b) = stack.pop() {
        debug!("pop {:?}", b);
        if a == b {
            return Ok(true);
        }
        if !visited.insert(b) {
            continue;
        }
        for p in iter_adjacent(
            txn,
            channel,
            b.inode_vertex(),
            EdgeFlags::FOLDER | EdgeFlags::PARENT,
            EdgeFlags::FOLDER | EdgeFlags::PARENT | EdgeFlags::PSEUDO,
        )? {
            let p = p?;
            // Ok, since `p` is in the channel.
            let parent = txn.find_block_end(channel, p.dest()).unwrap();
            for pp in iter_adjacent(
                txn,
                channel,
                *parent,
                EdgeFlags::FOLDER | EdgeFlags::PARENT,
                EdgeFlags::FOLDER | EdgeFlags::PARENT | EdgeFlags::PSEUDO,
            )? {
                let pp = pp?;
                if pp.dest() == a {
                    return Ok(true);
                }
                stack.push(pp.dest())
            }
        }
    }
    Ok(false)
}

pub struct ChannelIterator<'txn, T: TxnT> {
    txn: &'txn T,
    cursor: T::ChannelsCursor,
}

impl<'txn, T: TxnT> Iterator for ChannelIterator<'txn, T> {
    type Item = Result<(&'txn SmallStr, ChannelRef<T>), TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        // Option<(SmallString, (u64, u64, u64, u64, u64, u64))>
        match self.txn.cursor_channels_next(&mut self.cursor) {
            Err(e) => Some(Err(e)),
            Ok(Some((name, _))) => Some(Ok((name, self.txn.load_channel(name.as_str()).unwrap()?))),
            Ok(None) => None,
        }
    }
}

pub struct RemotesIterator<'txn, T: TxnT> {
    txn: &'txn T,
    cursor: T::RemotesCursor,
}

impl<'txn, T: TxnT> Iterator for RemotesIterator<'txn, T> {
    type Item = Result<RemoteRef<T>, TxnErr<T::GraphError>>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.txn.cursor_remotes_next(&mut self.cursor) {
            Ok(Some((name, _))) => self.txn.load_remote(name).transpose(),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait GraphMutTxnT: GraphTxnT {
    put_del!(internal, SerializedHash, ChangeId, GraphError);
    put_del!(external, ChangeId, SerializedHash, GraphError);

    /// Insert a key and a value to a graph. Returns `false` if and only if `(k, v)` was already in the graph, in which case no insertion happened.
    fn put_graph(
        &mut self,
        channel: &mut Self::Graph,
        k: &Vertex<ChangeId>,
        v: &SerializedEdge,
    ) -> Result<bool, TxnErr<Self::GraphError>>;

    /// Delete a key and a value from a graph. Returns `true` if and only if `(k, v)` was in the graph.
    fn del_graph(
        &mut self,
        channel: &mut Self::Graph,
        k: &Vertex<ChangeId>,
        v: Option<&SerializedEdge>,
    ) -> Result<bool, TxnErr<Self::GraphError>>;

    fn debug(&mut self, channel: &mut Self::Graph, extra: &str);

    /// Split a key `[a, b[` at position `pos`, yielding two keys `[a,
    /// pos[` and `[pos, b[` linked by an edge.
    fn split_block(
        &mut self,
        graph: &mut Self::Graph,
        key: &Vertex<ChangeId>,
        pos: ChangePosition,
        buf: &mut Vec<SerializedEdge>,
    ) -> Result<(), TxnErr<Self::GraphError>>;
}

pub trait ChannelMutTxnT: ChannelTxnT + GraphMutTxnT {
    fn graph_mut(channel: &mut Self::Channel) -> &mut Self::Graph;
    fn touch_channel(&mut self, channel: &mut Self::Channel, t: Option<u64>);

    /// Add a change and a timestamp to a change table. Returns `None` if and only if `(p, t)` was already in the change table, in which case no insertion happened. Returns the new state else.
    fn put_changes(
        &mut self,
        channel: &mut Self::Channel,
        p: ChangeId,
        t: ApplyTimestamp,
        h: &Hash,
    ) -> Result<Option<Merkle>, TxnErr<Self::GraphError>>;

    /// Delete a change from a change table. Returns `true` if and only if `(p, t)` was in the change table.
    fn del_changes(
        &mut self,
        channel: &mut Self::Channel,
        p: ChangeId,
        t: ApplyTimestamp,
    ) -> Result<bool, TxnErr<Self::GraphError>>;

    fn put_tags(
        &mut self,
        channel: &mut Self::Tags,
        n: u64,
        m: &Merkle,
    ) -> Result<(), TxnErr<Self::GraphError>>;

    fn tags_mut<'a>(&mut self, channel: &'a mut Self::Channel) -> &'a mut Self::Tags;

    fn del_tags(
        &mut self,
        channel: &mut Self::Tags,
        n: u64,
    ) -> Result<(), TxnErr<Self::GraphError>>;
}

pub trait DepsMutTxnT: DepsTxnT {
    put_del!(dep, ChangeId, ChangeId, DepsError);
    put_del!(revdep, ChangeId, ChangeId, DepsError);
    put_del!(touched_files, Position<ChangeId>, ChangeId, DepsError);
    put_del!(rev_touched_files, ChangeId, Position<ChangeId>, DepsError);
}

pub trait TreeMutTxnT: TreeTxnT {
    put_del!(inodes, Inode, Position<ChangeId>, TreeError, TreeErr);
    put_del!(revinodes, Position<ChangeId>, Inode, TreeError, TreeErr);
    put_del!(tree, PathId, Inode, TreeError, TreeErr);
    put_del!(revtree, Inode, PathId, TreeError, TreeErr);
    fn put_partials(
        &mut self,
        k: &str,
        e: Position<ChangeId>,
    ) -> Result<bool, TreeErr<Self::TreeError>>;

    fn del_partials(
        &mut self,
        k: &str,
        e: Option<Position<ChangeId>>,
    ) -> Result<bool, TreeErr<Self::TreeError>>;
}

/// The trait of immutable transactions.
pub trait MutTxnT:
    GraphMutTxnT
    + ChannelMutTxnT
    + DepsMutTxnT<DepsError = <Self as GraphTxnT>::GraphError>
    + TreeMutTxnT<TreeError = <Self as GraphTxnT>::GraphError>
    + TxnT
{
    /// Open a channel, creating it if it is missing. The return type
    /// is a `Rc<RefCell<…>>` in order to avoid:
    /// - opening the same channel twice. Since a channel contains pointers, that could potentially lead to double-borrow issues. We absolutely have to check that at runtime (hence the `RefCell`).
    /// - writing the channel to disk (if the backend is written on the disk) for every minor operation on the channel.
    ///
    /// Additionally, the `Rc` is used to:
    /// - avoid having to commit channels explicitly (channels are
    /// committed automatically upon committing the transaction), and
    /// - to return a value that doesn't borrow the transaction, so
    /// that the channel can actually be used in a mutable transaction.
    fn open_or_create_channel(&mut self, name: &str) -> Result<ChannelRef<Self>, Self::GraphError>;

    fn fork(
        &mut self,
        channel: &ChannelRef<Self>,
        name: &str,
    ) -> Result<ChannelRef<Self>, ForkError<Self::GraphError>>;

    fn rename_channel(
        &mut self,
        channel: &mut ChannelRef<Self>,
        name: &str,
    ) -> Result<(), ForkError<Self::GraphError>>;

    fn drop_channel(&mut self, name: &str) -> Result<bool, Self::GraphError>;

    /// Commit this transaction.
    fn commit(self) -> Result<(), Self::GraphError>;

    fn open_or_create_remote(
        &mut self,
        id: RemoteId,
        path: &str,
    ) -> Result<RemoteRef<Self>, Self::GraphError>;

    fn put_remote(
        &mut self,
        remote: &mut RemoteRef<Self>,
        k: u64,
        v: (Hash, Merkle),
    ) -> Result<bool, TxnErr<Self::GraphError>>;

    fn del_remote(
        &mut self,
        remote: &mut RemoteRef<Self>,
        k: u64,
    ) -> Result<bool, TxnErr<Self::GraphError>>;

    fn drop_remote(&mut self, remote: RemoteRef<Self>) -> Result<bool, Self::GraphError>;

    fn drop_named_remote(&mut self, id: RemoteId) -> Result<bool, Self::GraphError>;

    fn set_current_channel(&mut self, cur: &str) -> Result<(), Self::GraphError>;
}

pub(crate) fn put_inodes_with_rev<T: TreeMutTxnT>(
    txn: &mut T,
    inode: &Inode,
    position: &Position<ChangeId>,
) -> Result<(), TreeErr<T::TreeError>> {
    txn.put_inodes(inode, position)?;
    txn.put_revinodes(position, inode)?;
    Ok(())
}

pub(crate) fn del_inodes_with_rev<T: TreeMutTxnT>(
    txn: &mut T,
    inode: &Inode,
    position: &Position<ChangeId>,
) -> Result<bool, TreeErr<T::TreeError>> {
    if txn.del_inodes(inode, Some(position))? {
        assert!(txn.del_revinodes(position, Some(inode))?);
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn put_tree_with_rev<T: TreeMutTxnT>(
    txn: &mut T,
    file_id: &PathId,
    inode: &Inode,
) -> Result<(), TreeErr<T::TreeError>> {
    if txn.put_tree(file_id, inode)? {
        txn.put_revtree(inode, file_id)?;
    }
    Ok(())
}

pub(crate) fn del_tree_with_rev<T: TreeMutTxnT>(
    txn: &mut T,
    file_id: &PathId,
    inode: &Inode,
) -> Result<bool, TreeErr<T::TreeError>> {
    if txn.del_tree(file_id, Some(inode))? {
        if !file_id.basename.is_empty() {
            assert!(txn.del_revtree(inode, Some(file_id))?);
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn del_graph_with_rev<T: GraphMutTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    mut flag: EdgeFlags,
    mut k0: Vertex<ChangeId>,
    mut k1: Vertex<ChangeId>,
    introduced_by: ChangeId,
) -> Result<bool, TxnErr<T::GraphError>> {
    if flag.contains(EdgeFlags::PARENT) {
        std::mem::swap(&mut k0, &mut k1);
        flag -= EdgeFlags::PARENT
    }
    debug!("del_graph_with_rev {:?} {:?} {:?}", flag, k0, k1);
    let v0 = SerializedEdge::new(flag, k1.change, k1.start, introduced_by);
    let a = txn.del_graph(graph, &k0, Some(&v0))?;
    let v1 = SerializedEdge::new(flag | EdgeFlags::PARENT, k0.change, k0.end, introduced_by);
    let b = txn.del_graph(graph, &k1, Some(&v1))?;
    Ok(a && b)
}

pub(crate) fn put_graph_with_rev<T: GraphMutTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    flag: EdgeFlags,
    k0: Vertex<ChangeId>,
    k1: Vertex<ChangeId>,
    introduced_by: ChangeId,
) -> Result<bool, TxnErr<T::GraphError>> {
    debug_assert!(!flag.contains(EdgeFlags::PARENT));
    if k0.change == k1.change {
        assert_ne!(k0.start_pos(), k1.start_pos());
    }
    if introduced_by == ChangeId::ROOT {
        assert!(flag.contains(EdgeFlags::PSEUDO));
    }

    debug!("put_graph_with_rev {:?} {:?} {:?}", k0, k1, flag);
    let a = txn.put_graph(
        graph,
        &k0,
        &SerializedEdge::new(flag, k1.change, k1.start, introduced_by),
    )?;
    let b = txn.put_graph(
        graph,
        &k1,
        &SerializedEdge::new(flag | EdgeFlags::PARENT, k0.change, k0.end, introduced_by),
    )?;
    assert!(!(a ^ b));

    Ok(a && b)
}

pub(crate) fn register_change<
    T: GraphMutTxnT + DepsMutTxnT<DepsError = <T as GraphTxnT>::GraphError>,
>(
    txn: &mut T,
    internal: &ChangeId,
    hash: &Hash,
    change: &Change,
) -> Result<(), TxnErr<T::GraphError>> {
    debug!("registering change {:?}", hash);
    let shash = hash.into();
    txn.put_external(internal, &shash)?;
    txn.put_internal(&shash, internal)?;
    for dep in change.dependencies.iter() {
        debug!("dep = {:?}", dep);
        let dep_internal = *txn.get_internal(&dep.into())?.unwrap();
        debug!("{:?} depends on {:?}", internal, dep_internal);
        txn.put_revdep(&dep_internal, internal)?;
        txn.put_dep(internal, &dep_internal)?;
    }
    for hunk in change.changes.iter().flat_map(|r| r.iter()) {
        let (inode, pos) = match *hunk {
            Atom::NewVertex(NewVertex {
                ref inode,
                ref flag,
                ref start,
                ref end,
                ..
            }) => {
                if flag.contains(EdgeFlags::FOLDER) && start == end {
                    (inode, Some(*start))
                } else {
                    (inode, None)
                }
            }
            Atom::EdgeMap(EdgeMap { ref inode, .. }) => (inode, None),
        };
        let change = if let Some(c) = inode.change {
            txn.get_internal(&c.into())?.unwrap_or(internal)
        } else {
            internal
        };
        let inode = Position {
            change: *change,
            pos: inode.pos,
        };
        debug!("touched: {:?} {:?}", inode, internal);
        txn.put_touched_files(&inode, internal)?;
        txn.put_rev_touched_files(internal, &inode)?;
        if let Some(pos) = pos {
            let inode = Position {
                change: *internal,
                pos,
            };
            txn.put_touched_files(&inode, internal)?;
            txn.put_rev_touched_files(internal, &inode)?;
        }
    }
    Ok(())
}

fn first_state_after<T: ChannelTxnT>(
    txn: &T,
    c: &T::Channel,
    pos: u64,
) -> Result<Option<(u64, SerializedMerkle)>, TxnErr<T::GraphError>> {
    for x in T::cursor_revchangeset_ref(txn, txn.rev_changes(&c), Some(pos.into()))? {
        let (&n, m) = x?;
        let n: u64 = n.into();
        if n >= pos {
            return Ok(Some((n, m.b.clone())));
        }
    }
    Ok(None)
}

fn last_state<T: ChannelTxnT>(
    txn: &T,
    c: &T::Channel,
) -> Result<Option<(u64, SerializedMerkle)>, TxnErr<T::GraphError>> {
    if let Some(e) = txn
        .rev_cursor_revchangeset(txn.rev_changes(&c), None)?
        .next()
    {
        let (&b, state) = e?;
        let b: u64 = b.into();
        Ok(Some((b, state.b.clone())))
    } else {
        Ok(None)
    }
}

/// Find the last state of c1 that is also in c0.
pub fn last_common_state<T: ChannelTxnT>(
    txn: &T,
    c0: &T::Channel,
    c1: &T::Channel,
) -> Result<(u64, u64, SerializedMerkle), TxnErr<T::GraphError>> {
    let mut a = 0;
    let (mut b, mut state) = if let Some(x) = last_state(txn, c1)? {
        x
    } else {
        return Ok((0, 0, Merkle::zero().into()));
    };
    if let Some(aa) = txn.channel_has_state(txn.states(c0), &state)? {
        return Ok((aa.into(), b, state));
    }
    let mut aa = 0;
    let mut a_was_found = false;
    while a < b {
        let mid = (a + b) / 2;
        let (_, s) = first_state_after(txn, c1, mid)?.unwrap();
        state = s;
        if let Some(aa_) = txn.channel_has_state(txn.states(c0), &state)? {
            aa = aa_.into();
            a_was_found = true;
            if a == mid {
                break;
            } else {
                a = mid
            }
        } else {
            b = mid
        }
    }
    if a_was_found {
        Ok((aa, a, state))
    } else {
        Ok((0, 0, Merkle::zero().into()))
    }
}

/// Check that each inode in the inodes table maps to an alive vertex,
/// and that each inode in the tree table is reachable by only one
/// path.
pub fn check_tree_inodes<T: GraphTxnT + TreeTxnT>(txn: &T, channel: &T::Graph) {
    // Sanity check
    for x in txn.iter_inodes().unwrap() {
        let (inode, vertex) = x.unwrap();
        let mut inode_ = *inode;
        while !inode_.is_root() {
            if let Some(next) = txn.get_revtree(&inode_, None).unwrap() {
                inode_ = next.parent_inode;
            } else {
                panic!("inode = {:?}, inode_ = {:?}", inode, inode_);
            }
        }
        if !is_alive(txn, &channel, &vertex.inode_vertex()).unwrap() {
            for e in iter_adj_all(txn, channel, vertex.inode_vertex()).unwrap() {
                error!("{:?} {:?} {:?}", inode, vertex, e.unwrap())
            }
            panic!(
                "inode {:?}, vertex {:?}, is not alive, {:?}",
                inode,
                vertex,
                tree_path(txn, vertex)
            )
        }
    }
    let mut h = HashMap::default();
    let id0 = OwnedPathId {
        parent_inode: Inode::ROOT,
        basename: crate::small_string::SmallString::new(),
    };
    for x in txn.iter_tree(&id0, None).unwrap() {
        let (id, inode) = x.unwrap();
        if let Some(inode_) = h.insert(id.to_owned(), inode) {
            panic!("id {:?} maps to two inodes: {:?} {:?}", id, inode, inode_);
        }
    }
}

/// Check that each alive vertex in the graph is reachable, and vice-versa.
pub fn check_alive_debug<T: GraphIter + ChannelTxnT, C: crate::changestore::ChangeStore>(
    changes: &C,
    txn: &T,
    channel: &T::Channel,
    line: u32,
) -> Result<(), std::io::Error> {
    let (alive, reachable) = crate::pristine::check_alive(txn, txn.graph(channel));
    let mut h = HashSet::default();
    if !alive.is_empty() {
        for (k, file) in alive.iter() {
            debug!("alive = {:?}, file = {:?}", k, file);
            h.insert(file);
        }
    }
    if !reachable.is_empty() {
        for (k, file) in reachable.iter() {
            debug!("reachable = {:?}, file = {:?}", k, file);
            h.insert(file);
        }
    }
    for file in h.iter() {
        let file_ = file.unwrap().start_pos();

        let (path, _) = crate::fs::find_path(changes, txn, channel, true, file_)
            .unwrap()
            .unwrap();
        let path = path.replace("/", "_");
        let name = format!(
            "debug_{:?}_{}_{}",
            path,
            file_.change.to_base32(),
            file_.pos.0
        );
        let mut f = std::fs::File::create(&name)?;
        let graph = crate::alive::retrieve::retrieve(txn, txn.graph(channel), file_).unwrap();
        graph.debug(changes, txn, txn.graph(channel), false, false, &mut f)?;

        let mut f = std::fs::File::create(&format!("{}_all", name))?;
        debug_root(txn, txn.graph(channel), file.unwrap(), &mut f, false)?;
    }
    if !h.is_empty() {
        if !alive.is_empty() {
            panic!("alive call line {}: {:?}", line, alive);
        } else {
            panic!("reachable: {:?}", reachable);
        }
    }
    Ok(())
}
