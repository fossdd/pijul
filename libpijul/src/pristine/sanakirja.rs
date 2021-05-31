use super::*;
use crate::HashMap;
use ::sanakirja::*;
use std::collections::hash_map::Entry;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A Sanakirja pristine.
pub struct Pristine {
    pub env: Arc<::sanakirja::Env>,
}

pub(crate) type P<K, V> = btree::page::Page<K, V>;
type Db<K, V> = btree::Db<K, V>;
pub(crate) type UP<K, V> = btree::page_unsized::Page<K, V>;
type UDb<K, V> = btree::Db_<K, V, UP<K, V>>;

#[derive(Debug, Error)]
pub enum SanakirjaError {
    #[error(transparent)]
    Sanakirja(#[from] ::sanakirja::Error),
    #[error("Pristine locked")]
    PristineLocked,
    #[error("Pristine corrupt")]
    PristineCorrupt,
    #[error(transparent)]
    Borrow(#[from] std::cell::BorrowError),
    #[error("Cannot dropped a borrowed channel: {:?}", c)]
    ChannelRc { c: String },
    #[error("Pristine version mismatch. Cloning over the network can fix this.")]
    Version,
}

impl std::convert::From<::sanakirja::CRCError> for SanakirjaError {
    fn from(_: ::sanakirja::CRCError) -> Self {
        SanakirjaError::PristineCorrupt
    }
}

impl std::convert::From<::sanakirja::CRCError> for TxnErr<SanakirjaError> {
    fn from(_: ::sanakirja::CRCError) -> Self {
        TxnErr(SanakirjaError::PristineCorrupt)
    }
}

impl std::convert::From<::sanakirja::Error> for TxnErr<SanakirjaError> {
    fn from(e: ::sanakirja::Error) -> Self {
        TxnErr(e.into())
    }
}

impl std::convert::From<TxnErr<::sanakirja::Error>> for TxnErr<SanakirjaError> {
    fn from(e: TxnErr<::sanakirja::Error>) -> Self {
        TxnErr(e.0.into())
    }
}

impl Pristine {
    pub fn new<P: AsRef<Path>>(name: P) -> Result<Self, SanakirjaError> {
        Self::new_with_size(name, 1 << 20)
    }
    pub unsafe fn new_nolock<P: AsRef<Path>>(name: P) -> Result<Self, SanakirjaError> {
        Self::new_with_size_nolock(name, 1 << 20)
    }
    pub fn new_with_size<P: AsRef<Path>>(name: P, size: u64) -> Result<Self, SanakirjaError> {
        let env = ::sanakirja::Env::new(name, size, 2);
        match env {
            Ok(env) => Ok(Pristine { env: Arc::new(env) }),
            Err(::sanakirja::Error::IO(e)) => {
                if let std::io::ErrorKind::WouldBlock = e.kind() {
                    Err(SanakirjaError::PristineLocked)
                } else {
                    Err(SanakirjaError::Sanakirja(::sanakirja::Error::IO(e)))
                }
            }
            Err(e) => Err(SanakirjaError::Sanakirja(e)),
        }
    }
    pub unsafe fn new_with_size_nolock<P: AsRef<Path>>(
        name: P,
        size: u64,
    ) -> Result<Self, SanakirjaError> {
        Ok(Pristine {
            env: Arc::new(::sanakirja::Env::new_nolock(name, size, 2)?),
        })
    }
    pub fn new_anon() -> Result<Self, SanakirjaError> {
        Self::new_anon_with_size(1 << 20)
    }
    pub fn new_anon_with_size(size: u64) -> Result<Self, SanakirjaError> {
        Ok(Pristine {
            env: Arc::new(::sanakirja::Env::new_anon(size, 2)?),
        })
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(usize)]
pub enum Root {
    Version,
    Tree,
    RevTree,
    Inodes,
    RevInodes,
    Internal,
    External,
    RevDep,
    Channels,
    TouchedFiles,
    Dep,
    RevTouchedFiles,
    Partials,
    Remotes,
}

const VERSION: L64 = L64(1u64.to_le());

impl Pristine {
    pub fn txn_begin(&self) -> Result<Txn, SanakirjaError> {
        let txn = ::sanakirja::Env::txn_begin(self.env.clone())?;
        if L64(txn.root(Root::Version as usize)) != VERSION {
            return Err(SanakirjaError::Version);
        }
        fn begin(txn: ::sanakirja::Txn<Arc<::sanakirja::Env>>) -> Option<Txn> {
            Some(Txn {
                channels: txn.root_db(Root::Channels as usize)?,
                external: txn.root_db(Root::External as usize)?,
                internal: txn.root_db(Root::Internal as usize)?,
                inodes: txn.root_db(Root::Inodes as usize)?,
                revinodes: txn.root_db(Root::RevInodes as usize)?,
                tree: txn.root_db(Root::Tree as usize)?,
                revtree: txn.root_db(Root::RevTree as usize)?,
                revdep: txn.root_db(Root::RevDep as usize)?,
                touched_files: txn.root_db(Root::TouchedFiles as usize)?,
                rev_touched_files: txn.root_db(Root::RevTouchedFiles as usize)?,
                partials: txn.root_db(Root::Partials as usize)?,
                dep: txn.root_db(Root::Dep as usize)?,
                remotes: txn.root_db(Root::Remotes as usize)?,
                open_channels: Mutex::new(HashMap::default()),
                open_remotes: Mutex::new(HashMap::default()),
                txn,
                counter: 0,
            })
        }
        if let Some(txn) = begin(txn) {
            Ok(txn)
        } else {
            Err(SanakirjaError::PristineCorrupt)
        }
    }

    pub fn mut_txn_begin(&self) -> Result<MutTxn<()>, SanakirjaError> {
        let mut txn = ::sanakirja::Env::mut_txn_begin(self.env.clone()).unwrap();
        if let Some(version) = txn.root(Root::Version as usize) {
            if L64(version) != VERSION {
                return Err(SanakirjaError::Version.into());
            }
        } else {
            txn.set_root(Root::Version as usize, VERSION.0);
        }
        Ok(MutTxn {
            channels: if let Some(db) = txn.root_db(Root::Channels as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            external: if let Some(db) = txn.root_db(Root::External as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            internal: if let Some(db) = txn.root_db(Root::Internal as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            inodes: if let Some(db) = txn.root_db(Root::Inodes as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            revinodes: if let Some(db) = txn.root_db(Root::RevInodes as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            tree: if let Some(db) = txn.root_db(Root::Tree as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            revtree: if let Some(db) = txn.root_db(Root::RevTree as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            revdep: if let Some(db) = txn.root_db(Root::RevDep as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            dep: if let Some(db) = txn.root_db(Root::Dep as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            touched_files: if let Some(db) = txn.root_db(Root::TouchedFiles as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            rev_touched_files: if let Some(db) = txn.root_db(Root::RevTouchedFiles as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            partials: if let Some(db) = txn.root_db(Root::Partials as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            remotes: if let Some(db) = txn.root_db(Root::Remotes as usize) {
                db
            } else {
                btree::create_db_(&mut txn)?
            },
            open_channels: Mutex::new(HashMap::default()),
            open_remotes: Mutex::new(HashMap::default()),
            txn,
            counter: 0,
        })
    }
}

pub type Txn = GenericTxn<::sanakirja::Txn<Arc<::sanakirja::Env>>>;
pub type MutTxn<T> = GenericTxn<::sanakirja::MutTxn<Arc<::sanakirja::Env>, T>>;

/// A transaction, used both for mutable and immutable transactions,
/// depending on type parameter `T`.
///
/// In Sanakirja, both `sanakirja::Txn` and `sanakirja::MutTxn`
/// implement `sanakirja::Transaction`, explaining our implementation
/// of `TxnT` for `Txn<T>` for all `T: sanakirja::Transaction`. This
/// covers both mutable and immutable transactions in a single
/// implementation.
pub struct GenericTxn<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> {
    #[doc(hidden)]
    pub txn: T,
    #[doc(hidden)]
    pub internal: UDb<SerializedHash, ChangeId>,
    #[doc(hidden)]
    pub external: UDb<ChangeId, SerializedHash>,
    inodes: Db<Inode, Position<ChangeId>>,
    revinodes: Db<Position<ChangeId>, Inode>,

    pub tree: UDb<PathId, Inode>,
    revtree: UDb<Inode, PathId>,

    revdep: Db<ChangeId, ChangeId>,
    dep: Db<ChangeId, ChangeId>,

    touched_files: Db<Position<ChangeId>, ChangeId>,
    rev_touched_files: Db<ChangeId, Position<ChangeId>>,

    partials: UDb<SmallStr, Position<ChangeId>>,
    channels: UDb<SmallStr, T8>,
    remotes: UDb<SmallStr, T3>,
    pub(crate) open_channels: Mutex<HashMap<SmallString, ChannelRef<Self>>>,
    open_remotes: Mutex<HashMap<SmallString, RemoteRef<Self>>>,
    counter: usize,
}

/// This is actually safe because the only non-Send fields are
/// `open_channels` and `open_remotes`, but we can't do anything with
/// a `ChannelRef` whose transaction has been moved to another thread.
unsafe impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> Send for GenericTxn<T> {}

impl Txn {
    pub fn check_database(&self) {
        let mut refs = std::collections::BTreeMap::new();
        debug!("check: internal 0x{:x}", self.internal.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.internal, &mut refs).unwrap();
        debug!("check: external 0x{:x}", self.external.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.external, &mut refs).unwrap();
        debug!("check: inodes 0x{:x}", self.inodes.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.inodes, &mut refs).unwrap();
        debug!("check: revinodes 0x{:x}", self.revinodes.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.revinodes, &mut refs).unwrap();
        debug!("check: tree 0x{:x}", self.tree.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.tree, &mut refs).unwrap();
        debug!("check: revtree 0x{:x}", self.revtree.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.revtree, &mut refs).unwrap();
        debug!("check: revdep 0x{:x}", self.revdep.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.revdep, &mut refs).unwrap();
        debug!("check: dep 0x{:x}", self.dep.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.dep, &mut refs).unwrap();
        debug!("check: touched_files 0x{:x}", self.touched_files.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.touched_files, &mut refs).unwrap();
        debug!("check: rev_touched_files 0x{:x}", self.rev_touched_files.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.rev_touched_files, &mut refs).unwrap();
        debug!("check: partials 0x{:x}", self.partials.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.partials, &mut refs).unwrap();
        debug!("check: channels 0x{:x}", self.channels.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.channels, &mut refs).unwrap();
        for x in btree::iter(&self.txn, &self.channels, None).unwrap() {
            let (name, tup) = x.unwrap();
            debug!("check: channel name: {:?}", name.as_str());
            let graph: Db<Vertex<ChangeId>, SerializedEdge> = Db::from_page(tup.0[0].into());
            let changes: Db<ChangeId, L64> = Db::from_page(tup.0[1].into());
            let revchanges: UDb<L64, Pair<ChangeId, SerializedMerkle>> =
                UDb::from_page(tup.0[2].into());
            let states: UDb<SerializedMerkle, L64> = UDb::from_page(tup.0[3].into());
            let tags: UDb<L64, SerializedHash> = UDb::from_page(tup.0[4].into());
            debug!("check: graph 0x{:x}", graph.db);
            ::sanakirja::debug::add_refs(&self.txn, &graph, &mut refs).unwrap();
            debug!("check: changes 0x{:x}", changes.db);
            ::sanakirja::debug::add_refs(&self.txn, &changes, &mut refs).unwrap();
            debug!("check: revchanges 0x{:x}", revchanges.db);
            ::sanakirja::debug::add_refs(&self.txn, &revchanges, &mut refs).unwrap();
            debug!("check: states 0x{:x}", states.db);
            ::sanakirja::debug::add_refs(&self.txn, &states, &mut refs).unwrap();
            debug!("check: tags 0x{:x}", tags.db);
            ::sanakirja::debug::add_refs(&self.txn, &tags, &mut refs).unwrap();
        }
        debug!("check: remotes 0x{:x}", self.remotes.db);
        ::sanakirja::debug::add_refs(&self.txn, &self.remotes, &mut refs).unwrap();
        for x in btree::iter(&self.txn, &self.remotes, None).unwrap() {
            let (name, tup) = x.unwrap();
            debug!("check: remote name: {:?}", name.as_str());
            let remote: UDb<SmallStr, T3> = UDb::from_page(tup.0[0].into());
            let rev: UDb<SerializedHash, L64> = UDb::from_page(tup.0[1].into());
            let states: UDb<SerializedMerkle, L64> = UDb::from_page(tup.0[2].into());
            debug!("check: remote 0x{:x}", remote.db);
            ::sanakirja::debug::add_refs(&self.txn, &remote, &mut refs).unwrap();
            debug!("check: rev 0x{:x}", rev.db);
            ::sanakirja::debug::add_refs(&self.txn, &rev, &mut refs).unwrap();
            debug!("check: states 0x{:x}", states.db);
            ::sanakirja::debug::add_refs(&self.txn, &states, &mut refs).unwrap();
        }
        ::sanakirja::debug::add_free_refs(&self.txn, &mut refs).unwrap();
        ::sanakirja::debug::check_free(&self.txn, &refs);
    }
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> GraphTxnT for GenericTxn<T> {
    type Graph = Db<Vertex<ChangeId>, SerializedEdge>;
    type GraphError = SanakirjaError;

    sanakirja_get!(graph, Vertex<ChangeId>, SerializedEdge, GraphError);
    fn get_external(
        &self,
        p: &ChangeId,
    ) -> Result<Option<&SerializedHash>, TxnErr<Self::GraphError>> {
        debug!("get_external {:?}", p);
        if p.is_root() {
            Ok(Some(&HASH_NONE))
        } else {
            match btree::get(&self.txn, &self.external, p, None) {
                Ok(Some((k, v))) if k == p => Ok(Some(v)),
                Ok(_) => Ok(None),
                Err(e) => {
                    error!("{:?}", e);
                    Err(TxnErr(SanakirjaError::PristineCorrupt))
                }
            }
        }
    }

    fn get_internal(
        &self,
        p: &SerializedHash,
    ) -> Result<Option<&ChangeId>, TxnErr<Self::GraphError>> {
        if p.t == HashAlgorithm::None as u8 {
            Ok(Some(&ChangeId::ROOT))
        } else {
            match btree::get(&self.txn, &self.internal, &p, None) {
                Ok(Some((k, v))) if k == p => Ok(Some(v)),
                Ok(_) => Ok(None),
                Err(e) => {
                    error!("{:?}", e);
                    Err(TxnErr(SanakirjaError::PristineCorrupt))
                }
            }
        }
    }

    type Adj = Adj;

    fn init_adj(
        &self,
        g: &Self::Graph,
        key: Vertex<ChangeId>,
        dest: Position<ChangeId>,
        min_flag: EdgeFlags,
        max_flag: EdgeFlags,
    ) -> Result<Self::Adj, TxnErr<Self::GraphError>> {
        let edge = SerializedEdge::new(min_flag, dest.change, dest.pos, ChangeId::ROOT);
        let mut cursor = btree::cursor::Cursor::new(&self.txn, g).map_err(TxnErr)?;
        cursor.set(&self.txn, &key, Some(&edge))?;
        Ok(Adj {
            cursor,
            key,
            min_flag,
            max_flag,
        })
    }

    fn next_adj<'a>(
        &'a self,
        _: &Self::Graph,
        a: &mut Self::Adj,
    ) -> Option<Result<&'a SerializedEdge, TxnErr<Self::GraphError>>> {
        next_adj(&self.txn, a)
    }

    fn find_block(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        find_block(&self.txn, graph, p)
    }

    fn find_block_end(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        find_block_end(&self.txn, graph, p)
    }
}

#[doc(hidden)]
pub fn next_adj<'a, T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>>(
    txn: &'a T,
    a: &mut Adj,
) -> Option<Result<&'a SerializedEdge, TxnErr<SanakirjaError>>> {
    loop {
        let x: Result<Option<(&Vertex<ChangeId>, &SerializedEdge)>, _> = a.cursor.next(txn);
        match x {
            Ok(Some((v, e))) => {
                if *v == a.key {
                    if e.flag() >= a.min_flag {
                        if e.flag() <= a.max_flag {
                            return Some(Ok(e));
                        } else {
                            return None;
                        }
                    }
                } else if *v > a.key {
                    return None;
                }
            }
            Err(e) => return Some(Err(TxnErr(e.into()))),
            Ok(None) => {
                return None;
            }
        }
    }
}

#[doc(hidden)]
pub fn find_block<'a, T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>>(
    txn: &'a T,
    graph: &::sanakirja::btree::Db<Vertex<ChangeId>, SerializedEdge>,
    p: Position<ChangeId>,
) -> Result<&'a Vertex<ChangeId>, BlockError<SanakirjaError>> {
    if p.change.is_root() {
        return Ok(&Vertex::ROOT);
    }
    let key = Vertex {
        change: p.change,
        start: p.pos,
        end: p.pos,
    };
    let mut cursor =
        btree::cursor::Cursor::new(txn, &graph).map_err(|x| BlockError::Txn(x.into()))?;
    let mut k = if let Some((k, _)) = cursor
        .set(txn, &key, None)
        .map_err(|x| BlockError::Txn(x.into()))?
    {
        k
    } else if let Some((k, _)) = cursor.prev(txn).map_err(|x| BlockError::Txn(x.into()))? {
        k
    } else {
        debug!("find_block: BLOCK ERROR");
        return Err(BlockError::Block { block: p });
    };
    // The only guarantee here is that k is either the first key >=
    // `key`. We might need to rewind by one step if key is strictly
    // larger than the result (i.e. if `p` is in the middle of the
    // key).
    while k.change > p.change || (k.change == p.change && k.start > p.pos) {
        if let Some((k_, _)) = cursor.prev(txn).map_err(|x| BlockError::Txn(x.into()))? {
            k = k_
        } else {
            break;
        }
    }
    loop {
        if k.change == p.change && k.start <= p.pos {
            if k.end > p.pos || (k.start == k.end && k.end == p.pos) {
                return Ok(k);
            }
        } else if k.change > p.change {
            debug!("find_block: BLOCK ERROR");
            return Err(BlockError::Block { block: p });
        }
        if let Some((k_, _)) = cursor.next(txn).map_err(|x| BlockError::Txn(x.into()))? {
            k = k_
        } else {
            break;
        }
    }
    debug!("find_block: BLOCK ERROR");
    Err(BlockError::Block { block: p })
}

#[doc(hidden)]
pub fn find_block_end<'a, T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>>(
    txn: &'a T,
    graph: &::sanakirja::btree::Db<Vertex<ChangeId>, SerializedEdge>,
    p: Position<ChangeId>,
) -> Result<&'a Vertex<ChangeId>, BlockError<SanakirjaError>> {
    if p.change.is_root() {
        return Ok(&Vertex::ROOT);
    }
    let key = Vertex {
        change: p.change,
        start: p.pos,
        end: p.pos,
    };
    let mut cursor = ::sanakirja::btree::cursor::Cursor::new(txn, graph)
        .map_err(|x| BlockError::Txn(x.into()))?;
    let mut k = match cursor.set(txn, &key, None) {
        Ok(Some((k, _))) => k,
        Ok(None) => {
            if let Some((k, _)) = cursor.prev(txn).map_err(|x| BlockError::Txn(x.into()))? {
                k
            } else {
                debug!("find_block_end, no prev");
                return Err(BlockError::Block { block: p });
            }
        }
        Err(e) => {
            debug!("find_block_end: BLOCK ERROR");
            return Err(BlockError::Txn(e.into()));
        }
    };
    loop {
        debug!("find_block_end, loop, k = {:?}, p = {:?}", k, p);
        if k.change < p.change {
            break;
        } else if k.change == p.change {
            // Here we want to create an edge pointing between `p`
            // and its successor. If k.start == p.pos, the only
            // case where that's what we want is if k.start ==
            // k.end.
            if k.start == p.pos && k.end == p.pos {
                return Ok(k);
            } else if k.start < p.pos {
                break;
            }
        }
        if let Some((k_, _)) = cursor.prev(txn).map_err(|x| BlockError::Txn(x.into()))? {
            k = k_
        } else {
            break;
        }
    }
    // We also want k.end >= p.pos, so we just call next() until
    // we have that.
    debug!("find_block_end, {:?} {:?}", k, p);
    while k.change < p.change || (k.change == p.change && p.pos > k.end) {
        if let Some((k_, _)) = cursor.next(txn).map_err(|x| BlockError::Txn(x.into()))? {
            k = k_
        } else {
            break;
        }
    }
    debug!("find_block_end, {:?} {:?}", k, p);
    if k.change == p.change
        && ((k.start < p.pos && p.pos <= k.end) || (k.start == k.end && k.start == p.pos))
    {
        Ok(k)
    } else {
        debug!("find_block_end: BLOCK ERROR");
        Err(BlockError::Block { block: p })
    }
}

pub struct Adj {
    pub cursor: ::sanakirja::btree::cursor::Cursor<
        Vertex<ChangeId>,
        SerializedEdge,
        P<Vertex<ChangeId>, SerializedEdge>,
    >,
    pub key: Vertex<ChangeId>,
    pub min_flag: EdgeFlags,
    pub max_flag: EdgeFlags,
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> GraphIter for GenericTxn<T> {
    type GraphCursor = ::sanakirja::btree::cursor::Cursor<
        Vertex<ChangeId>,
        SerializedEdge,
        P<Vertex<ChangeId>, SerializedEdge>,
    >;

    fn iter_graph(
        &self,
        g: &Self::Graph,
        s: Option<&Vertex<ChangeId>>,
    ) -> Result<Self::GraphCursor, TxnErr<Self::GraphError>> {
        let mut c = ::sanakirja::btree::cursor::Cursor::new(&self.txn, &g)?;
        if let Some(s) = s {
            c.set(&self.txn, s, None)?;
        }
        Ok(c)
    }

    fn next_graph<'txn>(
        &'txn self,
        _: &Self::Graph,
        a: &mut Self::GraphCursor,
    ) -> Option<Result<(&'txn Vertex<ChangeId>, &'txn SerializedEdge), TxnErr<Self::GraphError>>>
    {
        match a.next(&self.txn) {
            Ok(Some(x)) => Some(Ok(x)),
            Ok(None) => None,
            Err(e) => {
                error!("{:?}", e);
                Some(Err(TxnErr(SanakirjaError::PristineCorrupt)))
            }
        }
    }
}

// There is a choice here: the datastructure for `revchanges` is
// intuitively a list. Moreover, when removing a change, we must
// recompute the entire merkle tree after the removed change.
//
// This seems to indicate that a linked list could be an appropriate
// structure (a growable array is excluded because amortised
// complexity is not really acceptable here).
//
// However, we want to be able to answers queries such as "when was
// change X introduced?" without having to read the entire database.
//
// Additionally, even though `SerializedMerkle` has only one
// implementation, and is therefore sized in the current
// implementation, we can't exclude that other algorithms may be
// added, which means that the pages inside linked lists won't even be
// randomly-accessible arrays.
pub struct Channel {
    pub graph: Db<Vertex<ChangeId>, SerializedEdge>,
    pub changes: Db<ChangeId, L64>,
    pub revchanges: UDb<L64, Pair<ChangeId, SerializedMerkle>>,
    pub states: UDb<SerializedMerkle, L64>,
    pub tags: UDb<L64, SerializedHash>,
    pub apply_counter: ApplyTimestamp,
    pub name: SmallString,
    pub last_modified: u64,
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> ChannelTxnT for GenericTxn<T> {
    type Channel = Channel;

    fn graph<'a>(&self, c: &'a Self::Channel) -> &'a Db<Vertex<ChangeId>, SerializedEdge> {
        &c.graph
    }
    fn name<'a>(&self, c: &'a Self::Channel) -> &'a str {
        c.name.as_str()
    }
    fn apply_counter(&self, channel: &Self::Channel) -> u64 {
        channel.apply_counter.into()
    }
    fn last_modified(&self, channel: &Self::Channel) -> u64 {
        channel.last_modified.into()
    }
    fn changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Changeset {
        &channel.changes
    }
    fn rev_changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::RevChangeset {
        &channel.revchanges
    }
    fn tags<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Tags {
        &channel.tags
    }

    type Changeset = Db<ChangeId, L64>;
    type RevChangeset = UDb<L64, Pair<ChangeId, SerializedMerkle>>;

    fn get_changeset(
        &self,
        channel: &Self::Changeset,
        c: &ChangeId,
    ) -> Result<Option<&L64>, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, channel, c, None) {
            Ok(Some((k, x))) if k == c => Ok(Some(x)),
            Ok(x) => {
                debug!("get_changeset = {:?}", x);
                Ok(None)
            }
            Err(e) => {
                error!("{:?}", e);
                Err(TxnErr(SanakirjaError::PristineCorrupt))
            }
        }
    }
    fn get_revchangeset(
        &self,
        revchanges: &Self::RevChangeset,
        c: &L64,
    ) -> Result<Option<&Pair<ChangeId, SerializedMerkle>>, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, revchanges, c, None) {
            Ok(Some((k, x))) if k == c => Ok(Some(x)),
            Ok(_) => Ok(None),
            Err(e) => {
                error!("{:?}", e);
                Err(TxnErr(SanakirjaError::PristineCorrupt))
            }
        }
    }

    type ChangesetCursor = ::sanakirja::btree::cursor::Cursor<ChangeId, L64, P<ChangeId, L64>>;

    fn cursor_changeset<'a>(
        &'a self,
        channel: &Self::Changeset,
        pos: Option<ChangeId>,
    ) -> Result<Cursor<Self, &'a Self, Self::ChangesetCursor, ChangeId, L64>, TxnErr<SanakirjaError>>
    {
        let mut cursor = btree::cursor::Cursor::new(&self.txn, &channel)?;
        if let Some(k) = pos {
            cursor.set(&self.txn, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    type RevchangesetCursor = ::sanakirja::btree::cursor::Cursor<
        L64,
        Pair<ChangeId, SerializedMerkle>,
        UP<L64, Pair<ChangeId, SerializedMerkle>>,
    >;

    fn cursor_revchangeset_ref<'a, RT: std::ops::Deref<Target = Self>>(
        txn: RT,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        Cursor<Self, RT, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<SanakirjaError>,
    > {
        let mut cursor = btree::cursor::Cursor::new(&txn.txn, channel)?;
        if let Some(k) = pos {
            cursor.set(&txn.txn, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    fn rev_cursor_revchangeset<'a>(
        &'a self,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        RevCursor<Self, &'a Self, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<SanakirjaError>,
    > {
        let mut cursor = btree::cursor::Cursor::new(&self.txn, channel)?;
        if let Some(ref pos) = pos {
            cursor.set(&self.txn, pos, None)?;
        } else {
            cursor.set_last(&self.txn)?;
        };
        Ok(RevCursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    fn cursor_revchangeset_next(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<SanakirjaError>> {
        if let Ok(x) = cursor.next(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }
    fn cursor_revchangeset_prev(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<SanakirjaError>> {
        if let Ok(x) = cursor.prev(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }

    fn cursor_changeset_next(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<SanakirjaError>> {
        if let Ok(x) = cursor.next(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }
    fn cursor_changeset_prev(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<SanakirjaError>> {
        if let Ok(x) = cursor.prev(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }

    type States = UDb<SerializedMerkle, L64>;
    fn states<'a>(&self, channel: &'a Self::Channel) -> &'a Self::States {
        &channel.states
    }
    fn channel_has_state(
        &self,
        channel: &Self::States,
        m: &SerializedMerkle,
    ) -> Result<Option<L64>, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, channel, m, None)? {
            Some((k, v)) if k == m => Ok(Some(*v)),
            _ => Ok(None),
        }
    }

    type Tags = UDb<L64, SerializedHash>;
    fn get_tags(
        &self,
        channel: &Self::Tags,
        c: &L64,
    ) -> Result<Option<&SerializedHash>, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, channel, c, None)? {
            Some((k, v)) if k == c => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    type TagsCursor =
        ::sanakirja::btree::cursor::Cursor<L64, SerializedHash, UP<L64, SerializedHash>>;
    fn cursor_tags<'txn>(
        &'txn self,
        channel: &Self::Tags,
        k: Option<L64>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::TagsCursor, L64, SerializedHash>,
        TxnErr<Self::GraphError>,
    > {
        let mut cursor = btree::cursor::Cursor::new(&self.txn, channel)?;
        if let Some(k) = k {
            cursor.set(&self.txn, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }
    fn cursor_tags_next(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &SerializedHash)>, TxnErr<Self::GraphError>> {
        if let Ok(x) = cursor.next(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }

    fn cursor_tags_prev(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &SerializedHash)>, TxnErr<Self::GraphError>> {
        if let Ok(x) = cursor.prev(&self.txn) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt))
        }
    }

    fn iter_tags(
        &self,
        channel: &Self::Tags,
        from: u64,
    ) -> Result<
        super::Cursor<Self, &Self, Self::TagsCursor, L64, SerializedHash>,
        TxnErr<Self::GraphError>,
    > {
        self.cursor_tags(channel, Some(from.into()))
    }

    fn rev_iter_tags(
        &self,
        channel: &Self::Tags,
        from: Option<u64>,
    ) -> Result<
        super::RevCursor<Self, &Self, Self::TagsCursor, L64, SerializedHash>,
        TxnErr<Self::GraphError>,
    > {
        let mut cursor = btree::cursor::Cursor::new(&self.txn, channel)?;
        if let Some(from) = from {
            cursor.set(&self.txn, &from.into(), None)?;
        } else {
            cursor.set_last(&self.txn)?;
        };
        Ok(RevCursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> DepsTxnT for GenericTxn<T> {
    type DepsError = SanakirjaError;
    type Dep = Db<ChangeId, ChangeId>;
    type Revdep = Db<ChangeId, ChangeId>;

    sanakirja_table_get!(dep, ChangeId, ChangeId, DepsError);
    sanakirja_table_get!(revdep, ChangeId, ChangeId, DepsError);
    type DepCursor = ::sanakirja::btree::cursor::Cursor<ChangeId, ChangeId, P<ChangeId, ChangeId>>;
    sanakirja_cursor_ref!(dep, ChangeId, ChangeId);
    fn iter_dep_ref<RT: std::ops::Deref<Target = Self> + Clone>(
        txn: RT,
        p: &ChangeId,
    ) -> Result<super::Cursor<Self, RT, Self::DepCursor, ChangeId, ChangeId>, TxnErr<Self::DepsError>>
    {
        Self::cursor_dep_ref(txn.clone(), &txn.dep, Some((p, None)))
    }

    sanakirja_table_get!(touched_files, Position<ChangeId>, ChangeId, DepsError);
    sanakirja_table_get!(rev_touched_files, ChangeId, Position<ChangeId>, DepsError);

    type Touched_files = Db<Position<ChangeId>, ChangeId>;

    type Rev_touched_files = Db<ChangeId, Position<ChangeId>>;

    type Touched_filesCursor = ::sanakirja::btree::cursor::Cursor<
        Position<ChangeId>,
        ChangeId,
        P<Position<ChangeId>, ChangeId>,
    >;
    sanakirja_iter!(touched_files, Position<ChangeId>, ChangeId);

    type Rev_touched_filesCursor = ::sanakirja::btree::cursor::Cursor<
        ChangeId,
        Position<ChangeId>,
        P<ChangeId, Position<ChangeId>>,
    >;
    sanakirja_iter!(rev_touched_files, ChangeId, Position<ChangeId>);
    fn iter_revdep(
        &self,
        k: &ChangeId,
    ) -> Result<
        super::Cursor<Self, &Self, Self::DepCursor, ChangeId, ChangeId>,
        TxnErr<Self::DepsError>,
    > {
        self.cursor_dep(&self.revdep, Some((k, None)))
    }

    fn iter_dep(
        &self,
        k: &ChangeId,
    ) -> Result<
        super::Cursor<Self, &Self, Self::DepCursor, ChangeId, ChangeId>,
        TxnErr<Self::DepsError>,
    > {
        self.cursor_dep(&self.dep, Some((k, None)))
    }

    fn iter_touched(
        &self,
        k: &Position<ChangeId>,
    ) -> Result<
        super::Cursor<Self, &Self, Self::Touched_filesCursor, Position<ChangeId>, ChangeId>,
        TxnErr<Self::DepsError>,
    > {
        self.cursor_touched_files(&self.touched_files, Some((k, None)))
    }

    fn iter_rev_touched(
        &self,
        k: &ChangeId,
    ) -> Result<
        super::Cursor<Self, &Self, Self::Rev_touched_filesCursor, ChangeId, Position<ChangeId>>,
        TxnErr<Self::DepsError>,
    > {
        self.cursor_rev_touched_files(&self.rev_touched_files, Some((k, None)))
    }
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> TreeTxnT for GenericTxn<T> {
    type TreeError = SanakirjaError;
    type Inodes = Db<Inode, Position<ChangeId>>;
    type Revinodes = Db<Position<ChangeId>, Inode>;
    sanakirja_table_get!(inodes, Inode, Position<ChangeId>, TreeError);
    sanakirja_table_get!(revinodes, Position<ChangeId>, Inode, TreeError);
    sanakirja_cursor!(inodes, Inode, Position<ChangeId>);
    // #[cfg(debug_assertions)]
    sanakirja_cursor!(revinodes, Position<ChangeId>, Inode);

    type Tree = UDb<PathId, Inode>;
    sanakirja_table_get!(tree, PathId, Inode, TreeError,);
    type TreeCursor = ::sanakirja::btree::cursor::Cursor<PathId, Inode, UP<PathId, Inode>>;
    sanakirja_iter!(tree, PathId, Inode,);
    type RevtreeCursor = ::sanakirja::btree::cursor::Cursor<Inode, PathId, UP<Inode, PathId>>;
    sanakirja_iter!(revtree, Inode, PathId);

    type Revtree = UDb<Inode, PathId>;
    sanakirja_table_get!(revtree, Inode, PathId, TreeError,);

    type Partials = UDb<SmallStr, Position<ChangeId>>;
    type PartialsCursor = ::sanakirja::btree::cursor::Cursor<
        SmallStr,
        Position<ChangeId>,
        UP<SmallStr, Position<ChangeId>>,
    >;
    sanakirja_cursor!(partials, SmallStr, Position<ChangeId>,);
    type InodesCursor =
        ::sanakirja::btree::cursor::Cursor<Inode, Position<ChangeId>, P<Inode, Position<ChangeId>>>;
    fn iter_inodes(
        &self,
    ) -> Result<
        super::Cursor<Self, &Self, Self::InodesCursor, Inode, Position<ChangeId>>,
        TxnErr<Self::TreeError>,
    > {
        self.cursor_inodes(&self.inodes, None)
    }

    // #[cfg(debug_assertions)]
    type RevinodesCursor =
        ::sanakirja::btree::cursor::Cursor<Position<ChangeId>, Inode, P<Position<ChangeId>, Inode>>;
    // #[cfg(debug_assertions)]
    fn iter_revinodes(
        &self,
    ) -> Result<
        super::Cursor<Self, &Self, Self::RevinodesCursor, Position<ChangeId>, Inode>,
        TxnErr<SanakirjaError>,
    > {
        self.cursor_revinodes(&self.revinodes, None)
    }

    fn iter_partials<'txn>(
        &'txn self,
        k: &str,
    ) -> Result<
        super::Cursor<Self, &'txn Self, Self::PartialsCursor, SmallStr, Position<ChangeId>>,
        TxnErr<SanakirjaError>,
    > {
        let k0 = SmallString::from_str(k);
        self.cursor_partials(&self.partials, Some((&k0, None)))
    }
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> GenericTxn<T> {
    #[doc(hidden)]
    pub unsafe fn unsafe_load_channel(
        &self,
        name: SmallString,
    ) -> Result<Option<Channel>, TxnErr<SanakirjaError>> {
        match btree::get(&self.txn, &self.channels, &name, None)? {
            Some((name_, tup)) if name_ == name.as_ref() => {
                debug!("load_channel: {:?} {:?}", name, tup);
                Ok(Some(Channel {
                    graph: Db::from_page(tup.0[0].into()),
                    changes: Db::from_page(tup.0[1].into()),
                    revchanges: UDb::from_page(tup.0[2].into()),
                    states: UDb::from_page(tup.0[3].into()),
                    tags: UDb::from_page(tup.0[4].into()),
                    apply_counter: tup.0[5].into(),
                    last_modified: tup.0[6].into(),
                    name,
                }))
            }
            _ => {
                debug!("unsafe_load_channel: not found");
                Ok(None)
            }
        }
    }
}

impl<T: ::sanakirja::LoadPage<Error = ::sanakirja::Error>> TxnT for GenericTxn<T> {
    fn hash_from_prefix(
        &self,
        s: &str,
    ) -> Result<(Hash, ChangeId), super::HashPrefixError<Self::GraphError>> {
        let h: SerializedHash = if let Some(ref h) = Hash::from_prefix(s) {
            h.into()
        } else {
            return Err(super::HashPrefixError::Parse(s.to_string()));
        };
        let mut result = None;
        debug!("h = {:?}", h);
        for x in btree::iter(&self.txn, &self.internal, Some((&h, None)))
            .map_err(|e| super::HashPrefixError::Txn(e.into()))?
        {
            let (e, i) = x.map_err(|e| super::HashPrefixError::Txn(e.into()))?;
            debug!("{:?} {:?}", e, i);
            if e < &h {
                continue;
            } else {
                let e: Hash = e.into();
                let b32 = e.to_base32();
                debug!("{:?}", b32);
                let (b32, _) = b32.split_at(s.len().min(b32.len()));
                if b32 != s {
                    break;
                } else if result.is_none() {
                    result = Some((e, *i))
                } else {
                    return Err(super::HashPrefixError::Ambiguous(s.to_string()));
                }
            }
        }
        if let Some(result) = result {
            Ok(result)
        } else {
            Err(super::HashPrefixError::NotFound(s.to_string()))
        }
    }

    fn hash_from_prefix_remote<'txn>(
        &'txn self,
        remote: &RemoteRef<Self>,
        s: &str,
    ) -> Result<Hash, super::HashPrefixError<Self::GraphError>> {
        let remote = remote.db.lock().unwrap();
        let h: SerializedHash = if let Some(h) = Hash::from_prefix(s) {
            (&h).into()
        } else {
            return Err(super::HashPrefixError::Parse(s.to_string()));
        };
        let mut result = None;
        debug!("h = {:?}", h);
        for x in btree::iter(&self.txn, &remote.rev, Some((&h, None)))
            .map_err(|e| super::HashPrefixError::Txn(e.into()))?
        {
            let (e, _) = x.map_err(|e| super::HashPrefixError::Txn(e.into()))?;
            debug!("{:?}", e);
            if e < &h {
                continue;
            } else {
                let e: Hash = e.into();
                let b32 = e.to_base32();
                debug!("{:?}", b32);
                let (b32, _) = b32.split_at(s.len().min(b32.len()));
                if b32 != s {
                    break;
                } else if result.is_none() {
                    result = Some(e)
                } else {
                    return Err(super::HashPrefixError::Ambiguous(s.to_string()));
                }
            }
        }
        if let Some(result) = result {
            Ok(result)
        } else {
            Err(super::HashPrefixError::NotFound(s.to_string()))
        }
    }

    fn load_channel(
        &self,
        name: &str,
    ) -> Result<Option<ChannelRef<Self>>, TxnErr<Self::GraphError>> {
        let name = SmallString::from_str(name);
        match self.open_channels.lock().unwrap().entry(name.clone()) {
            Entry::Vacant(v) => {
                if let Some(c) = unsafe { self.unsafe_load_channel(name)? } {
                    Ok(Some(
                        v.insert(ChannelRef {
                            r: Arc::new(RwLock::new(c)),
                        })
                        .clone(),
                    ))
                } else {
                    Ok(None)
                }
            }
            Entry::Occupied(occ) => Ok(Some(occ.get().clone())),
        }
    }

    fn load_remote(&self, name: &str) -> Result<Option<RemoteRef<Self>>, TxnErr<Self::GraphError>> {
        let name = SmallString::from_str(name);
        match self.open_remotes.lock().unwrap().entry(name.clone()) {
            Entry::Vacant(v) => match btree::get(&self.txn, &self.remotes, &name, None)? {
                Some((name_, remote)) if name.as_ref() == name_ => {
                    debug!("load_remote: {:?} {:?}", name_, remote);
                    let r = Remote {
                        remote: UDb::from_page(remote.0[0].into()),
                        rev: UDb::from_page(remote.0[1].into()),
                        states: UDb::from_page(remote.0[2].into()),
                    };
                    for x in btree::iter(&self.txn, &r.remote, None).unwrap() {
                        debug!("remote -> {:?}", x);
                    }
                    for x in btree::iter(&self.txn, &r.rev, None).unwrap() {
                        debug!("rev -> {:?}", x);
                    }
                    for x in btree::iter(&self.txn, &r.states, None).unwrap() {
                        debug!("states -> {:?}", x);
                    }

                    for x in self.iter_remote(&r.remote, 0).unwrap() {
                        debug!("ITER {:?}", x);
                    }

                    let r = RemoteRef {
                        db: Arc::new(Mutex::new(r)),
                        name: name.clone(),
                    };
                    Ok(Some(v.insert(r).clone()))
                }
                _ => return Ok(None),
            },
            Entry::Occupied(occ) => Ok(Some(occ.get().clone())),
        }
    }

    ///
    type Channels = UDb<SmallStr, T8>;
    type ChannelsCursor = ::sanakirja::btree::cursor::Cursor<SmallStr, T8, UP<SmallStr, T8>>;
    sanakirja_cursor!(channels, SmallStr, T8,);
    fn iter_channels<'txn>(
        &'txn self,
        start: &str,
    ) -> Result<ChannelIterator<'txn, Self>, TxnErr<Self::GraphError>> {
        let name = SmallString::from_str(start);
        let mut cursor = btree::cursor::Cursor::new(&self.txn, &self.channels)?;
        cursor.set(&self.txn, &name, None)?;
        Ok(ChannelIterator { cursor, txn: self })
    }

    type Remotes = UDb<SmallStr, T3>;
    type RemotesCursor = ::sanakirja::btree::cursor::Cursor<SmallStr, T3, UP<SmallStr, T3>>;
    sanakirja_cursor!(remotes, SmallStr, T3);
    fn iter_remotes<'txn>(
        &'txn self,
        start: &str,
    ) -> Result<RemotesIterator<'txn, Self>, TxnErr<Self::GraphError>> {
        let name = SmallString::from_str(start);
        let mut cursor = btree::cursor::Cursor::new(&self.txn, &self.remotes)?;
        cursor.set(&self.txn, &name, None)?;
        Ok(RemotesIterator { cursor, txn: self })
    }

    type Remote = UDb<L64, Pair<SerializedHash, SerializedMerkle>>;
    type Revremote = UDb<SerializedHash, L64>;
    type Remotestates = UDb<SerializedMerkle, L64>;
    type RemoteCursor = ::sanakirja::btree::cursor::Cursor<
        L64,
        Pair<SerializedHash, SerializedMerkle>,
        UP<L64, Pair<SerializedHash, SerializedMerkle>>,
    >;
    sanakirja_cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);
    sanakirja_rev_cursor!(remote, L64, Pair<SerializedHash, SerializedMerkle>);

    fn iter_remote<'txn>(
        &'txn self,
        remote: &Self::Remote,
        k: u64,
    ) -> Result<
        super::Cursor<
            Self,
            &'txn Self,
            Self::RemoteCursor,
            L64,
            Pair<SerializedHash, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        self.cursor_remote(remote, Some((&k.into(), None)))
    }

    fn iter_rev_remote<'txn>(
        &'txn self,
        remote: &Self::Remote,
        k: Option<L64>,
    ) -> Result<
        super::RevCursor<
            Self,
            &'txn Self,
            Self::RemoteCursor,
            L64,
            Pair<SerializedHash, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        self.rev_cursor_remote(remote, k.as_ref().map(|k| (k, None)))
    }

    fn get_remote(
        &mut self,
        name: &str,
    ) -> Result<Option<RemoteRef<Self>>, TxnErr<Self::GraphError>> {
        let name = SmallString::from_str(name);
        match self.open_remotes.lock().unwrap().entry(name.clone()) {
            Entry::Vacant(v) => match btree::get(&self.txn, &self.remotes, &name, None)? {
                Some((name_, remote)) if name_ == name.as_ref() => {
                    let r = RemoteRef {
                        db: Arc::new(Mutex::new(Remote {
                            remote: UDb::from_page(remote.0[0].into()),
                            rev: UDb::from_page(remote.0[1].into()),
                            states: UDb::from_page(remote.0[2].into()),
                        })),
                        name: name.clone(),
                    };
                    v.insert(r);
                }
                _ => return Ok(None),
            },
            Entry::Occupied(_) => {}
        }
        Ok(self.open_remotes.lock().unwrap().get(&name).cloned())
    }

    fn last_remote(
        &self,
        remote: &Self::Remote,
    ) -> Result<Option<(u64, &Pair<SerializedHash, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        if let Some(x) = btree::rev_iter(&self.txn, remote, None)?.next() {
            let (&k, v) = x?;
            Ok(Some((k.into(), v)))
        } else {
            Ok(None)
        }
    }

    fn get_remote_state(
        &self,
        remote: &Self::Remote,
        n: u64,
    ) -> Result<Option<(u64, &Pair<SerializedHash, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        let n = n.into();
        for x in btree::iter(&self.txn, remote, Some((&n, None)))? {
            let (&k, m) = x?;
            if k >= n {
                return Ok(Some((k.into(), m)));
            }
        }
        Ok(None)
    }

    fn remote_has_change(
        &self,
        remote: &RemoteRef<Self>,
        hash: &SerializedHash,
    ) -> Result<bool, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, &remote.db.lock().unwrap().rev, hash, None)? {
            Some((k, _)) if k == hash => Ok(true),
            _ => Ok(false),
        }
    }
    fn remote_has_state(
        &self,
        remote: &RemoteRef<Self>,
        m: &SerializedMerkle,
    ) -> Result<bool, TxnErr<Self::GraphError>> {
        match btree::get(&self.txn, &remote.db.lock().unwrap().states, m, None)? {
            Some((k, _)) if k == m => Ok(true),
            _ => Ok(false),
        }
    }
}

impl GraphMutTxnT for MutTxn<()> {
    fn put_graph(
        &mut self,
        graph: &mut Self::Graph,
        k: &Vertex<ChangeId>,
        e: &SerializedEdge,
    ) -> Result<bool, TxnErr<Self::GraphError>> {
        Ok(btree::put(&mut self.txn, graph, k, e)?)
    }

    fn del_graph(
        &mut self,
        graph: &mut Self::Graph,
        k: &Vertex<ChangeId>,
        e: Option<&SerializedEdge>,
    ) -> Result<bool, TxnErr<Self::GraphError>> {
        Ok(btree::del(&mut self.txn, graph, k, e)?)
    }

    fn debug(&mut self, graph: &mut Self::Graph, extra: &str) {
        ::sanakirja::debug::debug(
            &self.txn,
            &[graph],
            format!("debug{}{}", self.counter, extra),
            true,
        );
    }

    sanakirja_put_del!(internal, SerializedHash, ChangeId, GraphError);
    sanakirja_put_del!(external, ChangeId, SerializedHash, GraphError);

    fn split_block(
        &mut self,
        graph: &mut Self::Graph,
        key: &Vertex<ChangeId>,
        pos: ChangePosition,
        buf: &mut Vec<SerializedEdge>,
    ) -> Result<(), TxnErr<Self::GraphError>> {
        assert!(pos > key.start);
        assert!(pos < key.end);
        let mut cursor = btree::cursor::Cursor::new(&self.txn, graph)?;
        cursor.set(&self.txn, key, None)?;
        loop {
            match cursor.next(&self.txn) {
                Ok(Some((k, v))) => {
                    if k > key {
                        break;
                    } else if k < key {
                        continue;
                    }
                    buf.push(*v)
                }
                Ok(None) => break,
                Err(e) => {
                    error!("{:?}", e);
                    return Err(TxnErr(SanakirjaError::PristineCorrupt));
                }
            }
        }
        for chi in buf.drain(..) {
            assert!(
                chi.introduced_by() != ChangeId::ROOT || chi.flag().contains(EdgeFlags::PSEUDO)
            );
            if chi.flag().contains(EdgeFlags::PARENT | EdgeFlags::BLOCK) {
                put_graph_with_rev(
                    self,
                    graph,
                    chi.flag() - EdgeFlags::PARENT,
                    Vertex {
                        change: key.change,
                        start: key.start,
                        end: pos,
                    },
                    Vertex {
                        change: key.change,
                        start: pos,
                        end: key.end,
                    },
                    chi.introduced_by(),
                )?;
            }

            self.del_graph(graph, key, Some(&chi))?;
            self.put_graph(
                graph,
                &if chi.flag().contains(EdgeFlags::PARENT) {
                    Vertex {
                        change: key.change,
                        start: key.start,
                        end: pos,
                    }
                } else {
                    Vertex {
                        change: key.change,
                        start: pos,
                        end: key.end,
                    }
                },
                &chi,
            )?;
        }
        Ok(())
    }
}

impl ChannelMutTxnT for MutTxn<()> {
    fn graph_mut(c: &mut Self::Channel) -> &mut Self::Graph {
        &mut c.graph
    }
    fn touch_channel(&mut self, channel: &mut Self::Channel, t: Option<u64>) {
        use std::time::SystemTime;
        debug!("touch_channel: {:?}", t);
        if let Some(t) = t {
            channel.last_modified = t.into()
        } else if let Ok(duration) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            channel.last_modified = duration.as_secs().into()
        }
    }

    fn put_changes(
        &mut self,
        channel: &mut Self::Channel,
        p: ChangeId,
        t: ApplyTimestamp,
        h: &Hash,
    ) -> Result<Option<Merkle>, TxnErr<Self::GraphError>> {
        debug!("put_changes {:?} {:?}", p, h);
        if let Some(m) = self.get_changeset(&channel.changes, &p)? {
            debug!("found m = {:?}, p = {:?}", m, p);
            Ok(None)
        } else {
            channel.apply_counter += 1;
            debug!("put_changes {:?} {:?}", t, p);
            let m = if let Some(x) = btree::rev_iter(&self.txn, &channel.revchanges, None)?.next() {
                (&(x?.1).b).into()
            } else {
                Merkle::zero()
            };
            let m = m.next(h);
            assert!(self
                .get_revchangeset(&channel.revchanges, &t.into())?
                .is_none());
            assert!(btree::put(
                &mut self.txn,
                &mut channel.changes,
                &p,
                &t.into()
            )?);
            assert!(btree::put(
                &mut self.txn,
                &mut channel.revchanges,
                &t.into(),
                &Pair { a: p, b: m.into() }
            )?);
            Ok(Some(m.into()))
        }
    }

    fn del_changes(
        &mut self,
        channel: &mut Self::Channel,
        p: ChangeId,
        t: ApplyTimestamp,
    ) -> Result<bool, TxnErr<Self::GraphError>> {
        let mut repl = Vec::new();
        let tl = t.into();
        for x in btree::iter(&self.txn, &channel.revchanges, Some((&tl, None)))? {
            let (t_, p) = x?;
            if *t_ >= tl {
                repl.push((*t_, p.a))
            }
        }
        let mut m = Merkle::zero();
        for x in btree::rev_iter(&self.txn, &channel.revchanges, Some((&tl, None)))? {
            let (t_, mm) = x?;
            if t_ < &tl {
                m = (&mm.b).into();
                break;
            }
        }
        for (t_, p) in repl.iter() {
            debug!("del_changes {:?} {:?}", t_, p);
            btree::del(&mut self.txn, &mut channel.revchanges, t_, None)?;
            if *t_ > tl {
                m = m.next(&self.get_external(p)?.unwrap().into());
                btree::put(
                    &mut self.txn,
                    &mut channel.revchanges,
                    t_,
                    &Pair { a: *p, b: m.into() },
                )?;
            }
        }
        btree::del(&mut self.txn, &mut channel.tags, &t.into(), None)?;
        Ok(btree::del(
            &mut self.txn,
            &mut channel.changes,
            &p,
            Some(&t.into()),
        )?)
    }

    fn put_tags(
        &mut self,
        channel: &mut Self::Channel,
        t: ApplyTimestamp,
        h: &Hash,
    ) -> Result<(), TxnErr<Self::GraphError>> {
        btree::put(&mut self.txn, &mut channel.tags, &t.into(), &h.into())?;
        Ok(())
    }

    fn del_tags(
        &mut self,
        channel: &mut Self::Channel,
        t: ApplyTimestamp,
    ) -> Result<(), TxnErr<Self::GraphError>> {
        btree::del(&mut self.txn, &mut channel.tags, &t.into(), None)?;
        Ok(())
    }
}

impl DepsMutTxnT for MutTxn<()> {
    sanakirja_put_del!(dep, ChangeId, ChangeId, DepsError);
    sanakirja_put_del!(revdep, ChangeId, ChangeId, DepsError);
    sanakirja_put_del!(touched_files, Position<ChangeId>, ChangeId, DepsError);
    sanakirja_put_del!(rev_touched_files, ChangeId, Position<ChangeId>, DepsError);
}

impl TreeMutTxnT for MutTxn<()> {
    sanakirja_put_del!(inodes, Inode, Position<ChangeId>, TreeError);
    sanakirja_put_del!(revinodes, Position<ChangeId>, Inode, TreeError);

    sanakirja_put_del!(tree, PathId, Inode, TreeError,);
    sanakirja_put_del!(revtree, Inode, PathId, TreeError,);

    fn put_partials(
        &mut self,
        k: &str,
        e: Position<ChangeId>,
    ) -> Result<bool, TxnErr<Self::TreeError>> {
        let k = SmallString::from_str(k);
        Ok(btree::put(&mut self.txn, &mut self.partials, &k, &e)?)
    }

    fn del_partials(
        &mut self,
        k: &str,
        e: Option<Position<ChangeId>>,
    ) -> Result<bool, TxnErr<Self::TreeError>> {
        let k = SmallString::from_str(k);
        Ok(btree::del(
            &mut self.txn,
            &mut self.partials,
            &k,
            e.as_ref(),
        )?)
    }
}

impl MutTxnT for MutTxn<()> {
    fn put_remote(
        &mut self,
        remote: &mut RemoteRef<Self>,
        k: u64,
        v: (Hash, Merkle),
    ) -> Result<bool, Self::GraphError> {
        let mut remote = remote.db.lock().unwrap();
        let h = (&v.0).into();
        let m: SerializedMerkle = (&v.1).into();
        btree::put(
            &mut self.txn,
            &mut remote.remote,
            &k.into(),
            &Pair { a: h, b: m.clone() },
        )?;
        btree::put(&mut self.txn, &mut remote.states, &m, &k.into())?;
        Ok(btree::put(&mut self.txn, &mut remote.rev, &h, &k.into())?)
    }

    fn del_remote(
        &mut self,
        remote: &mut RemoteRef<Self>,
        k: u64,
    ) -> Result<bool, Self::GraphError> {
        let mut remote = remote.db.lock().unwrap();
        let k = k.into();
        match btree::get(&self.txn, &remote.remote, &k, None)? {
            Some((k0, p)) if k0 == &k => {
                debug!("del_remote {:?} {:?}", k0, p);
                let p = p.clone();
                btree::del(&mut self.txn, &mut remote.rev, &p.a, None)?;
                btree::del(&mut self.txn, &mut remote.states, &p.b, None)?;
                Ok(btree::del(
                    &mut self.txn,
                    &mut remote.remote,
                    &k.into(),
                    None,
                )?)
            }
            x => {
                debug!("not found, {:?}", x);
                Ok(false)
            }
        }
    }

    fn open_or_create_channel(&mut self, name: &str) -> Result<ChannelRef<Self>, Self::GraphError> {
        let name = crate::small_string::SmallString::from_str(name);
        let mut commit = None;
        let result = match self.open_channels.lock().unwrap().entry(name.clone()) {
            Entry::Vacant(v) => {
                let r = match btree::get(&self.txn, &self.channels, &name, None)? {
                    Some((name_, b)) if name_ == name.as_ref() => ChannelRef {
                        r: Arc::new(RwLock::new(Channel {
                            graph: Db::from_page(b.0[0].into()),
                            changes: Db::from_page(b.0[1].into()),
                            revchanges: UDb::from_page(b.0[2].into()),
                            states: UDb::from_page(b.0[3].into()),
                            tags: UDb::from_page(b.0[4].into()),
                            apply_counter: b.0[5].into(),
                            last_modified: b.0[6].into(),
                            name: name.clone(),
                        })),
                    },
                    _ => {
                        let br = ChannelRef {
                            r: Arc::new(RwLock::new(Channel {
                                graph: btree::create_db_(&mut self.txn)?,
                                changes: btree::create_db_(&mut self.txn)?,
                                revchanges: btree::create_db_(&mut self.txn)?,
                                states: btree::create_db_(&mut self.txn)?,
                                tags: btree::create_db_(&mut self.txn)?,
                                apply_counter: 0,
                                last_modified: 0,
                                name: name.clone(),
                            })),
                        };
                        commit = Some(br.clone());
                        br
                    }
                };
                v.insert(r).clone()
            }
            Entry::Occupied(occ) => occ.get().clone(),
        };
        if let Some(commit) = commit {
            self.put_channel(commit)?;
        }
        Ok(result)
    }

    fn fork(
        &mut self,
        channel: &ChannelRef<Self>,
        new_name: &str,
    ) -> Result<ChannelRef<Self>, ForkError<Self::GraphError>> {
        let channel = channel.r.read().unwrap();
        let name = SmallString::from_str(new_name);
        match btree::get(&self.txn, &self.channels, &name, None)
            .map_err(|e| ForkError::Txn(e.into()))?
        {
            Some((name_, _)) if name_ == name.as_ref() => {
                Err(super::ForkError::ChannelNameExists(new_name.to_string()))
            }
            _ => {
                let br = ChannelRef {
                    r: Arc::new(RwLock::new(Channel {
                        graph: btree::fork_db(&mut self.txn, &channel.graph)
                            .map_err(|e| ForkError::Txn(e.into()))?,
                        changes: btree::fork_db(&mut self.txn, &channel.changes)
                            .map_err(|e| ForkError::Txn(e.into()))?,
                        revchanges: btree::fork_db(&mut self.txn, &channel.revchanges)
                            .map_err(|e| ForkError::Txn(e.into()))?,
                        states: btree::fork_db(&mut self.txn, &channel.states)
                            .map_err(|e| ForkError::Txn(e.into()))?,
                        tags: btree::fork_db(&mut self.txn, &channel.tags)
                            .map_err(|e| ForkError::Txn(e.into()))?,
                        name: name.clone(),
                        apply_counter: channel.apply_counter,
                        last_modified: channel.last_modified,
                    })),
                };
                self.open_channels.lock().unwrap().insert(name, br.clone());
                Ok(br)
            }
        }
    }

    fn rename_channel(
        &mut self,
        channel: &mut ChannelRef<Self>,
        new_name: &str,
    ) -> Result<(), ForkError<Self::GraphError>> {
        let name = SmallString::from_str(new_name);
        match btree::get(&self.txn, &self.channels, &name, None)
            .map_err(|e| ForkError::Txn(e.into()))?
        {
            Some((name_, _)) if name_ == name.as_ref() => {
                Err(super::ForkError::ChannelNameExists(new_name.to_string()))
            }
            _ => {
                btree::del(
                    &mut self.txn,
                    &mut self.channels,
                    &channel.r.read().unwrap().name,
                    None,
                )
                .map_err(|e| ForkError::Txn(e.into()))?;
                std::mem::drop(
                    self.open_channels
                        .lock()
                        .unwrap()
                        .remove(&channel.r.read().unwrap().name)
                        .unwrap(),
                );
                channel.r.write().unwrap().name = name.clone();
                self.open_channels
                    .lock()
                    .unwrap()
                    .insert(name, channel.clone());
                Ok(())
            }
        }
    }

    fn drop_channel(&mut self, name0: &str) -> Result<bool, Self::GraphError> {
        let name = SmallString::from_str(name0);
        let channel = if let Some(channel) = self.open_channels.lock().unwrap().remove(&name) {
            let channel = Arc::try_unwrap(channel.r)
                .map_err(|_| SanakirjaError::ChannelRc {
                    c: name0.to_string(),
                })?
                .into_inner()
                .unwrap();
            Some((
                channel.graph,
                channel.changes,
                channel.revchanges,
                channel.states,
                channel.tags,
            ))
        } else if let Some((name_, chan)) = btree::get(&self.txn, &self.channels, &name, None)? {
            if name_ == name.as_ref() {
                Some((
                    Db::from_page(chan.0[0].into()),
                    Db::from_page(chan.0[1].into()),
                    UDb::from_page(chan.0[2].into()),
                    UDb::from_page(chan.0[3].into()),
                    UDb::from_page(chan.0[4].into()),
                ))
            } else {
                None
            }
        } else {
            None
        };
        btree::del(&mut self.txn, &mut self.channels, &name, None)?;
        if let Some((a, b, c, d, e)) = channel {
            let mut unused_changes = Vec::new();
            'outer: for x in btree::rev_iter(&self.txn, &c, None)? {
                let (_, p) = x?;
                for chan in self.iter_channels("").map_err(|e| e.0)? {
                    let (name, chan) = chan.map_err(|e| e.0)?;
                    assert_ne!(name.as_str(), name0);
                    let chan = chan.read().unwrap();
                    if self.channel_has_state(&chan.states, &p.b).map_err(|e| e.0)?.is_some() {
                        break 'outer
                    }
                    if self.get_changeset(&chan.changes, &p.a).map_err(|e| e.0)?.is_some() {
                        continue 'outer
                    }
                }
                unused_changes.push(p.a);
            }
            let mut deps = Vec::new();
            for ch in unused_changes.iter() {
                for x in btree::iter(&self.txn, &self.dep, Some((ch, None)))? {
                    let (k, v) = x?;
                    if k > ch {
                        break
                    }
                    deps.push((*k, *v));
                }
                for (k, v) in deps.drain(..) {
                    btree::del(&mut self.txn, &mut self.revdep, &k, Some(&v))?;
                    btree::del(&mut self.txn, &mut self.revdep, &v, Some(&k))?;
                }
            }
            btree::drop(&mut self.txn, a)?;
            btree::drop(&mut self.txn, b)?;
            btree::drop(&mut self.txn, c)?;
            btree::drop(&mut self.txn, d)?;
            btree::drop(&mut self.txn, e)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn open_or_create_remote(&mut self, name: &str) -> Result<RemoteRef<Self>, Self::GraphError> {
        let name = crate::small_string::SmallString::from_str(name);
        let mut commit = None;
        match self.open_remotes.lock().unwrap().entry(name.clone()) {
            Entry::Vacant(v) => {
                let r = match btree::get(&self.txn, &self.remotes, &name, None)? {
                    Some((name_, remote)) if name_ == name.as_ref() => RemoteRef {
                        db: Arc::new(Mutex::new(Remote {
                            remote: UDb::from_page(remote.0[0].into()),
                            rev: UDb::from_page(remote.0[1].into()),
                            states: UDb::from_page(remote.0[2].into()),
                        })),
                        name: name.clone(),
                    },
                    _ => {
                        let br = RemoteRef {
                            db: Arc::new(Mutex::new(Remote {
                                remote: btree::create_db_(&mut self.txn)?,
                                rev: btree::create_db_(&mut self.txn)?,
                                states: btree::create_db_(&mut self.txn)?,
                            })),
                            name: name.clone(),
                        };
                        commit = Some(br.clone());
                        br
                    }
                };
                v.insert(r);
            }
            Entry::Occupied(_) => {}
        }
        if let Some(commit) = commit {
            self.put_remotes(commit)?;
        }
        Ok(self
            .open_remotes
            .lock()
            .unwrap()
            .get(&name)
            .unwrap()
            .clone())
    }

    fn drop_remote(&mut self, remote: RemoteRef<Self>) -> Result<bool, Self::GraphError> {
        let name = remote.name.clone();
        let r = self.open_remotes.lock().unwrap().remove(&name).unwrap();
        std::mem::drop(remote);
        assert_eq!(Arc::strong_count(&r.db), 1);
        Ok(btree::del(&mut self.txn, &mut self.remotes, &name, None)?)
    }

    fn drop_named_remote(&mut self, name: &str) -> Result<bool, Self::GraphError> {
        let name = SmallString::from_str(name);
        if let Some(r) = self.open_remotes.lock().unwrap().remove(&name) {
            assert_eq!(Arc::strong_count(&r.db), 1);
        }
        Ok(btree::del(&mut self.txn, &mut self.remotes, &name, None)?)
    }

    fn commit(mut self) -> Result<(), Self::GraphError> {
        use std::ops::DerefMut;
        {
            let open_channels = std::mem::replace(
                self.open_channels.lock().unwrap().deref_mut(),
                HashMap::default(),
            );
            for (name, channel) in open_channels {
                debug!("commit_channel {:?}", name);
                self.commit_channel(channel)?
            }
        }
        {
            let open_remotes = std::mem::replace(
                self.open_remotes.lock().unwrap().deref_mut(),
                HashMap::default(),
            );
            for (name, remote) in open_remotes {
                debug!("commit remote {:?}", name);
                self.commit_remote(remote)?
            }
        }
        // No need to set `Root::Version`, it is set at init.
        self.txn.set_root(Root::Tree as usize, self.tree.db);
        self.txn.set_root(Root::RevTree as usize, self.revtree.db);
        self.txn.set_root(Root::Inodes as usize, self.inodes.db);
        self.txn
            .set_root(Root::RevInodes as usize, self.revinodes.db);
        self.txn.set_root(Root::Internal as usize, self.internal.db);
        self.txn.set_root(Root::External as usize, self.external.db);
        self.txn.set_root(Root::RevDep as usize, self.revdep.db);
        self.txn.set_root(Root::Channels as usize, self.channels.db);
        self.txn.set_root(Root::Remotes as usize, self.remotes.db);
        self.txn
            .set_root(Root::TouchedFiles as usize, self.touched_files.db);
        self.txn.set_root(Root::Dep as usize, self.dep.db);
        self.txn
            .set_root(Root::RevTouchedFiles as usize, self.rev_touched_files.db);
        self.txn.set_root(Root::Partials as usize, self.partials.db);

        self.txn.commit()?;
        Ok(())
    }
}

impl Txn {
    pub fn load_const_channel(&self, name: &str) -> Result<Option<Channel>, SanakirjaError> {
        let name = SmallString::from_str(name);
        match btree::get(&self.txn, &self.channels, &name, None)? {
            Some((name_, c)) if name.as_ref() == name_ => {
                debug!("load_const_channel = {:?} {:?}", name_, c);
                Ok(Some(Channel {
                    graph: Db::from_page(c.0[0].into()),
                    changes: Db::from_page(c.0[1].into()),
                    revchanges: UDb::from_page(c.0[2].into()),
                    states: UDb::from_page(c.0[3].into()),
                    tags: UDb::from_page(c.0[4].into()),
                    apply_counter: c.0[5].into(),
                    last_modified: c.0[6].into(),
                    name,
                }))
            }
            _ => Ok(None),
        }
    }
}

impl<T> MutTxn<T> {
    fn put_channel(&mut self, channel: ChannelRef<Self>) -> Result<(), SanakirjaError> {
        debug!("Commit_channel.");
        let channel = channel.r.read().unwrap();
        // Since we are replacing the value, we don't want to
        // decrement its reference counter (which del would do), hence
        // the transmute.
        //
        // This would normally be wrong. The only reason it works is
        // because we know that dbs_channels has never been forked
        // from another database, hence all the reference counts to
        // its elements are 1 (and therefore represented as "not
        // referenced" in Sanakirja).
        debug!("Commit_channel, dbs_channels = {:?}", self.channels);
        btree::del(&mut self.txn, &mut self.channels, &channel.name, None)?;
        let t8 = T8([
            channel.graph.db.into(),
            channel.changes.db.into(),
            channel.revchanges.db.into(),
            channel.states.db.into(),
            channel.tags.db.into(),
            channel.apply_counter.into(),
            channel.last_modified.into(),
            0u64.into(),
        ]);
        btree::put(&mut self.txn, &mut self.channels, &channel.name, &t8)?;
        debug!("Commit_channel, self.channels = {:?}", self.channels);
        Ok(())
    }

    fn commit_channel(&mut self, channel: ChannelRef<Self>) -> Result<(), SanakirjaError> {
        std::mem::drop(
            self.open_channels
                .lock()
                .unwrap()
                .remove(&channel.r.read().unwrap().name),
        );
        self.put_channel(channel)
    }

    fn put_remotes(&mut self, remote: RemoteRef<Self>) -> Result<(), SanakirjaError> {
        btree::del(&mut self.txn, &mut self.remotes, &remote.name, None)?;
        debug!("Commit_remote, dbs_remotes = {:?}", self.remotes);
        let r = remote.db.lock().unwrap();
        btree::put(
            &mut self.txn,
            &mut self.remotes,
            &remote.name,
            &T3([r.remote.db.into(), r.rev.db.into(), r.states.db.into()]),
        )?;
        debug!("Commit_remote, self.dbs.remotes = {:?}", self.remotes);
        Ok(())
    }

    fn commit_remote(&mut self, remote: RemoteRef<Self>) -> Result<(), SanakirjaError> {
        std::mem::drop(self.open_remotes.lock().unwrap().remove(&remote.name));
        // assert_eq!(Rc::strong_count(&remote.db), 1);
        self.put_remotes(remote)
    }
}

direct_repr!(L64);

direct_repr!(ChangeId);

direct_repr!(Vertex<ChangeId>);
direct_repr!(Position<ChangeId>);

direct_repr!(SerializedEdge);

impl Storable for PathId {
    fn compare<T>(&self, _: &T, x: &Self) -> std::cmp::Ordering {
        self.cmp(x)
    }
    type PageReferences = std::iter::Empty<u64>;
    fn page_references(&self) -> Self::PageReferences {
        std::iter::empty()
    }
}
impl UnsizedStorable for PathId {
    const ALIGN: usize = 8;
    fn size(&self) -> usize {
        9 + self.basename.len()
    }
    unsafe fn onpage_size(p: *const u8) -> usize {
        let len = *(p.add(8)) as usize;
        9 + len
    }
    unsafe fn from_raw_ptr<'a, T>(_: &T, p: *const u8) -> &'a Self {
        path_id_from_raw_ptr(p)
    }
    unsafe fn write_to_page(&self, p: *mut u8) {
        *(p as *mut u64) = (self.parent_inode.0).0;
        self.basename.write_to_page(p.add(8))
    }
}

unsafe fn path_id_from_raw_ptr<'a>(p: *const u8) -> &'a PathId {
    let len = *(p.add(8)) as usize;
    std::mem::transmute(std::slice::from_raw_parts(p, 1 + len as usize))
}

#[test]
fn pathid_repr() {
    let o = OwnedPathId {
        parent_inode: Inode::ROOT,
        basename: SmallString::from_str("blablabla"),
    };
    let mut x = vec![0u8; 200];

    unsafe {
        o.write_to_page(x.as_mut_ptr());
        let p = path_id_from_raw_ptr(x.as_ptr());
        assert_eq!(p.basename.as_str(), "blablabla");
        assert_eq!(p.parent_inode, Inode::ROOT);
    }
}

direct_repr!(Inode);
direct_repr!(SerializedMerkle);
direct_repr!(SerializedHash);

impl<A: Storable, B: Storable> Storable for Pair<A, B> {
    type PageReferences = core::iter::Chain<A::PageReferences, B::PageReferences>;
    fn page_references(&self) -> Self::PageReferences {
        self.a.page_references().chain(self.b.page_references())
    }
    fn compare<T: LoadPage>(&self, t: &T, b: &Self) -> core::cmp::Ordering {
        match self.a.compare(t, &b.a) {
            core::cmp::Ordering::Equal => self.b.compare(t, &b.b),
            ord => ord,
        }
    }
}

impl<A: Ord + UnsizedStorable, B: Ord + UnsizedStorable> UnsizedStorable for Pair<A, B> {
    const ALIGN: usize = std::mem::align_of::<(A, B)>();

    fn size(&self) -> usize {
        let a = self.a.size();
        let b_off = (a + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        (b_off + self.b.size() + (Self::ALIGN - 1)) & !(Self::ALIGN - 1)
    }
    unsafe fn onpage_size(p: *const u8) -> usize {
        let a = A::onpage_size(p);
        let b_off = (a + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        let b_size = B::onpage_size(p.add(b_off));
        (b_off + b_size + (Self::ALIGN - 1)) & !(Self::ALIGN - 1)
    }
    unsafe fn from_raw_ptr<'a, T>(_: &T, p: *const u8) -> &'a Self {
        &*(p as *const Self)
    }
    unsafe fn write_to_page(&self, p: *mut u8) {
        self.a.write_to_page(p);
        let off = (self.a.size() + (B::ALIGN - 1)) & !(B::ALIGN - 1);
        self.b.write_to_page(p.add(off));
    }
}

direct_repr!(T3);
direct_repr!(T8);
