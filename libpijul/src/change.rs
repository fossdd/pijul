use crate::HashSet;
use std::collections::BTreeSet;

use crate::pristine::*;
use crate::text_encoding::Encoding;
use chrono::{DateTime, Utc};

#[cfg(feature = "zstd")]
use std::io::Write;

#[cfg(feature = "text-changes")]
mod text_changes;
pub use text_changes::{TextDeError, TextSerError, WriteChangeLine};

mod change_file;
pub use change_file::*;

mod noenc;

#[derive(Debug, Error)]
pub enum ChangeError {
    #[error("Version mismatch: got {}", got)]
    VersionMismatch { got: u64 },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Zstd(#[from] zstd_seekable::Error),
    #[error(transparent)]
    TomlDe(#[from] toml::de::Error),
    #[error(transparent)]
    TomlSer(#[from] toml::ser::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("Missing contents for change {:?}", hash)]
    MissingContents { hash: crate::pristine::Hash },
    #[error("Change hash mismatch, claimed {:?}, computed {:?}", claimed, computed)]
    ChangeHashMismatch {
        claimed: crate::pristine::Hash,
        computed: crate::pristine::Hash,
    },
    #[error(
        "Change contents hash mismatch, claimed {:?}, computed {:?}",
        claimed,
        computed
    )]
    ContentsHashMismatch {
        claimed: crate::pristine::Hash,
        computed: crate::pristine::Hash,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Atom<Change> {
    NewVertex(NewVertex<Change>),
    EdgeMap(EdgeMap<Change>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewVertex<Change> {
    pub up_context: Vec<Position<Change>>,
    pub down_context: Vec<Position<Change>>,
    pub flag: EdgeFlags,
    pub start: ChangePosition,
    pub end: ChangePosition,
    pub inode: Position<Change>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgeMap<Change> {
    pub edges: Vec<NewEdge<Change>>,
    pub inode: Position<Change>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewEdge<Change> {
    pub previous: EdgeFlags,
    pub flag: EdgeFlags,
    /// The origin of the edge, i.e. if a vertex split is needed, the
    /// left-hand side of the split will include `from.pos`. This
    /// means that splitting vertex `[a, b[` to apply this edge
    /// modification will yield vertices `[a, from.pos+1[` and
    /// `[from.pos+1, b[`.
    pub from: Position<Change>,
    /// The destination of the edge, i.e. the last byte affected by
    /// this change.
    pub to: Vertex<Change>,
    /// The change that introduced the previous version of the edge
    /// (the one being replaced by this `NewEdge`).
    pub introduced_by: Change,
}

impl<T: Clone> NewEdge<T> {
    pub(crate) fn reverse(&self, introduced_by: T) -> Self {
        NewEdge {
            previous: self.flag,
            flag: self.previous,
            from: self.from.clone(),
            to: self.to.clone(),
            introduced_by,
        }
    }
}

/// The header of a change contains all the metadata about a change
/// (but not the actual contents of a change).
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ChangeHeader_<Author> {
    pub message: String,
    pub description: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub authors: Vec<Author>,
}

/// The header of a change contains all the metadata about a change
/// (but not the actual contents of a change).
pub type ChangeHeader = ChangeHeader_<Author>;

impl Default for ChangeHeader {
    fn default() -> Self {
        ChangeHeader {
            message: String::new(),
            description: None,
            timestamp: Utc::now(),
            authors: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LocalChange<Hunk, Author> {
    pub offsets: Offsets,
    pub hashed: Hashed<Hunk, Author>,
    /// unhashed TOML extra contents.
    pub unhashed: Option<serde_json::Value>,
    /// The contents.
    pub contents: Vec<u8>,
}

impl std::ops::Deref for LocalChange<Hunk<Option<Hash>, Local>, Author> {
    type Target = Hashed<Hunk<Option<Hash>, Local>, Author>;
    fn deref(&self) -> &Self::Target {
        &self.hashed
    }
}

impl std::ops::DerefMut for LocalChange<Hunk<Option<Hash>, Local>, Author> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.hashed
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Author(pub std::collections::BTreeMap<String, String>);

// Beware of changes in the version, tags also use that.
pub const VERSION: u64 = 6;
pub const VERSION_NOENC: u64 = 4;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Hashed<Hunk, Author> {
    /// Version, again (in order to hash it).
    pub version: u64,
    /// Header part, containing the metadata.
    pub header: ChangeHeader_<Author>,
    /// The dependencies of this change.
    pub dependencies: Vec<Hash>,
    /// Extra known "context" changes to recover from deleted contexts.
    pub extra_known: Vec<Hash>,
    /// Some space to write application-specific data.
    pub metadata: Vec<u8>,
    /// The changes, without the contents.
    pub changes: Vec<Hunk>,
    /// Hash of the contents, so that the "contents" field is
    /// verifiable independently from the actions in this change.
    pub contents_hash: Hash,
}

pub type Change = LocalChange<Hunk<Option<Hash>, Local>, Author>;

pub fn dependencies<
    'a,
    Local: 'a,
    I: Iterator<Item = &'a Hunk<Option<Hash>, Local>>,
    T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
>(
    txn: &T,
    channel: &T::Channel,
    changes: I,
) -> Result<(Vec<Hash>, Vec<Hash>), TxnErr<T::DepsError>> {
    let mut deps = BTreeSet::new();
    let mut zombie_deps = BTreeSet::new();
    for ch in changes.flat_map(|r| r.iter()) {
        match *ch {
            Atom::NewVertex(NewVertex {
                ref up_context,
                ref down_context,
                ..
            }) => {
                for up in up_context.iter().chain(down_context.iter()) {
                    match up.change {
                        None | Some(Hash::None) => {}
                        Some(ref dep) => {
                            deps.insert(*dep);
                        }
                    }
                }
            }
            Atom::EdgeMap(EdgeMap { ref edges, .. }) => {
                for e in edges {
                    assert!(!e.flag.contains(EdgeFlags::PARENT));
                    assert!(e.introduced_by != Some(Hash::None));
                    if let Some(p) = e.from.change {
                        deps.insert(p);
                    }
                    if let Some(p) = e.introduced_by {
                        deps.insert(p);
                    }
                    if let Some(p) = e.to.change {
                        deps.insert(p);
                    }
                    add_zombie_deps_from(txn, txn.graph(channel), &mut zombie_deps, e.from)?;
                    add_zombie_deps_to(txn, txn.graph(channel), &mut zombie_deps, e.to)?
                }
            }
        }
    }
    let deps = minimize_deps(txn, &channel, &deps)?;
    for d in deps.iter() {
        zombie_deps.remove(d);
    }
    let mut deps: Vec<Hash> = deps.into_iter().collect();
    deps.sort_by(|a, b| {
        let a = txn.get_internal(&a.into()).unwrap().unwrap();
        let b = txn.get_internal(&b.into()).unwrap().unwrap();
        txn.get_changeset(txn.changes(&channel), a)
            .unwrap()
            .cmp(&txn.get_changeset(txn.changes(&channel), b).unwrap())
    });
    let mut zombie_deps: Vec<Hash> = zombie_deps.into_iter().collect();
    zombie_deps.sort_by(|a, b| {
        let a = txn.get_internal(&a.into()).unwrap().unwrap();
        let b = txn.get_internal(&b.into()).unwrap().unwrap();
        txn.get_changeset(txn.changes(&channel), a)
            .unwrap()
            .cmp(&txn.get_changeset(txn.changes(&channel), b).unwrap())
    });
    Ok((deps, zombie_deps))
}

pub fn full_dependencies<T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>>(
    txn: &T,
    channel: &ChannelRef<T>,
) -> Result<(Vec<Hash>, Vec<Hash>), TxnErr<T::DepsError>> {
    let mut deps = BTreeSet::new();
    let channel = channel.read();
    for x in changeid_log(txn, &channel, L64(0))? {
        let (_, p) = x?;
        let h = txn.get_external(&p.a)?.unwrap();
        deps.insert(h.into());
    }
    let deps = minimize_deps(txn, &channel, &deps)?;
    Ok((deps, Vec::new()))
}

fn add_zombie_deps_from<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    zombie_deps: &mut BTreeSet<Hash>,
    e_from: Position<Option<Hash>>,
) -> Result<(), TxnErr<T::GraphError>> {
    let e_from = if let Some(p) = e_from.change {
        Position {
            change: *txn.get_internal(&p.into())?.unwrap(),
            pos: e_from.pos,
        }
    } else {
        return Ok(());
    };
    let from = txn.find_block_end(channel, e_from).unwrap();
    for edge in iter_adj_all(txn, channel, *from)? {
        let edge = edge?;
        if let Some(ext) = txn.get_external(&edge.introduced_by())? {
            let ext: Hash = ext.into();
            if let Hash::None = ext {
            } else {
                zombie_deps.insert(ext);
            }
        }
        if let Some(ext) = txn.get_external(&edge.dest().change)? {
            let ext: Hash = ext.into();
            if let Hash::None = ext {
            } else {
                zombie_deps.insert(ext);
            }
        }
    }
    Ok(())
}

fn add_zombie_deps_to<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    zombie_deps: &mut BTreeSet<Hash>,
    e_to: Vertex<Option<Hash>>,
) -> Result<(), TxnErr<T::GraphError>> {
    let to_pos = if let Some(p) = e_to.change {
        Position {
            change: *txn.get_internal(&p.into())?.unwrap(),
            pos: e_to.start,
        }
    } else {
        return Ok(());
    };
    let mut to = txn.find_block(channel, to_pos).unwrap();
    loop {
        for edge in iter_adj_all(txn, channel, *to)? {
            let edge = edge?;
            if let Some(ext) = txn.get_external(&edge.introduced_by())? {
                let ext = ext.into();
                if let Hash::None = ext {
                } else {
                    zombie_deps.insert(ext);
                }
            }
            if let Some(ext) = txn.get_external(&edge.dest().change)? {
                let ext = ext.into();
                if let Hash::None = ext {
                } else {
                    zombie_deps.insert(ext);
                }
            }
        }
        if to.end >= e_to.end {
            break;
        }
        to = txn.find_block(channel, to.end_pos()).unwrap();
    }
    Ok(())
}

fn minimize_deps<T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>>(
    txn: &T,
    channel: &T::Channel,
    deps: &BTreeSet<Hash>,
) -> Result<Vec<Hash>, TxnErr<T::DepsError>> {
    let mut min_time = std::u64::MAX;
    let mut internal_deps = Vec::new();
    let mut internal_deps_ = HashSet::default();
    for h in deps.iter() {
        if let Hash::None = h {
            continue;
        }
        debug!("h = {:?}", h);
        let id = txn.get_internal(&h.into())?.unwrap();
        debug!("id = {:?}", id);
        let time = txn.get_changeset(txn.changes(&channel), id)?.unwrap();
        let time = u64::from_le(time.0);
        debug!("time = {:?}", time);
        min_time = min_time.min(time);
        internal_deps.push((id, true));
        internal_deps_.insert(id);
    }
    internal_deps.sort_by(|a, b| a.1.cmp(&b.1));
    let mut visited = HashSet::default();
    while let Some((id, is_root)) = internal_deps.pop() {
        if is_root {
            if !internal_deps_.contains(&id) {
                continue;
            }
        } else if internal_deps_.remove(&id) {
            debug!("removing dep {:?}", id);
        }
        if !visited.insert(id) {
            continue;
        }
        let mut cursor = txn.iter_dep(id)?;
        while let Some(x) = txn.cursor_dep_next(&mut cursor.cursor)? {
            let (id0, dep) = x;
            trace!("minimize loop = {:?} {:?}", id0, dep);
            if id0 < id {
                continue;
            } else if id0 > id {
                break;
            }
            let time = if let Some(time) = txn.get_changeset(txn.changes(&channel), dep)? {
                time
            } else {
                panic!(
                    "not found in channel {:?}: id = {:?} depends on {:?}",
                    txn.name(channel),
                    id,
                    dep
                );
            };
            let time = u64::from_le(time.0);
            trace!("time = {:?}", time);
            if time >= min_time {
                internal_deps.push((dep, false))
            }
        }
    }
    Ok(internal_deps_
        .into_iter()
        .map(|id| txn.get_external(id).unwrap().unwrap().into())
        .collect())
}

impl Change {
    pub fn knows(&self, hash: &Hash) -> bool {
        self.extra_known.contains(hash) || self.dependencies.contains(&hash)
    }

    pub fn has_edge(
        &self,
        hash: Hash,
        from: Position<Option<Hash>>,
        to: Position<Option<Hash>>,
        flags: crate::pristine::EdgeFlags,
    ) -> bool {
        debug!("has_edge: {:?} {:?} {:?} {:?}", hash, from, to, flags);
        for change_ in self.changes.iter() {
            for change_ in change_.iter() {
                match change_ {
                    Atom::NewVertex(n) => {
                        debug!("has_edge: {:?}", n);
                        if from.change == Some(hash) && from.pos >= n.start && from.pos <= n.end {
                            if to.change == Some(hash) {
                                // internal
                                return flags | EdgeFlags::FOLDER
                                    == EdgeFlags::BLOCK | EdgeFlags::FOLDER;
                            } else {
                                // down context
                                if n.down_context.iter().any(|d| *d == to) {
                                    return flags.is_empty();
                                } else {
                                    return false;
                                }
                            }
                        } else if to.change == Some(hash) && to.pos >= n.start && to.pos <= n.end {
                            // up context
                            if n.up_context.iter().any(|d| *d == from) {
                                return flags | EdgeFlags::FOLDER
                                    == EdgeFlags::BLOCK | EdgeFlags::FOLDER;
                            } else {
                                return false;
                            }
                        }
                    }
                    Atom::EdgeMap(e) => {
                        debug!("has_edge: {:?}", e);
                        if e.edges
                            .iter()
                            .any(|e| e.from == from && e.to.start_pos() == to && e.flag == flags)
                        {
                            return true;
                        }
                    }
                }
            }
        }
        debug!("not found");
        false
    }
}

impl<A> Atom<A> {
    pub fn as_newvertex(&self) -> &NewVertex<A> {
        if let Atom::NewVertex(n) = self {
            n
        } else {
            panic!("Not a NewVertex")
        }
    }
}

impl Atom<Option<Hash>> {
    pub fn inode(&self) -> Position<Option<Hash>> {
        match self {
            Atom::NewVertex(ref n) => n.inode,
            Atom::EdgeMap(ref n) => n.inode,
        }
    }

    pub fn inverse(&self, hash: &Hash) -> Self {
        match *self {
            Atom::NewVertex(NewVertex {
                ref up_context,
                flag,
                start,
                end,
                ref inode,
                ..
            }) => {
                let mut edges = Vec::new();
                for up in up_context {
                    edges.push(NewEdge {
                        previous: flag,
                        flag: flag | EdgeFlags::DELETED,
                        from: Position {
                            change: Some(if let Some(ref h) = up.change {
                                *h
                            } else {
                                *hash
                            }),
                            pos: up.pos,
                        },
                        to: Vertex {
                            change: Some(*hash),
                            start,
                            end,
                        },
                        introduced_by: Some(*hash),
                    })
                }
                Atom::EdgeMap(EdgeMap {
                    edges,
                    inode: Position {
                        change: Some(if let Some(p) = inode.change { p } else { *hash }),
                        pos: inode.pos,
                    },
                })
            }
            Atom::EdgeMap(EdgeMap {
                ref edges,
                ref inode,
            }) => Atom::EdgeMap(EdgeMap {
                inode: Position {
                    change: Some(if let Some(p) = inode.change { p } else { *hash }),
                    pos: inode.pos,
                },
                edges: edges
                    .iter()
                    .map(|e| {
                        let mut e = e.clone();
                        e.introduced_by = Some(*hash);
                        std::mem::swap(&mut e.flag, &mut e.previous);
                        e
                    })
                    .collect(),
            }),
        }
    }
}

impl EdgeMap<Option<Hash>> {
    fn concat(mut self, e: EdgeMap<Option<Hash>>) -> Self {
        assert_eq!(self.inode, e.inode);
        self.edges.extend(e.edges.into_iter());
        EdgeMap {
            inode: self.inode,
            edges: self.edges,
        }
    }
}

impl<L: Clone> Hunk<Option<Hash>, L> {
    pub fn inverse(&self, hash: &Hash) -> Self {
        match self {
            Hunk::FileMove { del, add, path } => Hunk::FileMove {
                del: add.inverse(hash),
                add: del.inverse(hash),
                path: path.clone(),
            },
            Hunk::FileDel {
                del,
                contents,
                path,
                encoding,
            } => Hunk::FileUndel {
                undel: del.inverse(hash),
                contents: contents.as_ref().map(|c| c.inverse(hash)),
                path: path.clone(),
                encoding: encoding.clone(),
            },
            Hunk::FileUndel {
                undel,
                contents,
                path,
                encoding,
            } => Hunk::FileDel {
                del: undel.inverse(hash),
                contents: contents.as_ref().map(|c| c.inverse(hash)),
                path: path.clone(),
                encoding: encoding.clone(),
            },
            Hunk::FileAdd {
                add_name,
                add_inode,
                contents,
                path,
                encoding,
            } => {
                let del = match (add_name.inverse(hash), add_inode.inverse(hash)) {
                    (Atom::EdgeMap(e0), Atom::EdgeMap(e1)) => Atom::EdgeMap(e0.concat(e1)),
                    _ => unreachable!(),
                };
                Hunk::FileDel {
                    del,
                    contents: contents.as_ref().map(|c| c.inverse(hash)),
                    path: path.clone(),
                    encoding: encoding.clone(),
                }
            }
            Hunk::SolveNameConflict { name, path } => Hunk::UnsolveNameConflict {
                name: name.inverse(hash),
                path: path.clone(),
            },
            Hunk::UnsolveNameConflict { name, path } => Hunk::SolveNameConflict {
                name: name.inverse(hash),
                path: path.clone(),
            },
            Hunk::Edit {
                change,
                local,
                encoding,
            } => Hunk::Edit {
                change: change.inverse(hash),
                local: local.clone(),
                encoding: encoding.clone(),
            },
            Hunk::Replacement {
                change,
                replacement,
                local,
                encoding,
            } => Hunk::Replacement {
                change: replacement.inverse(hash),
                replacement: change.inverse(hash),
                local: local.clone(),
                encoding: encoding.clone(),
            },
            Hunk::SolveOrderConflict { change, local } => Hunk::UnsolveOrderConflict {
                change: change.inverse(hash),
                local: local.clone(),
            },
            Hunk::UnsolveOrderConflict { change, local } => Hunk::SolveOrderConflict {
                change: change.inverse(hash),
                local: local.clone(),
            },
            Hunk::ResurrectZombies {
                change,
                local,
                encoding,
            } => Hunk::Edit {
                change: change.inverse(hash),
                local: local.clone(),
                encoding: encoding.clone(),
            },
        }
    }
}

impl Change {
    pub fn inverse(&self, hash: &Hash, header: ChangeHeader, metadata: Vec<u8>) -> Self {
        let dependencies = vec![*hash];
        let contents_hash = Hasher::default().finish();
        Change {
            offsets: Offsets::default(),
            hashed: Hashed {
                version: VERSION,
                header,
                dependencies,
                extra_known: self.extra_known.clone(),
                metadata,
                changes: self.changes.iter().map(|r| r.inverse(hash)).collect(),
                contents_hash,
            },
            contents: Vec::new(),
            unhashed: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalByte {
    pub path: String,
    pub line: usize,
    pub inode: Inode,
    pub byte: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Local {
    pub path: String,
    pub line: usize,
}

pub type Hunk<Hash, Local> = BaseHunk<Atom<Hash>, Local>;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum BaseHunk<Atom, Local> {
    FileMove {
        del: Atom,
        add: Atom,
        path: String,
    },
    FileDel {
        del: Atom,
        contents: Option<Atom>,
        path: String,
        encoding: Option<Encoding>,
    },
    FileUndel {
        undel: Atom,
        contents: Option<Atom>,
        path: String,
        encoding: Option<Encoding>,
    },
    FileAdd {
        add_name: Atom,
        add_inode: Atom,
        contents: Option<Atom>,
        path: String,
        encoding: Option<Encoding>,
    },
    SolveNameConflict {
        name: Atom,
        path: String,
    },
    UnsolveNameConflict {
        name: Atom,
        path: String,
    },
    Edit {
        change: Atom,
        local: Local,
        encoding: Option<Encoding>,
    },
    Replacement {
        change: Atom,
        replacement: Atom,
        local: Local,
        encoding: Option<Encoding>,
    },
    SolveOrderConflict {
        change: Atom,
        local: Local,
    },
    UnsolveOrderConflict {
        change: Atom,
        local: Local,
    },
    ResurrectZombies {
        change: Atom,
        local: Local,
        encoding: Option<Encoding>,
    },
}

#[doc(hidden)]
pub struct HunkIter<R, C> {
    rec: Option<R>,
    extra: Option<C>,
    extra2: Option<C>,
}

impl<Context, Local> IntoIterator for Hunk<Context, Local> {
    type IntoIter = HunkIter<Hunk<Context, Local>, Atom<Context>>;
    type Item = Atom<Context>;
    fn into_iter(self) -> Self::IntoIter {
        HunkIter {
            rec: Some(self),
            extra: None,
            extra2: None,
        }
    }
}

impl<Context, Local> Hunk<Context, Local> {
    pub fn iter(&self) -> HunkIter<&Hunk<Context, Local>, &Atom<Context>> {
        HunkIter {
            rec: Some(self),
            extra: None,
            extra2: None,
        }
    }
    pub fn rev_iter(&self) -> RevHunkIter<&Hunk<Context, Local>, &Atom<Context>> {
        RevHunkIter {
            rec: Some(self),
            extra: None,
            extra2: None,
        }
    }
}

impl<Context, Local> Iterator for HunkIter<Hunk<Context, Local>, Atom<Context>> {
    type Item = Atom<Context>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(extra) = self.extra2.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match rec {
                Hunk::FileMove { del, add, .. } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Hunk::FileDel { del, contents, .. } => {
                    self.extra = contents;
                    Some(del)
                }
                Hunk::FileUndel {
                    undel, contents, ..
                } => {
                    self.extra = contents;
                    Some(undel)
                }
                Hunk::FileAdd {
                    add_name,
                    add_inode,
                    contents,
                    ..
                } => {
                    self.extra = Some(add_inode);
                    self.extra2 = contents;
                    Some(add_name)
                }
                Hunk::SolveNameConflict { name, .. } => Some(name),
                Hunk::UnsolveNameConflict { name, .. } => Some(name),
                Hunk::Edit { change, .. } => Some(change),
                Hunk::Replacement {
                    change,
                    replacement,
                    ..
                } => {
                    self.extra = Some(replacement);
                    Some(change)
                }
                Hunk::SolveOrderConflict { change, .. } => Some(change),
                Hunk::UnsolveOrderConflict { change, .. } => Some(change),
                Hunk::ResurrectZombies { change, .. } => Some(change),
            }
        } else {
            None
        }
    }
}

impl<'a, Context, Local> Iterator for HunkIter<&'a Hunk<Context, Local>, &'a Atom<Context>> {
    type Item = &'a Atom<Context>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(extra) = self.extra2.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match *rec {
                Hunk::FileMove {
                    ref del, ref add, ..
                } => {
                    self.extra = Some(add);
                    Some(del)
                }
                Hunk::FileDel {
                    ref del,
                    ref contents,
                    ..
                } => {
                    self.extra = contents.as_ref();
                    Some(del)
                }
                Hunk::FileUndel {
                    ref undel,
                    ref contents,
                    ..
                } => {
                    self.extra = contents.as_ref();
                    Some(undel)
                }
                Hunk::FileAdd {
                    ref add_name,
                    ref add_inode,
                    ref contents,
                    ..
                } => {
                    self.extra = Some(add_inode);
                    self.extra2 = contents.as_ref();
                    Some(&add_name)
                }
                Hunk::SolveNameConflict { ref name, .. } => Some(&name),
                Hunk::UnsolveNameConflict { ref name, .. } => Some(&name),
                Hunk::Edit { change: ref c, .. } => Some(c),
                Hunk::Replacement {
                    replacement: ref r,
                    change: ref c,
                    ..
                } => {
                    self.extra = Some(r);
                    Some(c)
                }
                Hunk::SolveOrderConflict { ref change, .. } => Some(change),
                Hunk::UnsolveOrderConflict { ref change, .. } => Some(change),
                Hunk::ResurrectZombies { ref change, .. } => Some(change),
            }
        } else {
            None
        }
    }
}

pub struct RevHunkIter<R, C> {
    rec: Option<R>,
    extra: Option<C>,
    extra2: Option<C>,
}

impl<'a, Context, Local> Iterator for RevHunkIter<&'a Hunk<Context, Local>, &'a Atom<Context>> {
    type Item = &'a Atom<Context>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(extra) = self.extra.take() {
            Some(extra)
        } else if let Some(extra) = self.extra2.take() {
            Some(extra)
        } else if let Some(rec) = self.rec.take() {
            match *rec {
                Hunk::FileMove {
                    ref del, ref add, ..
                } => {
                    self.extra = Some(del);
                    Some(add)
                }
                Hunk::FileDel {
                    ref del,
                    ref contents,
                    ..
                } => {
                    if let Some(ref c) = contents {
                        self.extra = Some(del);
                        Some(c)
                    } else {
                        Some(del)
                    }
                }
                Hunk::FileUndel {
                    ref undel,
                    ref contents,
                    ..
                } => {
                    if let Some(ref c) = contents {
                        self.extra = Some(undel);
                        Some(c)
                    } else {
                        Some(undel)
                    }
                }
                Hunk::FileAdd {
                    ref add_name,
                    ref add_inode,
                    ref contents,
                    ..
                } => {
                    if let Some(ref c) = contents {
                        self.extra = Some(add_inode);
                        self.extra2 = Some(add_name);
                        Some(c)
                    } else {
                        self.extra = Some(add_name);
                        Some(add_inode)
                    }
                }
                Hunk::SolveNameConflict { ref name, .. } => Some(&name),
                Hunk::UnsolveNameConflict { ref name, .. } => Some(&name),
                Hunk::Edit { change: ref c, .. } => Some(c),
                Hunk::Replacement {
                    replacement: ref r,
                    change: ref c,
                    ..
                } => {
                    self.extra = Some(c);
                    Some(r)
                }
                Hunk::SolveOrderConflict { ref change, .. } => Some(change),
                Hunk::UnsolveOrderConflict { ref change, .. } => Some(change),
                Hunk::ResurrectZombies { ref change, .. } => Some(change),
            }
        } else {
            None
        }
    }
}

impl Atom<Option<ChangeId>> {
    fn globalize<T: GraphTxnT>(&self, txn: &T) -> Result<Atom<Option<Hash>>, T::GraphError> {
        match self {
            Atom::NewVertex(NewVertex {
                up_context,
                down_context,
                start,
                end,
                flag,
                inode,
            }) => Ok(Atom::NewVertex(NewVertex {
                up_context: up_context
                    .iter()
                    .map(|&up| Position {
                        change: up
                            .change
                            .as_ref()
                            .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                        pos: up.pos,
                    })
                    .collect(),
                down_context: down_context
                    .iter()
                    .map(|&down| Position {
                        change: down
                            .change
                            .as_ref()
                            .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                        pos: down.pos,
                    })
                    .collect(),
                start: *start,
                end: *end,
                flag: *flag,
                inode: Position {
                    change: inode
                        .change
                        .as_ref()
                        .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                    pos: inode.pos,
                },
            })),
            Atom::EdgeMap(EdgeMap { edges, inode }) => Ok(Atom::EdgeMap(EdgeMap {
                edges: edges
                    .iter()
                    .map(|edge| NewEdge {
                        previous: edge.previous,
                        flag: edge.flag,
                        from: Position {
                            change: edge
                                .from
                                .change
                                .as_ref()
                                .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                            pos: edge.from.pos,
                        },
                        to: Vertex {
                            change: edge
                                .to
                                .change
                                .as_ref()
                                .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                            start: edge.to.start,
                            end: edge.to.end,
                        },
                        introduced_by: edge.introduced_by.as_ref().map(|a| {
                            if let Some(a) = txn.get_external(a).unwrap() {
                                a.into()
                            } else {
                                panic!("introduced by {:?}", a);
                            }
                        }),
                    })
                    .collect(),
                inode: Position {
                    change: inode
                        .change
                        .as_ref()
                        .and_then(|a| txn.get_external(a).unwrap().map(Into::into)),
                    pos: inode.pos,
                },
            })),
        }
    }
}

impl<H> Hunk<H, Local> {
    pub fn local(&self) -> Option<&Local> {
        match self {
            Hunk::Edit { ref local, .. }
            | Hunk::Replacement { ref local, .. }
            | Hunk::SolveOrderConflict { ref local, .. }
            | Hunk::UnsolveOrderConflict { ref local, .. }
            | Hunk::ResurrectZombies { ref local, .. } => Some(local),
            _ => None,
        }
    }

    pub fn path(&self) -> &str {
        match self {
            Hunk::FileMove { ref path, .. }
            | Hunk::FileDel { ref path, .. }
            | Hunk::FileUndel { ref path, .. }
            | Hunk::SolveNameConflict { ref path, .. }
            | Hunk::UnsolveNameConflict { ref path, .. }
            | Hunk::FileAdd { ref path, .. } => path,
            Hunk::Edit { ref local, .. }
            | Hunk::Replacement { ref local, .. }
            | Hunk::SolveOrderConflict { ref local, .. }
            | Hunk::UnsolveOrderConflict { ref local, .. }
            | Hunk::ResurrectZombies { ref local, .. } => &local.path,
        }
    }

    pub fn line(&self) -> Option<usize> {
        self.local().map(|x| x.line)
    }
}

impl<A, Local> BaseHunk<A, Local> {
    pub fn atom_map<B, E, Loc, F: FnMut(A) -> Result<B, E>, L: FnMut(Local) -> Loc>(
        self,
        mut f: F,
        mut l: L,
    ) -> Result<BaseHunk<B, Loc>, E> {
        Ok(match self {
            BaseHunk::FileMove { del, add, path } => BaseHunk::FileMove {
                del: f(del)?,
                add: f(add)?,
                path,
            },
            BaseHunk::FileDel {
                del,
                contents,
                path,
                encoding,
            } => BaseHunk::FileDel {
                del: f(del)?,
                contents: if let Some(c) = contents {
                    Some(f(c)?)
                } else {
                    None
                },
                path,
                encoding,
            },
            BaseHunk::FileUndel {
                undel,
                contents,
                path,
                encoding,
            } => BaseHunk::FileUndel {
                undel: f(undel)?,
                contents: if let Some(c) = contents {
                    Some(f(c)?)
                } else {
                    None
                },
                path,
                encoding,
            },
            BaseHunk::SolveNameConflict { name, path } => BaseHunk::SolveNameConflict {
                name: f(name)?,
                path,
            },
            BaseHunk::UnsolveNameConflict { name, path } => BaseHunk::UnsolveNameConflict {
                name: f(name)?,
                path,
            },
            BaseHunk::FileAdd {
                add_inode,
                add_name,
                contents,
                path,
                encoding,
            } => BaseHunk::FileAdd {
                add_name: f(add_name)?,
                add_inode: f(add_inode)?,
                contents: if let Some(c) = contents {
                    Some(f(c)?)
                } else {
                    None
                },
                path,
                encoding,
            },
            BaseHunk::Edit {
                change,
                local,
                encoding,
            } => BaseHunk::Edit {
                change: f(change)?,
                local: l(local),
                encoding,
            },
            BaseHunk::Replacement {
                change,
                replacement,
                local,
                encoding,
            } => BaseHunk::Replacement {
                change: f(change)?,
                replacement: f(replacement)?,
                local: l(local),
                encoding,
            },
            BaseHunk::SolveOrderConflict { change, local } => BaseHunk::SolveOrderConflict {
                change: f(change)?,
                local: l(local),
            },
            BaseHunk::UnsolveOrderConflict { change, local } => BaseHunk::UnsolveOrderConflict {
                change: f(change)?,
                local: l(local),
            },
            BaseHunk::ResurrectZombies {
                change,
                local,
                encoding,
            } => BaseHunk::ResurrectZombies {
                change: f(change)?,
                local: l(local),
                encoding,
            },
        })
    }
}

impl Hunk<Option<ChangeId>, LocalByte> {
    pub fn globalize<T: GraphTxnT>(
        self,
        txn: &T,
    ) -> Result<Hunk<Option<Hash>, Local>, T::GraphError> {
        self.atom_map(
            |x| x.globalize(txn),
            |l| Local { path: l.path, line: l.line }
        )
    }
}

/// A table of contents of a change, indicating where each section is,
/// to allow seeking inside a change file.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct Offsets {
    pub version: u64,
    pub hashed_len: u64, // length of the hashed contents
    pub unhashed_off: u64,
    pub unhashed_len: u64, // length of the unhashed contents
    pub contents_off: u64,
    pub contents_len: u64,
    pub total: u64,
}

impl LocalChange<Hunk<Option<Hash>, Local>, Author> {
    #[cfg(feature = "zstd")]
    pub const OFFSETS_SIZE: u64 = 56;

    pub fn make_change<T: ChannelTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>>(
        txn: &T,
        channel: &ChannelRef<T>,
        changes: Vec<Hunk<Option<Hash>, Local>>,
        contents: Vec<u8>,
        header: ChangeHeader,
        metadata: Vec<u8>,
    ) -> Result<Self, TxnErr<T::DepsError>> {
        let (dependencies, extra_known) = dependencies(txn, &channel.read(), changes.iter())?;
        trace!("make_change, contents = {:?}", contents);
        let contents_hash = {
            let mut hasher = Hasher::default();
            hasher.update(&contents);
            hasher.finish()
        };
        debug!("make_change, contents_hash = {:?}", contents_hash);
        Ok(LocalChange {
            offsets: Offsets::default(),
            hashed: Hashed {
                version: VERSION,
                header,
                changes,
                contents_hash,
                metadata,
                dependencies,
                extra_known,
            },
            contents,
            unhashed: None,
        })
    }

    pub fn new() -> Self {
        LocalChange {
            offsets: Offsets::default(),
            hashed: Hashed {
                version: VERSION,
                header: ChangeHeader::default(),
                changes: Vec::new(),
                contents_hash: Hasher::default().finish(),
                metadata: Vec::new(),
                dependencies: Vec::new(),
                extra_known: Vec::new(),
            },
            unhashed: None,
            contents: Vec::new(),
        }
    }
}

#[cfg(feature = "zstd")]
const LEVEL: usize = 10;
#[cfg(feature = "zstd")]
const FRAME_SIZE: usize = 256;
#[cfg(feature = "zstd")]
fn compress(input: &[u8], w: &mut Vec<u8>) -> Result<(), ChangeError> {
    let mut cstream = zstd_seekable::SeekableCStream::new(LEVEL, FRAME_SIZE).unwrap();
    let mut output = [0; 4096];
    let mut input_pos = 0;
    while input_pos < input.len() {
        let (out_pos, inp_pos) = cstream.compress(&mut output, &input[input_pos..])?;
        w.write_all(&output[..out_pos])?;
        input_pos += inp_pos;
    }
    while let Ok(n) = cstream.end_stream(&mut output) {
        if n == 0 {
            break;
        }
        w.write_all(&output[..n])?;
    }
    Ok(())
}

impl Change {
    pub fn size_no_contents<R: std::io::Read + std::io::Seek>(
        r: &mut R,
    ) -> Result<u64, ChangeError> {
        let pos = r.seek(std::io::SeekFrom::Current(0))?;
        let mut off = [0u8; Self::OFFSETS_SIZE as usize];
        r.read_exact(&mut off)?;
        let off: Offsets = bincode::deserialize(&off)?;
        if off.version != VERSION && off.version != VERSION_NOENC {
            return Err(ChangeError::VersionMismatch { got: off.version });
        }
        r.seek(std::io::SeekFrom::Start(pos))?;
        Ok(off.contents_off)
    }

    /// Serialise the change as a file named "<hash>.change" in
    /// directory `dir`, where "<hash>" is the actual hash of the
    /// change.
    #[cfg(feature = "zstd")]
    pub fn serialize<W: Write, E: From<ChangeError>, F: FnOnce(&mut Self, &Hash) -> Result<(), E>>(&mut self, mut w: W, f: F) -> Result<Hash, E> {
        // Hashed part.
        let mut hashed = Vec::new();
        bincode::serialize_into(&mut hashed, &self.hashed).map_err(From::from)?;
        trace!("hashed = {:?}", hashed);
        let mut hasher = Hasher::default();
        hasher.update(&hashed);
        let hash = hasher.finish();
        debug!("{:?}", hash);

        f(self, &hash)?;

        // Unhashed part.
        let unhashed = if let Some(ref un) = self.unhashed {
            let s = serde_json::to_string(un).unwrap();
            s.into()
        } else {
            Vec::new()
        };

        // Compress the change.
        let mut hashed_comp = Vec::new();
        let now = std::time::Instant::now();
        compress(&hashed, &mut hashed_comp)?;
        debug!("compressed hashed in {:?}", now.elapsed());
        let now = std::time::Instant::now();
        let unhashed_off = Self::OFFSETS_SIZE + hashed_comp.len() as u64;
        let mut unhashed_comp = Vec::new();
        compress(&unhashed, &mut unhashed_comp)?;
        debug!("compressed unhashed in {:?}", now.elapsed());
        let contents_off = unhashed_off + unhashed_comp.len() as u64;
        let mut contents_comp = Vec::new();
        let now = std::time::Instant::now();
        compress(&self.contents, &mut contents_comp)?;
        debug!("compressed {:?} bytes of contents in {:?}", self.contents.len(), now.elapsed());

        let offsets = Offsets {
            version: VERSION,
            hashed_len: hashed.len() as u64,
            unhashed_off,
            unhashed_len: unhashed.len() as u64,
            contents_off,
            contents_len: self.contents.len() as u64,
            total: contents_off + contents_comp.len() as u64,
        };

        bincode::serialize_into(&mut w, &offsets).map_err(From::from)?;
        w.write_all(&hashed_comp).map_err(From::from)?;
        w.write_all(&unhashed_comp).map_err(From::from)?;
        w.write_all(&contents_comp).map_err(From::from)?;
        debug!("change serialized");

        Ok(hash)
    }

    /// Deserialise a change from the file given as input `file`.
    #[cfg(feature = "zstd")]
    pub fn check_from_buffer(buf: &[u8], hash: &Hash) -> Result<(), ChangeError> {
        let offsets: Offsets = bincode::deserialize_from(&buf[..Self::OFFSETS_SIZE as usize])?;
        if offsets.version != VERSION && offsets.version != VERSION_NOENC {
            return Err(ChangeError::VersionMismatch {
                got: offsets.version,
            });
        }

        debug!("check_from_buffer, offsets = {:?}", offsets);
        let mut s = zstd_seekable::Seekable::init_buf(
            &buf[Self::OFFSETS_SIZE as usize..offsets.unhashed_off as usize],
        )?;
        let mut buf_ = Vec::new();
        buf_.resize(offsets.hashed_len as usize, 0);
        s.decompress(&mut buf_[..], 0)?;
        trace!("check_from_buffer, buf_ = {:?}", buf_);
        let mut hasher = Hasher::default();
        hasher.update(&buf_);
        let computed_hash = hasher.finish();
        debug!("{:?} {:?}", computed_hash, hash);
        if &computed_hash != hash {
            return Err((ChangeError::ChangeHashMismatch {
                claimed: *hash,
                computed: computed_hash,
            })
            .into());
        }

        let hashed: Hashed<Hunk<Option<Hash>, Local>, Author> = if offsets.version == VERSION {
            bincode::deserialize(&buf_)?
        } else {
            let h: Hashed<noenc::Hunk<Option<Hash>, Local>, noenc::Author> =
                bincode::deserialize(&buf_)?;
            h.into()
        };
        buf_.clear();
        buf_.resize(offsets.contents_len as usize, 0);
        let mut s = zstd_seekable::Seekable::init_buf(&buf[offsets.contents_off as usize..])?;
        buf_.resize(offsets.contents_len as usize, 0);
        s.decompress(&mut buf_[..], 0)?;
        let mut hasher = Hasher::default();
        trace!("contents = {:?}", buf_);
        hasher.update(&buf_);
        let computed_hash = hasher.finish();
        debug!(
            "contents hash: {:?}, computed: {:?}",
            hashed.contents_hash, computed_hash
        );
        if computed_hash != hashed.contents_hash {
            return Err(ChangeError::ContentsHashMismatch {
                claimed: hashed.contents_hash,
                computed: computed_hash,
            });
        }
        Ok(())
    }

    /// Deserialise a change from the file given as input `file`.
    #[cfg(feature = "zstd")]
    pub fn deserialize(file: &str, hash: Option<&Hash>) -> Result<Self, ChangeError> {
        use std::io::Read;
        let mut r = std::fs::File::open(file)?;
        let mut buf = vec![0u8; Self::OFFSETS_SIZE as usize];
        r.read_exact(&mut buf)?;
        let offsets: Offsets = bincode::deserialize(&buf)?;
        if offsets.version == VERSION_NOENC {
            return Self::deserialize_noenc(offsets, r, hash);
        } else if offsets.version != VERSION {
            return Err(ChangeError::VersionMismatch {
                got: offsets.version,
            });
        }
        debug!("offsets = {:?}", offsets);
        buf.clear();
        buf.resize((offsets.unhashed_off - Self::OFFSETS_SIZE) as usize, 0);
        r.read_exact(&mut buf)?;

        let hashed: Hashed<Hunk<Option<Hash>, Local>, Author> = {
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut out = vec![0u8; offsets.hashed_len as usize];
            s.decompress(&mut out[..], 0)?;
            let mut hasher = Hasher::default();
            hasher.update(&out);
            let computed_hash = hasher.finish();
            if let Some(hash) = hash {
                if &computed_hash != hash {
                    return Err(ChangeError::ChangeHashMismatch {
                        claimed: *hash,
                        computed: computed_hash,
                    });
                }
            }
            bincode::deserialize_from(&out[..])?
        };
        buf.clear();
        buf.resize((offsets.contents_off - offsets.unhashed_off) as usize, 0);
        let unhashed = if buf.is_empty() {
            None
        } else {
            r.read_exact(&mut buf)?;
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut out = vec![0u8; offsets.unhashed_len as usize];
            s.decompress(&mut out[..], 0)?;
            debug!("parsing unhashed: {:?}", std::str::from_utf8(&out));
            serde_json::from_slice(&out).ok()
        };
        debug!("unhashed = {:?}", unhashed);

        buf.clear();
        buf.resize((offsets.total - offsets.contents_off) as usize, 0);
        let contents = if r.read_exact(&mut buf).is_ok() {
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut contents = vec![0u8; offsets.contents_len as usize];
            s.decompress(&mut contents[..], 0)?;
            contents
        } else {
            Vec::new()
        };
        debug!("contents = {:?}", contents);

        Ok(LocalChange {
            offsets,
            hashed,
            unhashed,
            contents,
        })
    }

    /// Compute the hash of this change. If the `zstd` feature is
    /// enabled, it is probably more efficient to serialise the change
    /// (using the `serialize` method) at the same time, which also
    /// returns the hash.
    pub fn hash(&self) -> Result<Hash, bincode::Error> {
        let input = bincode::serialize(&self.hashed)?;
        let mut hasher = Hasher::default();
        hasher.update(&input);
        Ok(hasher.finish())
    }
}
