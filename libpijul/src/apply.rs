//! Apply a change.
use crate::change::{Atom, Change, EdgeMap, NewVertex};
use crate::changestore::ChangeStore;
use crate::missing_context::*;
use crate::pristine::*;
use crate::record::InodeUpdate;
use crate::{HashMap, HashSet};
use thiserror::Error;
pub(crate) mod edge;
pub(crate) use edge::*;
mod vertex;
pub(crate) use vertex::*;

#[derive(Debug, Error)]
pub enum ApplyError<ChangestoreError: std::error::Error, TxnError: std::error::Error + 'static> {
    #[error("Changestore error: {0}")]
    Changestore(ChangestoreError),
    #[error("Local change error: {err}")]
    LocalChange {
        #[from]
        err: LocalApplyError<TxnError>,
    },
}

#[derive(Debug, Error)]
pub enum LocalApplyError<TxnError: std::error::Error + 'static> {
    #[error("Dependency missing: {:?}", hash)]
    DependencyMissing { hash: crate::pristine::Hash },
    #[error("Change already on channel: {:?}", hash)]
    ChangeAlreadyOnChannel { hash: crate::pristine::Hash },
    #[error("Transaction error: {0}")]
    Txn(TxnError),
    #[error("Block error: {:?}", block)]
    Block { block: Position<ChangeId> },
    #[error("Invalid change")]
    InvalidChange,
}

impl<TxnError: std::error::Error> LocalApplyError<TxnError> {
    fn from_missing(err: MissingError<TxnError>) -> Self {
        match err {
            MissingError::Txn(e) => LocalApplyError::Txn(e),
            MissingError::Block(e) => e.into(),
            MissingError::Inconsistent(_) => LocalApplyError::InvalidChange,
        }
    }
}

impl<T: std::error::Error> From<crate::pristine::InconsistentChange<T>> for LocalApplyError<T> {
    fn from(err: crate::pristine::InconsistentChange<T>) -> Self {
        match err {
            InconsistentChange::Txn(e) => LocalApplyError::Txn(e),
            _ => LocalApplyError::InvalidChange,
        }
    }
}

impl<T: std::error::Error> From<crate::pristine::TxnErr<T>> for LocalApplyError<T> {
    fn from(err: crate::pristine::TxnErr<T>) -> Self {
        LocalApplyError::Txn(err.0)
    }
}

impl<C: std::error::Error, T: std::error::Error> From<crate::pristine::TxnErr<T>>
    for ApplyError<C, T>
{
    fn from(err: crate::pristine::TxnErr<T>) -> Self {
        LocalApplyError::Txn(err.0).into()
    }
}

impl<T: std::error::Error> From<crate::pristine::BlockError<T>> for LocalApplyError<T> {
    fn from(err: crate::pristine::BlockError<T>) -> Self {
        match err {
            BlockError::Txn(e) => LocalApplyError::Txn(e),
            BlockError::Block { block } => LocalApplyError::Block { block },
        }
    }
}

/// Apply a change to a channel. This function does not update the
/// inodes/tree tables, i.e. the correspondence between the pristine
/// and the working copy. Therefore, this function must be used only
/// on remote changes, or on "bare" repositories.
pub fn apply_change_ws<T: MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &mut T,
    channel: &mut T::Channel,
    hash: &Hash,
    workspace: &mut Workspace,
) -> Result<(u64, Merkle), ApplyError<P::Error, T::GraphError>> {
    debug!("apply_change {:?}", hash.to_base32());
    workspace.clear();
    let change = changes.get_change(&hash).map_err(ApplyError::Changestore)?;

    for hash in change.dependencies.iter() {
        if let Hash::None = hash {
            continue;
        }
        if let Some(int) = txn.get_internal(&hash.into())? {
            if txn.get_changeset(txn.changes(&channel), int)?.is_some() {
                continue;
            }
        }
        return Err((LocalApplyError::DependencyMissing { hash: *hash }).into());
    }

    let internal = if let Some(&p) = txn.get_internal(&hash.into())? {
        p
    } else {
        let internal: ChangeId = make_changeid(txn, &hash)?;
        register_change(txn, &internal, hash, &change)?;
        internal
    };
    debug!("internal = {:?}", internal);
    Ok(apply_change_to_channel(
        txn, channel, internal, &hash, &change, workspace,
    )?)
}

pub fn apply_change_rec_ws<T: TxnT + MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &mut T,
    channel: &mut T::Channel,
    hash: &Hash,
    workspace: &mut Workspace,
    deps_only: bool,
) -> Result<(), ApplyError<P::Error, T::GraphError>> {
    debug!("apply_change {:?}", hash.to_base32());
    workspace.clear();
    let mut dep_stack = vec![(*hash, true, !deps_only)];
    let mut visited = HashSet::default();
    while let Some((hash, first, actually_apply)) = dep_stack.pop() {
        let change = changes.get_change(&hash).map_err(ApplyError::Changestore)?;
        let shash: SerializedHash = (&hash).into();
        if first {
            if !visited.insert(hash) {
                continue;
            }
            if let Some(change_id) = txn.get_internal(&shash)? {
                if txn
                    .get_changeset(txn.changes(&channel), change_id)?
                    .is_some()
                {
                    continue;
                }
            }

            dep_stack.push((hash, false, actually_apply));
            for &hash in change.dependencies.iter() {
                if let Hash::None = hash {
                    continue;
                }
                dep_stack.push((hash, true, true))
            }
        } else if actually_apply {
            let applied = if let Some(int) = txn.get_internal(&shash)? {
                txn.get_changeset(txn.changes(&channel), int)?.is_some()
            } else {
                false
            };
            if !applied {
                let internal = if let Some(&p) = txn.get_internal(&shash)? {
                    p
                } else {
                    let internal: ChangeId = make_changeid(txn, &hash)?;
                    register_change(txn, &internal, &hash, &change)?;
                    internal
                };
                debug!("internal = {:?}", internal);
                workspace.clear();
                apply_change_to_channel(txn, channel, internal, &hash, &change, workspace)?;
            }
        }
    }
    Ok(())
}

/// Same as [apply_change_ws], but allocates its own workspace.
pub fn apply_change<T: MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &mut T,
    channel: &mut T::Channel,
    hash: &Hash,
) -> Result<(u64, Merkle), ApplyError<P::Error, T::GraphError>> {
    apply_change_ws(changes, txn, channel, hash, &mut Workspace::new())
}

/// Same as [apply_change], but with a wrapped `txn` and `channel`.
pub fn apply_change_arc<T: MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    hash: &Hash,
) -> Result<(u64, Merkle), ApplyError<P::Error, T::GraphError>> {
    apply_change_ws(
        changes,
        &mut *txn.write(),
        &mut *channel.write(),
        hash,
        &mut Workspace::new(),
    )
}

/// Same as [apply_change_ws], but allocates its own workspace.
pub fn apply_change_rec<T: MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &mut T,
    channel: &mut T::Channel,
    hash: &Hash,
    deps_only: bool,
) -> Result<(), ApplyError<P::Error, T::GraphError>> {
    apply_change_rec_ws(
        changes,
        txn,
        channel,
        hash,
        &mut Workspace::new(),
        deps_only,
    )
}

fn apply_change_to_channel<T: ChannelMutTxnT>(
    txn: &mut T,
    channel: &mut T::Channel,
    change_id: ChangeId,
    hash: &Hash,
    change: &Change,
    ws: &mut Workspace,
) -> Result<(u64, Merkle), LocalApplyError<T::GraphError>> {
    ws.assert_empty();
    let n = txn.apply_counter(channel);
    debug!("apply_change_to_channel {:?} {:?}", change_id, hash);
    let merkle =
        if let Some(m) = txn.put_changes(channel, change_id, txn.apply_counter(channel), hash)? {
            m
        } else {
            return Err(LocalApplyError::ChangeAlreadyOnChannel { hash: *hash });
        };
    debug!("apply change to channel");
    let now = std::time::Instant::now();
    for change_ in change.changes.iter() {
        debug!("Applying {:?} (1)", change_);
        for change_ in change_.iter() {
            match *change_ {
                Atom::NewVertex(ref n) => {
                    put_newvertex(txn, T::graph_mut(channel), change, ws, change_id, n)?
                }
                Atom::EdgeMap(ref n) => {
                    for edge in n.edges.iter() {
                        if !edge.flag.contains(EdgeFlags::DELETED) {
                            put_newedge(
                                txn,
                                T::graph_mut(channel),
                                ws,
                                change_id,
                                n.inode,
                                edge,
                                |_, _| true,
                                |h| change.knows(h),
                            )?;
                        }
                    }
                }
            }
        }
    }
    for change_ in change.changes.iter() {
        debug!("Applying {:?} (2)", change_);
        for change_ in change_.iter() {
            if let Atom::EdgeMap(ref n) = *change_ {
                for edge in n.edges.iter() {
                    if edge.flag.contains(EdgeFlags::DELETED) {
                        put_newedge(
                            txn,
                            T::graph_mut(channel),
                            ws,
                            change_id,
                            n.inode,
                            edge,
                            |_, _| true,
                            |h| change.knows(h),
                        )?;
                    }
                }
            }
        }
    }
    crate::TIMERS.lock().unwrap().apply += now.elapsed();

    clean_obsolete_pseudo_edges(txn, T::graph_mut(channel), ws, change_id)?;

    info!("repairing missing contexts");
    repair_missing_contexts(txn, T::graph_mut(channel), ws, change_id, change)?;
    detect_folder_conflict_resolutions(
        txn,
        T::graph_mut(channel),
        &mut ws.missing_context,
        change_id,
        change,
    )
    .map_err(LocalApplyError::from_missing)?;

    repair_cyclic_paths(txn, T::graph_mut(channel), ws)?;
    info!("done applying change");
    Ok((n, merkle))
}

/// Apply a change created locally: serialize it, compute its hash, and
/// apply it. This function also registers changes in the filesystem
/// introduced by the change (file additions, deletions and moves), to
/// synchronise the pristine and the working copy after the
/// application.
pub fn apply_local_change_ws<
    T: ChannelMutTxnT
        + DepsMutTxnT<DepsError = <T as GraphTxnT>::GraphError>
        + TreeMutTxnT<TreeError = <T as GraphTxnT>::GraphError>,
>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    change: &Change,
    hash: &Hash,
    inode_updates: &HashMap<usize, InodeUpdate>,
    workspace: &mut Workspace,
) -> Result<(u64, Merkle), LocalApplyError<T::GraphError>> {
    let mut channel = channel.write();
    let internal: ChangeId = make_changeid(txn, hash)?;
    debug!("make_changeid {:?} {:?}", hash, internal);

    for hash in change.dependencies.iter() {
        if let Hash::None = hash {
            continue;
        }
        if let Some(int) = txn.get_internal(&hash.into())? {
            if txn.get_changeset(txn.changes(&channel), int)?.is_some() {
                continue;
            }
        }
        return Err((LocalApplyError::DependencyMissing { hash: *hash }).into());
    }

    register_change(txn, &internal, hash, &change)?;
    let n = apply_change_to_channel(txn, &mut channel, internal, &hash, &change, workspace)?;
    for (_, update) in inode_updates.iter() {
        info!("updating {:?}", update);
        update_inode(txn, &channel, internal, update)?;
    }
    Ok(n)
}

/// Same as [apply_local_change_ws], but allocates its own workspace.
pub fn apply_local_change<
    T: ChannelMutTxnT
        + DepsMutTxnT<DepsError = <T as GraphTxnT>::GraphError>
        + TreeMutTxnT<TreeError = <T as GraphTxnT>::GraphError>,
>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    change: &Change,
    hash: &Hash,
    inode_updates: &HashMap<usize, InodeUpdate>,
) -> Result<(u64, Merkle), LocalApplyError<T::GraphError>> {
    apply_local_change_ws(
        txn,
        channel,
        change,
        hash,
        inode_updates,
        &mut Workspace::new(),
    )
}

fn update_inode<T: ChannelTxnT + TreeMutTxnT<TreeError = <T as GraphTxnT>::GraphError>>(
    txn: &mut T,
    channel: &T::Channel,
    internal: ChangeId,
    update: &InodeUpdate,
) -> Result<(), LocalApplyError<T::TreeError>> {
    debug!("update_inode {:?}", update);
    match *update {
        InodeUpdate::Add { inode, pos, .. } => {
            let vertex = Position {
                change: internal,
                pos,
            };
            if txn
                .get_graph(txn.graph(channel), &vertex.inode_vertex(), None)?
                .is_some()
            {
                debug!("Adding inodes: {:?} {:?}", inode, vertex);
                put_inodes_with_rev(txn, &inode, &vertex)?;
            } else {
                debug!("Not adding inodes: {:?} {:?}", inode, vertex);
            }
        }
        InodeUpdate::Deleted { inode } => {
            if let Some(parent) = txn.get_revtree(&inode, None)?.map(|x| x.to_owned()) {
                del_tree_with_rev(txn, &parent, &inode)?;
            }
            // Delete the directory, if it's there.
            txn.del_tree(&OwnedPathId::inode(inode), Some(&inode))?;
            if let Some(&vertex) = txn.get_inodes(&inode, None)? {
                del_inodes_with_rev(txn, &inode, &vertex)?;
            }
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct Workspace {
    parents: HashSet<Vertex<ChangeId>>,
    children: HashSet<Vertex<ChangeId>>,
    pseudo: Vec<(Vertex<ChangeId>, SerializedEdge, Position<Option<Hash>>)>,
    deleted_by: HashSet<ChangeId>,
    up_context: Vec<Vertex<ChangeId>>,
    down_context: Vec<Vertex<ChangeId>>,
    pub(crate) missing_context: crate::missing_context::Workspace,
    rooted: HashMap<Vertex<ChangeId>, bool>,
    adjbuf: Vec<SerializedEdge>,
}

impl Workspace {
    pub fn new() -> Self {
        Self::default()
    }
    fn clear(&mut self) {
        self.children.clear();
        self.parents.clear();
        self.pseudo.clear();
        self.deleted_by.clear();
        self.up_context.clear();
        self.down_context.clear();
        self.missing_context.clear();
        self.rooted.clear();
        self.adjbuf.clear();
    }
    fn assert_empty(&self) {
        assert!(self.children.is_empty());
        assert!(self.parents.is_empty());
        assert!(self.pseudo.is_empty());
        assert!(self.deleted_by.is_empty());
        assert!(self.up_context.is_empty());
        assert!(self.down_context.is_empty());
        self.missing_context.assert_empty();
        assert!(self.rooted.is_empty());
        assert!(self.adjbuf.is_empty());
    }
}

pub(crate) fn clean_obsolete_pseudo_edges<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
) -> Result<(), LocalApplyError<T::GraphError>> {
    for (next_vertex, p, inode) in ws.pseudo.drain(..) {
        let (a, b) = if p.flag().is_parent() {
            if let Ok(&dest) = txn.find_block_end(channel, p.dest()) {
                (dest, next_vertex)
            } else {
                continue;
            }
        } else if let Ok(&dest) = txn.find_block(channel, p.dest()) {
            (next_vertex, dest)
        } else {
            continue;
        };
        let a_is_alive = is_alive(txn, channel, &a)?;
        let b_is_alive = is_alive(txn, channel, &b)?;
        if a_is_alive && b_is_alive {
            continue;
        }
        debug!(
            "Deleting {:?} {:?} {:?} {:?}",
            a,
            b,
            p.introduced_by(),
            p.flag()
        );
        del_graph_with_rev(
            txn,
            channel,
            p.flag() - EdgeFlags::PARENT,
            a,
            b,
            p.introduced_by(),
        )?;
        if a_is_alive {
            debug!("repair down");
            debug_assert!(!b_is_alive);
            crate::missing_context::repair_missing_down_context(
                txn,
                channel,
                &mut ws.missing_context,
                inode,
                b,
                &[a],
            )
            .map_err(LocalApplyError::from_missing)?
        } else if b_is_alive && !p.flag().is_folder() {
            debug!("repair up");
            crate::missing_context::repair_missing_up_context(
                txn,
                channel,
                &mut ws.missing_context,
                change_id,
                inode,
                a,
                &[b],
            )
            .map_err(LocalApplyError::from_missing)?
        }
    }
    Ok(())
}

fn repair_missing_contexts<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    change: &Change,
) -> Result<(), LocalApplyError<T::GraphError>> {
    let now = std::time::Instant::now();
    crate::missing_context::repair_parents_of_deleted(txn, channel, &mut ws.missing_context)
        .map_err(LocalApplyError::from_missing)?;
    for atom in change.changes.iter().flat_map(|r| r.iter()) {
        match atom {
            Atom::NewVertex(ref n) if !n.flag.is_folder() => {
                let vertex = Vertex {
                    change: change_id,
                    start: n.start,
                    end: n.end,
                };
                repair_new_vertex_context_up(txn, channel, ws, change_id, n, vertex)?;
                repair_new_vertex_context_down(txn, channel, ws, change_id, n, vertex)?;
            }
            Atom::NewVertex(_) => {}
            Atom::EdgeMap(ref n) => {
                repair_edge_context(txn, channel, ws, change_id, change, n)?;
            }
        }
    }
    crate::missing_context::delete_pseudo_edges(txn, channel, &mut ws.missing_context)
        .map_err(LocalApplyError::from_missing)?;
    crate::TIMERS.lock().unwrap().repair_context += now.elapsed();
    Ok(())
}

fn repair_new_vertex_context_up<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    n: &NewVertex<Option<Hash>>,
    vertex: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T::GraphError>> {
    for up in n.up_context.iter() {
        let up = *txn.find_block_end(channel, internal_pos(txn, &up, change_id)?)?;
        if !is_alive(txn, channel, &up)? {
            debug!("repairing missing up context {:?} {:?}", up, vertex);
            repair_missing_up_context(
                txn,
                channel,
                &mut ws.missing_context,
                change_id,
                n.inode,
                up,
                &[vertex],
            )
            .map_err(LocalApplyError::from_missing)?
        }
    }
    Ok(())
}

fn repair_new_vertex_context_down<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    n: &NewVertex<Option<Hash>>,
    vertex: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T::GraphError>> {
    debug!("repairing missing context for {:?}", vertex);
    if n.flag.contains(EdgeFlags::FOLDER) {
        return Ok(());
    }
    'outer: for down in n.down_context.iter() {
        let down = *txn.find_block(channel, internal_pos(txn, &down, change_id)?)?;
        for e in iter_adjacent(
            txn,
            channel,
            down,
            EdgeFlags::PARENT,
            EdgeFlags::all() - EdgeFlags::DELETED,
        )? {
            let e = e?;
            if e.introduced_by() != change_id {
                continue 'outer;
            }
        }
        debug!("repairing missing down context {:?} {:?}", down, vertex);
        repair_missing_down_context(
            txn,
            channel,
            &mut ws.missing_context,
            n.inode,
            down,
            &[vertex],
        )
        .map_err(LocalApplyError::from_missing)?
    }
    Ok(())
}

fn repair_edge_context<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    change: &Change,
    n: &EdgeMap<Option<Hash>>,
) -> Result<(), LocalApplyError<T::GraphError>> {
    for e in n.edges.iter() {
        assert!(!e.flag.contains(EdgeFlags::PARENT));
        if e.flag.contains(EdgeFlags::DELETED) {
            trace!("repairing context deleted {:?}", e);
            repair_context_deleted(
                txn,
                channel,
                &mut ws.missing_context,
                n.inode,
                change_id,
                |h| change.knows(&h),
                e,
            )
            .map_err(LocalApplyError::from_missing)?
        } else {
            trace!("repairing context nondeleted {:?}", e);
            repair_context_nondeleted(txn, channel, &mut ws.missing_context, n.inode, change_id, e)
                .map_err(LocalApplyError::from_missing)?
        }
    }
    Ok(())
}

pub(crate) fn repair_cyclic_paths<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
) -> Result<(), LocalApplyError<T::GraphError>> {
    let now = std::time::Instant::now();
    let mut files = std::mem::replace(&mut ws.missing_context.files, HashSet::default());
    for file in files.drain() {
        if file.is_empty() {
            if !is_rooted(txn, channel, file, ws)? {
                repair_edge(txn, channel, file, ws)?
            }
        } else {
            let f0 = EdgeFlags::FOLDER;
            let f1 = EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::PSEUDO;
            let mut iter = iter_adjacent(txn, channel, file, f0, f1)?;
            if let Some(ee) = iter.next() {
                let ee = ee?;
                let dest = ee.dest().inode_vertex();
                if !is_rooted(txn, channel, dest, ws)? {
                    repair_edge(txn, channel, dest, ws)?
                }
            }
        }
    }
    ws.missing_context.files = files;
    crate::TIMERS.lock().unwrap().check_cyclic_paths += now.elapsed();
    Ok(())
}

fn repair_edge<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    to0: Vertex<ChangeId>,
    ws: &mut Workspace,
) -> Result<(), LocalApplyError<T::GraphError>> {
    debug!("repair_edge {:?}", to0);
    let mut stack = vec![(to0, true, true, true)];
    ws.parents.clear();
    while let Some((current, _, al, anc_al)) = stack.pop() {
        if !ws.parents.insert(current) {
            continue;
        }
        debug!("repair_cyclic {:?}", current);
        if current != to0 {
            stack.push((current, true, al, anc_al));
        }
        if current.is_root() {
            debug!("root");
            break;
        }
        if let Some(&true) = ws.rooted.get(&current) {
            debug!("rooted");
            break;
        }
        let f = EdgeFlags::PARENT | EdgeFlags::FOLDER;
        let len = stack.len();
        for parent in iter_adjacent(txn, channel, current, f, EdgeFlags::all())? {
            let parent = parent?;
            if parent.flag().is_parent() {
                let anc = txn.find_block_end(channel, parent.dest())?;
                debug!("is_rooted, parent = {:?}", parent);
                let al = if let Some(e) = iter_adjacent(
                    txn,
                    channel,
                    *anc,
                    f,
                    f | EdgeFlags::BLOCK | EdgeFlags::PSEUDO,
                )?
                .next()
                {
                    e?;
                    true
                } else {
                    false
                };
                debug!("al = {:?}, flag = {:?}", al, parent.flag());
                stack.push((*anc, false, parent.flag().is_deleted(), al));
            }
        }
        if stack.len() == len {
            stack.pop();
        } else {
            (&mut stack[len..]).sort_unstable_by(|a, b| a.3.cmp(&b.3))
        }
    }
    let mut current = to0;
    for (next, on_path, del, _) in stack {
        if on_path {
            if del {
                put_graph_with_rev(
                    txn,
                    channel,
                    EdgeFlags::FOLDER | EdgeFlags::PSEUDO,
                    next,
                    current,
                    ChangeId::ROOT,
                )?;
            }
            current = next
        }
    }
    ws.parents.clear();
    Ok(())
}

fn is_rooted<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    v: Vertex<ChangeId>,
    ws: &mut Workspace,
) -> Result<bool, LocalApplyError<T::GraphError>> {
    let mut alive = false;
    assert!(v.is_empty());
    for e in iter_adjacent(txn, channel, v, EdgeFlags::empty(), EdgeFlags::all())? {
        let e = e?;
        if e.flag().contains(EdgeFlags::PARENT) {
            if e.flag() & (EdgeFlags::FOLDER | EdgeFlags::DELETED) == EdgeFlags::FOLDER {
                alive = true;
                break;
            }
        } else if !e.flag().is_deleted() {
            alive = true;
            break;
        }
    }
    if !alive {
        debug!("is_rooted, not alive");
        return Ok(true);
    }
    // Recycling ws.up_context and ws.parents as a stack and a
    // "visited" hashset, respectively.
    let stack = &mut ws.up_context;
    stack.clear();
    stack.push(v);
    let visited = &mut ws.parents;
    visited.clear();

    while let Some(to) = stack.pop() {
        debug!("is_rooted, pop = {:?}", to);
        if to.is_root() {
            stack.clear();
            for v in visited.drain() {
                ws.rooted.insert(v, true);
            }
            return Ok(true);
        }
        if !visited.insert(to) {
            continue;
        }
        if let Some(&rooted) = ws.rooted.get(&to) {
            if rooted {
                for v in visited.drain() {
                    ws.rooted.insert(v, true);
                }
                return Ok(true);
            } else {
                continue;
            }
        }
        let f = EdgeFlags::PARENT | EdgeFlags::FOLDER;
        for parent in iter_adjacent(
            txn,
            channel,
            to,
            f,
            f | EdgeFlags::PSEUDO | EdgeFlags::BLOCK,
        )? {
            let parent = parent?;
            debug!("is_rooted, parent = {:?}", parent);
            stack.push(*txn.find_block_end(channel, parent.dest())?)
        }
    }
    for v in visited.drain() {
        ws.rooted.insert(v, false);
    }
    Ok(false)
}
