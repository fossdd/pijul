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

pub enum ApplyError<ChangestoreError: std::error::Error, T: GraphTxnT + TreeTxnT> {
    Changestore(ChangestoreError),
    LocalChange(LocalApplyError<T>),
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> std::fmt::Debug for ApplyError<C, T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ApplyError::Changestore(e) => std::fmt::Debug::fmt(e, fmt),
            ApplyError::LocalChange(e) => std::fmt::Debug::fmt(e, fmt),
        }
    }
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> std::fmt::Display for ApplyError<C, T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ApplyError::Changestore(e) => std::fmt::Display::fmt(e, fmt),
            ApplyError::LocalChange(e) => std::fmt::Display::fmt(e, fmt),
        }
    }
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> std::error::Error for ApplyError<C, T> {}

#[derive(Error)]
pub enum LocalApplyError<T: GraphTxnT + TreeTxnT> {
    DependencyMissing { hash: crate::pristine::Hash },
    ChangeAlreadyOnChannel { hash: crate::pristine::Hash },
    Txn(#[from] TxnErr<T::GraphError>),
    Tree(#[from] TreeErr<T::TreeError>),
    Block { block: Position<ChangeId> },
    InvalidChange,
}

impl<T: GraphTxnT + TreeTxnT> std::fmt::Debug for LocalApplyError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            LocalApplyError::DependencyMissing { hash } => {
                write!(fmt, "Dependency missing: {:?}", hash)
            }
            LocalApplyError::ChangeAlreadyOnChannel { hash } => {
                write!(fmt, "Change already on channel: {:?}", hash)
            }
            LocalApplyError::Txn(e) => std::fmt::Debug::fmt(e, fmt),
            LocalApplyError::Tree(e) => std::fmt::Debug::fmt(e, fmt),
            LocalApplyError::Block { block } => write!(fmt, "Block error: {:?}", block),
            LocalApplyError::InvalidChange => write!(fmt, "Invalid change"),
        }
    }
}

impl<T: GraphTxnT + TreeTxnT> std::fmt::Display for LocalApplyError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            LocalApplyError::DependencyMissing { hash } => {
                write!(fmt, "Dependency missing: {:?}", hash)
            }
            LocalApplyError::ChangeAlreadyOnChannel { hash } => {
                write!(fmt, "Change already on channel: {:?}", hash)
            }
            LocalApplyError::Txn(e) => std::fmt::Display::fmt(e, fmt),
            LocalApplyError::Tree(e) => std::fmt::Display::fmt(e, fmt),
            LocalApplyError::Block { block } => write!(fmt, "Block error: {:?}", block),
            LocalApplyError::InvalidChange => write!(fmt, "Invalid change"),
        }
    }
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> From<crate::pristine::TxnErr<T::GraphError>>
    for ApplyError<C, T>
{
    fn from(err: crate::pristine::TxnErr<T::GraphError>) -> Self {
        ApplyError::LocalChange(LocalApplyError::Txn(err))
    }
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> From<crate::pristine::TreeErr<T::TreeError>>
    for ApplyError<C, T>
{
    fn from(err: crate::pristine::TreeErr<T::TreeError>) -> Self {
        ApplyError::LocalChange(LocalApplyError::Tree(err))
    }
}

impl<T: GraphTxnT + TreeTxnT> LocalApplyError<T> {
    fn from_missing(err: MissingError<T::GraphError>) -> Self {
        match err {
            MissingError::Txn(e) => LocalApplyError::Txn(TxnErr(e)),
            MissingError::Block(e) => e.into(),
            MissingError::Inconsistent(_) => LocalApplyError::InvalidChange,
        }
    }
}

impl<T: GraphTxnT + TreeTxnT> From<crate::pristine::InconsistentChange<T::GraphError>>
    for LocalApplyError<T>
{
    fn from(err: crate::pristine::InconsistentChange<T::GraphError>) -> Self {
        match err {
            InconsistentChange::Txn(e) => LocalApplyError::Txn(TxnErr(e)),
            _ => LocalApplyError::InvalidChange,
        }
    }
}

impl<T: GraphTxnT + TreeTxnT> From<crate::pristine::BlockError<T::GraphError>>
    for LocalApplyError<T>
{
    fn from(err: crate::pristine::BlockError<T::GraphError>) -> Self {
        match err {
            BlockError::Txn(e) => LocalApplyError::Txn(TxnErr(e)),
            BlockError::Block { block } => LocalApplyError::Block { block },
        }
    }
}

impl<C: std::error::Error, T: GraphTxnT + TreeTxnT> From<crate::pristine::BlockError<T::GraphError>>
    for ApplyError<C, T>
{
    fn from(err: crate::pristine::BlockError<T::GraphError>) -> Self {
        ApplyError::LocalChange(LocalApplyError::from(err))
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
) -> Result<(u64, Merkle), ApplyError<P::Error, T>> {
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
        return Err(ApplyError::LocalChange(
            LocalApplyError::DependencyMissing { hash: *hash },
        ));
    }

    let internal = if let Some(&p) = txn.get_internal(&hash.into())? {
        p
    } else {
        let internal: ChangeId = make_changeid(txn, &hash)?;
        register_change(txn, &internal, hash, &change)?;
        internal
    };
    debug!("internal = {:?}", internal);
    Ok(
        apply_change_to_channel(txn, channel, internal, &hash, &change, workspace)
            .map_err(ApplyError::LocalChange)?,
    )
}

pub fn apply_change_rec_ws<T: TxnT + MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &mut T,
    channel: &mut T::Channel,
    hash: &Hash,
    workspace: &mut Workspace,
    deps_only: bool,
) -> Result<(), ApplyError<P::Error, T>> {
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
                apply_change_to_channel(txn, channel, internal, &hash, &change, workspace)
                    .map_err(ApplyError::LocalChange)?;
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
) -> Result<(u64, Merkle), ApplyError<P::Error, T>> {
    apply_change_ws(changes, txn, channel, hash, &mut Workspace::new())
}

/// Same as [apply_change], but with a wrapped `txn` and `channel`.
pub fn apply_change_arc<T: MutTxnT, P: ChangeStore>(
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    hash: &Hash,
) -> Result<(u64, Merkle), ApplyError<P::Error, T>> {
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
) -> Result<(), ApplyError<P::Error, T>> {
    apply_change_rec_ws(
        changes,
        txn,
        channel,
        hash,
        &mut Workspace::new(),
        deps_only,
    )
}

fn apply_change_to_channel<T: ChannelMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Channel,
    change_id: ChangeId,
    hash: &Hash,
    change: &Change,
    ws: &mut Workspace,
) -> Result<(u64, Merkle), LocalApplyError<T>> {
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
    T: ChannelMutTxnT + DepsMutTxnT<DepsError = <T as GraphTxnT>::GraphError> + TreeMutTxnT,
>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    change: &Change,
    hash: &Hash,
    inode_updates: &HashMap<usize, InodeUpdate>,
    workspace: &mut Workspace,
) -> Result<(u64, Merkle), LocalApplyError<T>> {
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
    T: ChannelMutTxnT + DepsMutTxnT<DepsError = <T as GraphTxnT>::GraphError> + TreeMutTxnT,
>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    change: &Change,
    hash: &Hash,
    inode_updates: &HashMap<usize, InodeUpdate>,
) -> Result<(u64, Merkle), LocalApplyError<T>> {
    apply_local_change_ws(
        txn,
        channel,
        change,
        hash,
        inode_updates,
        &mut Workspace::new(),
    )
}

fn update_inode<T: ChannelTxnT + TreeMutTxnT>(
    txn: &mut T,
    channel: &T::Channel,
    internal: ChangeId,
    update: &InodeUpdate,
) -> Result<(), LocalApplyError<T>> {
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
    alive_folder: HashMap<Vertex<ChangeId>, bool>,
    folder_stack: Vec<(Vertex<ChangeId>, bool)>,
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
        self.alive_folder.clear();
        self.folder_stack.clear();
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
        assert!(self.alive_folder.is_empty());
        assert!(self.folder_stack.is_empty());
    }
}

pub(crate) fn clean_obsolete_pseudo_edges<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
) -> Result<(), LocalApplyError<T>> {
    let mut alive_folder = std::mem::replace(&mut ws.alive_folder, HashMap::new());
    let mut folder_stack = std::mem::replace(&mut ws.folder_stack, Vec::new());
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

        // If we're deleting a FOLDER edge, repair_context_deleted
        // will not repair its potential descendants. Hence, we must
        // also count as "alive" a FOLDER node with alive descendants.
        if p.flag().is_folder() {
            if folder_has_alive_descendants(txn, channel, &mut alive_folder, &mut folder_stack, b)?
            {
                continue;
            }
        }

        if a.is_empty() && b_is_alive {
            // In this case, `a` can be an inode, in which case we
            // can't simply delete the edge, since b would become
            // unreachable.
            //
            // We test this here:
            let mut is_inode = false;
            for e in iter_adjacent(
                txn,
                channel,
                a,
                EdgeFlags::FOLDER | EdgeFlags::PARENT,
                EdgeFlags::all(),
            )? {
                let e = e?;
                if e.flag().contains(EdgeFlags::FOLDER | EdgeFlags::PARENT) {
                    is_inode = true;
                    break;
                }
            }
            if is_inode {
                continue;
            }
        }

        debug!(
            "Deleting {:?} {:?} {:?} {:?} {:?} {:?}",
            a,
            b,
            p.introduced_by(),
            p.flag(),
            a_is_alive,
            b_is_alive,
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

            // Note: if this is a folder edge,
            // repair_missing_up_context will stop immediately, so we
            // don't even need to call it.
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
    ws.alive_folder = alive_folder;
    ws.folder_stack = folder_stack;
    Ok(())
}

fn folder_has_alive_descendants<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    alive: &mut HashMap<Vertex<ChangeId>, bool>,
    stack: &mut Vec<(Vertex<ChangeId>, bool)>,
    b: Vertex<ChangeId>,
) -> Result<bool, LocalApplyError<T>> {
    if let Some(r) = alive.get(&b) {
        return Ok(*r);
    }
    debug!("alive descendants");
    stack.clear();
    stack.push((b, false));
    while let Some((b, visited)) = stack.pop() {
        debug!("visiting {:?} {:?}", b, visited);
        if visited {
            if !alive.contains_key(&b) {
                alive.insert(b, false);
            }
            continue;
        }
        stack.push((b, true));
        for e in iter_adjacent(
            txn,
            channel,
            b,
            EdgeFlags::empty(),
            EdgeFlags::all() - EdgeFlags::DELETED - EdgeFlags::PARENT,
        )? {
            let e = e?;
            if e.flag().contains(EdgeFlags::FOLDER) {
                let c = txn.find_block(channel, e.dest())?;
                stack.push((*c, false));
            } else {
                // This is a non-deleted non-folder edge.
                let c = txn.find_block(channel, e.dest())?;
                if is_alive(txn, channel, &c)? {
                    // The entire path is alive.
                    for (x, on_path) in stack.iter() {
                        if *on_path {
                            alive.insert(*x, true);
                        }
                    }
                }
            }
        }
    }
    Ok(*alive.get(&b).unwrap_or(&false))
}

fn repair_missing_contexts<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    change: &Change,
) -> Result<(), LocalApplyError<T>> {
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

fn repair_new_vertex_context_up<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    n: &NewVertex<Option<Hash>>,
    vertex: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T>> {
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

fn repair_new_vertex_context_down<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    n: &NewVertex<Option<Hash>>,
    vertex: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T>> {
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

fn repair_edge_context<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    change: &Change,
    n: &EdgeMap<Option<Hash>>,
) -> Result<(), LocalApplyError<T>> {
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

pub(crate) fn repair_cyclic_paths<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
) -> Result<(), LocalApplyError<T>> {
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

fn repair_edge<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    to0: Vertex<ChangeId>,
    ws: &mut Workspace,
) -> Result<(), LocalApplyError<T>> {
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

fn is_rooted<T: GraphTxnT + TreeTxnT>(
    txn: &T,
    channel: &T::Graph,
    v: Vertex<ChangeId>,
    ws: &mut Workspace,
) -> Result<bool, LocalApplyError<T>> {
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

pub fn apply_root_change<R: rand::Rng, T: MutTxnT, P: ChangeStore>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    store: &P,
    rng: R,
) -> Result<Option<(Hash, u64, Merkle)>, ApplyError<P::Error, T>> {
    let mut change = {
        // If the graph already has a root.
        {
            let channel = channel.read();
            let gr = txn.graph(&*channel);
            for v in iter_adjacent(
                &*txn,
                gr,
                Vertex::ROOT,
                EdgeFlags::FOLDER,
                EdgeFlags::FOLDER | EdgeFlags::BLOCK,
            )? {
                let v = txn.find_block(gr, v?.dest())?;
                if v.start == v.end {
                    // Already has a root
                    return Ok(None);
                } else {
                    // Non-empty channel without a root
                    break;
                }
            }
            // If we are here, either the channel is empty, or it
            // isn't and doesn't have a root.
        }
        let root = Position {
            change: Some(Hash::None),
            pos: ChangePosition(0u64.into()),
        };
        let contents = rng
            .sample_iter(rand::distributions::Standard)
            .take(32)
            .collect();
        crate::change::LocalChange::make_change(
            txn,
            channel,
            vec![crate::change::Hunk::AddRoot {
                name: Atom::NewVertex(NewVertex {
                    up_context: vec![root],
                    down_context: Vec::new(),
                    start: ChangePosition(0u64.into()),
                    end: ChangePosition(0u64.into()),
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                    inode: root,
                }),
                inode: Atom::NewVertex(NewVertex {
                    up_context: vec![Position {
                        change: None,
                        pos: ChangePosition(0u64.into()),
                    }],
                    down_context: Vec::new(),
                    start: ChangePosition(1u64.into()),
                    end: ChangePosition(1u64.into()),
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                    inode: root,
                }),
            }],
            contents,
            crate::change::ChangeHeader::default(),
            Vec::new(),
        )?
    };
    let h = store
        .save_change(&mut change, |_, _| Ok(()))
        .map_err(ApplyError::Changestore)?;
    let (n, merkle) = apply_change(store, txn, &mut channel.write(), &h)?;
    Ok(Some((h, n, merkle)))
}
