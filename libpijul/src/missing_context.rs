use crate::alive::{Graph, VertexId};
use crate::change::*;
use crate::find_alive::*;
use crate::pristine::*;
use crate::{HashMap, HashSet};
use std::collections::hash_map::Entry;

#[derive(Debug, Error)]
pub enum MissingError<TxnError: std::error::Error + 'static> {
    #[error(transparent)]
    Txn(TxnError),
    #[error(transparent)]
    Block(#[from] BlockError<TxnError>),
    #[error(transparent)]
    Inconsistent(#[from] InconsistentChange<TxnError>),
}

impl<T: std::error::Error + 'static> std::convert::From<TxnErr<T>> for MissingError<T> {
    fn from(e: TxnErr<T>) -> Self {
        MissingError::Txn(e.0)
    }
}

impl Workspace {
    pub(crate) fn load_graph<T: GraphTxnT>(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        inode: Position<Option<Hash>>,
    ) -> Result<
        Option<&(Graph, HashMap<Vertex<ChangeId>, VertexId>)>,
        InconsistentChange<T::GraphError>,
    > {
        if let Some(change) = inode.change {
            match self.graphs.0.entry(inode) {
                Entry::Occupied(e) => Ok(Some(e.into_mut())),
                Entry::Vacant(v) => {
                    let pos = Position {
                        change: if let Some(&i) = txn.get_internal(&change.into())? {
                            i
                        } else {
                            return Err(InconsistentChange::UndeclaredDep);
                        },
                        pos: inode.pos,
                    };
                    let mut graph = crate::alive::retrieve(txn, channel, pos)?;
                    graph.tarjan();
                    let mut ids = HashMap::default();
                    for (i, l) in graph.lines.iter().enumerate() {
                        ids.insert(l.vertex, VertexId(i));
                    }
                    Ok(Some(v.insert((graph, ids))))
                }
            }
        } else {
            Ok(None)
        }
    }
}

pub(crate) fn repair_missing_up_context<
    'a,
    T: GraphMutTxnT,
    I: IntoIterator<Item = &'a Vertex<ChangeId>>,
>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    change_id: ChangeId,
    inode: Position<Option<Hash>>,
    c: Vertex<ChangeId>,
    d: I,
) -> Result<(), MissingError<T::GraphError>> {
    let now = std::time::Instant::now();
    let mut alive = find_alive_up(txn, channel, &mut ws.files, c, change_id)?;
    crate::TIMERS.lock().unwrap().find_alive += now.elapsed();
    ws.load_graph(txn, channel, inode)?;

    debug!("repair_missing_up_context, alive = {:?}", alive);
    for &d in d {
        if let Some((graph, vids)) = ws.graphs.0.get(&inode) {
            crate::alive::remove_redundant_parents(
                graph,
                vids,
                &mut alive,
                &mut ws.covered_parents,
                d,
            );
        }
        repair_regular_up(txn, channel, &alive, d, EdgeFlags::PSEUDO)?;
    }
    Ok(())
}

fn repair_regular_up<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    alive: &HashSet<Vertex<ChangeId>>,
    d: Vertex<ChangeId>,
    flag: EdgeFlags,
) -> Result<(), TxnErr<T::GraphError>> {
    for &ancestor in alive.iter() {
        debug!("put_graph_with_rev {:?} -> {:?}", ancestor, d);
        if ancestor == d {
            info!(
                "repair_missing_up_context, alive: {:?} == {:?}",
                ancestor, d
            );
            continue;
        }
        debug!("repair_missing_up {:?} {:?}", ancestor, d);
        put_graph_with_rev(txn, channel, flag, ancestor, d, ChangeId::ROOT)?;
    }
    Ok(())
}

pub(crate) fn repair_missing_down_context<
    'a,
    T: GraphMutTxnT,
    I: IntoIterator<Item = &'a Vertex<ChangeId>>,
>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    inode: Position<Option<Hash>>,
    c: Vertex<ChangeId>,
    d: I,
) -> Result<(), MissingError<T::GraphError>> {
    let now = std::time::Instant::now();
    let mut alive = find_alive_down(txn, channel, c)?;
    crate::TIMERS.lock().unwrap().find_alive += now.elapsed();
    ws.load_graph(txn, channel, inode)?;
    if let Some((graph, vids)) = ws.graphs.0.get(&inode) {
        crate::alive::remove_redundant_children(graph, vids, &mut alive, c);
    }

    if !alive.is_empty() {
        debug!("repair_missing_down_context alive = {:#?}", alive);
    }

    for &d in d {
        for &desc in alive.iter() {
            if d == desc {
                info!("repair_missing_down_context, alive: {:?} == {:?}", d, desc);
                continue;
            }
            debug!("repair_missing_down {:?} {:?}", d, desc);
            put_graph_with_rev(txn, channel, EdgeFlags::PSEUDO, d, desc, ChangeId::ROOT)?;
        }
    }
    Ok(())
}

pub(crate) fn repair_context_nondeleted<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    inode: Position<Option<Hash>>,
    change_id: ChangeId,
    e: &NewEdge<Option<Hash>>,
) -> Result<(), MissingError<T::GraphError>> {
    if e.flag.contains(EdgeFlags::FOLDER) {
        return Ok(());
    }
    let source = *txn.find_block_end(&channel, internal_pos(txn, &e.from, change_id)?)?;
    let target = *txn.find_block(&channel, internal_pos(txn, &e.to.start_pos(), change_id)?)?;
    repair_missing_up_context(txn, channel, ws, change_id, inode, source, &[target])?;
    reconnect_target_up(txn, channel, ws, inode, target, change_id)?;
    if e.flag.contains(EdgeFlags::BLOCK) {
        repair_missing_down_context(txn, channel, ws, inode, target, &[target])?;
    } else if is_alive(txn, channel, &source)? {
        repair_missing_down_context(txn, channel, ws, inode, target, &[source])?;
    }
    Ok(())
}

fn reconnect_target_up<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    inode: Position<Option<Hash>>,
    target: Vertex<ChangeId>,
    change_id: ChangeId,
) -> Result<(), MissingError<T::GraphError>> {
    let mut unknown = HashSet::default();
    for v in iter_deleted_parents(txn, channel, target)? {
        let v = v?;
        if v.dest().change.is_root() || v.introduced_by().is_root() {
            continue;
        }
        if v.introduced_by() == change_id {
            unknown.clear();
            break;
        }
        // Else change ~v.introduced_by~ is a change we don't know,
        // since no change can create a conflict with itself.
        unknown.insert(*txn.find_block_end(channel, v.dest())?);
    }
    for up in unknown.drain() {
        repair_missing_up_context(txn, channel, ws, change_id, inode, up, &[target])?;
    }
    Ok(())
}

pub(crate) fn repair_context_deleted<T: GraphMutTxnT, K>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    inode: Position<Option<Hash>>,
    change_id: ChangeId,
    mut known: K,
    e: &NewEdge<Option<Hash>>,
) -> Result<(), MissingError<T::GraphError>>
where
    K: FnMut(Hash) -> bool,
{
    if e.flag.contains(EdgeFlags::FOLDER) {
        return Ok(());
    }
    debug!("repair_context_deleted {:?}", e);
    let mut pos = internal_pos(txn, &e.to.start_pos(), change_id)?;
    while let Ok(&dest_vertex) = txn.find_block(&channel, pos) {
        debug!("repair_context_deleted, dest_vertex = {:?}", dest_vertex);
        repair_children_of_deleted(txn, channel, ws, inode, &mut known, change_id, dest_vertex)?;
        if dest_vertex.end < e.to.end {
            pos.pos = dest_vertex.end
        } else {
            break;
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct Workspace {
    pub(crate) unknown_parents: Vec<(
        Vertex<ChangeId>,
        Vertex<ChangeId>,
        Position<Option<Hash>>,
        EdgeFlags,
    )>,
    unknown: Vec<SerializedEdge>,
    pub(crate) parents: HashSet<SerializedEdge>,
    pub(crate) pseudo: Vec<(Vertex<ChangeId>, SerializedEdge)>,
    repaired: HashSet<Vertex<ChangeId>>,
    pub(crate) graphs: Graphs,
    pub(crate) covered_parents: HashSet<(Vertex<ChangeId>, Vertex<ChangeId>)>,
    pub(crate) files: HashSet<Vertex<ChangeId>>,
}

#[derive(Debug, Default)]
pub(crate) struct Graphs(
    pub HashMap<Position<Option<Hash>>, (Graph, HashMap<Vertex<ChangeId>, crate::alive::VertexId>)>,
);

impl Graphs {
    pub(crate) fn get(
        &self,
        inode: Position<Option<Hash>>,
    ) -> Option<&(Graph, HashMap<Vertex<ChangeId>, VertexId>)> {
        self.0.get(&inode)
    }

    pub fn split(
        &mut self,
        inode: Position<Option<Hash>>,
        vertex: Vertex<ChangeId>,
        mid: ChangePosition,
    ) {
        if let Some((_, vids)) = self.0.get_mut(&inode) {
            if let Some(vid) = vids.remove(&vertex) {
                vids.insert(Vertex { end: mid, ..vertex }, vid);
                vids.insert(
                    Vertex {
                        start: mid,
                        ..vertex
                    },
                    vid,
                );
            }
        }
    }
}

impl Workspace {
    pub fn clear(&mut self) {
        self.unknown.clear();
        self.unknown_parents.clear();
        self.pseudo.clear();
        self.parents.clear();
        self.graphs.0.clear();
        self.repaired.clear();
        self.covered_parents.clear();
    }
    pub fn assert_empty(&self) {
        assert!(self.unknown.is_empty());
        assert!(self.unknown_parents.is_empty());
        assert!(self.pseudo.is_empty());
        assert!(self.parents.is_empty());
        assert!(self.graphs.0.is_empty());
        assert!(self.repaired.is_empty());
        assert!(self.covered_parents.is_empty());
    }
}

fn collect_unknown_children<T: GraphTxnT, K>(
    txn: &T,
    channel: &T::Graph,
    ws: &mut Workspace,
    dest_vertex: Vertex<ChangeId>,
    change_id: ChangeId,
    known: &mut K,
) -> Result<(), TxnErr<T::GraphError>>
where
    K: FnMut(Hash) -> bool,
{
    for v in iter_alive_children(txn, channel, dest_vertex)? {
        let v = v?;
        debug!(
            "collect_unknown_children dest_vertex = {:?}, v = {:?}",
            dest_vertex, v
        );
        if v.introduced_by() == change_id || v.dest().change.is_root() {
            continue;
        }
        if v.introduced_by().is_root() {
            ws.pseudo.push((dest_vertex, *v));
            continue;
        }
        let mut not_del_by_change = true;
        for e in iter_adjacent(
            txn,
            channel,
            dest_vertex,
            EdgeFlags::PARENT | EdgeFlags::DELETED,
            EdgeFlags::all(),
        )? {
            let e = e?;
            if e.introduced_by() == v.introduced_by() {
                not_del_by_change = false;
                break;
            }
        }
        if not_del_by_change {
            let intro = txn.get_external(&v.introduced_by())?.unwrap().into();
            if !known(intro) {
                ws.unknown.push(*v);
            }
        }
    }
    Ok(())
}

fn repair_children_of_deleted<T: GraphMutTxnT, K>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
    inode: Position<Option<Hash>>,
    mut known: K,
    change_id: ChangeId,
    dest_vertex: Vertex<ChangeId>,
) -> Result<(), MissingError<T::GraphError>>
where
    K: FnMut(Hash) -> bool,
{
    trace!("repair_children_of_deleted {:?}", dest_vertex);
    collect_unknown_children(txn, channel, ws, dest_vertex, change_id, &mut known)?;
    let mut unknown = std::mem::replace(&mut ws.unknown, Vec::new());
    debug!("dest_vertex = {:?}, unknown = {:?}", dest_vertex, unknown);
    for edge in unknown.drain(..) {
        let p = *txn.find_block(channel, edge.dest())?;
        assert!(!edge.flag().contains(EdgeFlags::FOLDER));
        debug!("dest_vertex {:?}, p {:?}", dest_vertex, p);
        put_graph_with_rev(txn, channel, EdgeFlags::db(), dest_vertex, p, change_id)?;
        let mut u = p;
        while let Ok(&v) = txn.find_block(channel, u.end_pos()) {
            if u != v {
                debug!("repair_children_of_deleted: {:?} -> {:?}", u, v);
                put_graph_with_rev(txn, channel, EdgeFlags::db(), u, v, change_id)?;
                u = v
            } else {
                break;
            }
        }
        if is_alive(txn, channel, &p)? {
            repair_missing_up_context(txn, channel, ws, change_id, inode, dest_vertex, &[p])?;
        } else {
            let alive = find_alive_down(txn, channel, p)?;
            repair_missing_up_context(txn, channel, ws, change_id, inode, dest_vertex, &alive)?;
        }
    }
    ws.unknown = unknown;
    Ok(())
}

pub(crate) fn delete_pseudo_edges<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
) -> Result<(), MissingError<T::GraphError>> {
    if ws.pseudo.is_empty() {
        debug!("no pseudo edges")
    }
    for (dest_vertex, mut e) in ws.pseudo.drain(..) {
        debug!("repair_context_deleted, deleting {:?} {:?}", dest_vertex, e);
        if !is_alive(txn, channel, &dest_vertex)? && !ws.repaired.contains(&dest_vertex) {
            if e.flag().contains(EdgeFlags::PARENT) {
                let p = *txn.find_block_end(channel, e.dest())?;
                if !is_alive(txn, channel, &p)? {
                    debug!("delete {:?} {:?}", p, dest_vertex);
                    e -= EdgeFlags::PARENT;
                    del_graph_with_rev(txn, channel, e.flag(), p, dest_vertex, e.introduced_by())?;
                }
            } else {
                let p = *txn.find_block(channel, e.dest())?;
                if !is_alive(txn, channel, &p)? {
                    debug!("delete (2) {:?} {:?}", dest_vertex, p);
                    del_graph_with_rev(txn, channel, e.flag(), dest_vertex, p, e.introduced_by())?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn repair_parents_of_deleted<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut Workspace,
) -> Result<(), MissingError<T::GraphError>> {
    debug!("repair_parents_of_deleted");
    let mut unknown = std::mem::replace(&mut ws.unknown_parents, Vec::new());
    for (dest_vertex, p, inode, flag) in unknown.drain(..) {
        if flag.contains(EdgeFlags::FOLDER) {
            repair_missing_down_context(txn, channel, ws, inode, dest_vertex, &[dest_vertex])?
        } else {
            repair_missing_down_context(txn, channel, ws, inode, dest_vertex, &[p])?
        }
    }
    ws.unknown_parents = unknown;
    Ok(())
}
