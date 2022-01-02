use super::LocalApplyError;
use crate::change::NewEdge;
use crate::missing_context::*;
use crate::pristine::*;

pub fn put_newedge<T, F, K>(
    txn: &mut T,
    graph: &mut T::Graph,
    ws: &mut super::Workspace,
    change: ChangeId,
    inode: Position<Option<Hash>>,
    n: &NewEdge<Option<Hash>>,
    apply_check: F,
    mut known: K,
) -> Result<(), LocalApplyError<T>>
where
    T: GraphMutTxnT + TreeTxnT,
    F: Fn(Vertex<ChangeId>, Vertex<ChangeId>) -> bool,
    K: FnMut(&Hash) -> bool,
{
    debug!("put_newedge {:?} {:?}", n, change);
    check_valid(txn, graph, inode, n, ws)?;
    let n_introduced_by = if let Some(n) = internal(txn, &n.introduced_by, change)? {
        n
    } else {
        return Err(LocalApplyError::InvalidChange.into());
    };

    let mut source = find_source_vertex(txn, graph, &n.from, change, inode, n.flag, ws)?;
    let mut target = find_target_vertex(txn, graph, &n.to, change, inode, n.flag, ws)?;

    if n.flag.contains(EdgeFlags::FOLDER) {
        ws.missing_context.files.insert(target);
    }

    let mut zombies = Vec::new();

    loop {
        if !n.flag.contains(EdgeFlags::DELETED) {
            collect_nondeleted_zombies::<_, _>(
                txn,
                graph,
                &mut known,
                source,
                target,
                &mut zombies,
            )?;
        }
        if target.end > n.to.end {
            assert!(!n.flag.contains(EdgeFlags::FOLDER));
            ws.missing_context.graphs.split(inode, target, n.to.end);
            txn.split_block(graph, &target, n.to.end, &mut ws.adjbuf)?;
            target.end = n.to.end
        }

        if n.flag.contains(EdgeFlags::DELETED) {
            debug_assert!(ws.children.is_empty());
            debug_assert!(ws.parents.is_empty());
            collect_pseudo_edges(txn, graph, ws, inode, target)?;
            if !n.flag.contains(EdgeFlags::FOLDER) {
                reconnect_pseudo_edges(txn, graph, inode, ws, target)?;
            }
            ws.children.clear();
            ws.parents.clear();
        }

        del_graph_with_rev(txn, graph, n.previous, source, target, n_introduced_by)?;
        if apply_check(source, target) {
            put_graph_with_rev(txn, graph, n.flag, source, target, change)?;
            for intro in zombies.drain(..) {
                put_graph_with_rev(txn, graph, EdgeFlags::DELETED, source, target, intro)?;
            }
        }

        if target.end >= n.to.end {
            debug!("{:?} {:?}", target, n.to);
            debug_assert_eq!(target.end, n.to.end);
            break;
        }

        source = target;
        target = *txn
            .find_block(graph, target.end_pos())
            .map_err(LocalApplyError::from)?;
        assert_ne!(source, target);

        if !n.flag.contains(EdgeFlags::BLOCK) {
            break;
        }
    }
    if n.flag.contains(EdgeFlags::DELETED) {
        collect_zombie_context(txn, graph, &mut ws.missing_context, inode, n, change, known)
            .map_err(LocalApplyError::from_missing)?;
    }
    Ok(())
}

fn collect_nondeleted_zombies<T, K>(
    txn: &mut T,
    graph: &mut T::Graph,
    mut known: K,
    source: Vertex<ChangeId>,
    target: Vertex<ChangeId>,
    zombies: &mut Vec<ChangeId>,
) -> Result<(), LocalApplyError<T>>
where
    T: GraphMutTxnT + TreeTxnT,
    K: FnMut(&Hash) -> bool,
{
    for v in iter_deleted_parents(txn, graph, source)? {
        let v = v?;
        let intro = v.introduced_by();
        if !known(&txn.get_external(&intro)?.unwrap().into()) {
            zombies.push(intro)
        }
    }
    for v in iter_adjacent(txn, graph, target, EdgeFlags::empty(), EdgeFlags::all())? {
        let v = v?;
        if v.flag().contains(EdgeFlags::PARENT) {
            continue;
        }
        for v in iter_deleted_parents(txn, graph, target)? {
            let v = v?;
            let intro = v.introduced_by();
            if !known(&txn.get_external(&intro)?.unwrap().into()) {
                zombies.push(intro)
            }
        }
    }
    Ok(())
}

fn check_valid<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    inode: Position<Option<Hash>>,
    n: &NewEdge<Option<Hash>>,
    ws: &mut super::Workspace,
) -> Result<(), LocalApplyError<T>> {
    if n.flag.contains(EdgeFlags::DELETED) {
        ws.missing_context
            .load_graph(txn, graph, inode)
            .map_err(|_| LocalApplyError::InvalidChange)?;
    }
    if (n.previous.is_block() && !n.flag.is_block())
        || (n.previous.is_folder() != n.flag.is_folder())
    {
        return Err(LocalApplyError::InvalidChange.into());
    }

    debug_assert!(ws.children.is_empty());
    debug_assert!(ws.parents.is_empty());
    Ok(())
}

pub(crate) fn find_source_vertex<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    from: &Position<Option<Hash>>,
    change: ChangeId,
    inode: Position<Option<Hash>>,
    flag: EdgeFlags,
    ws: &mut super::Workspace,
) -> Result<Vertex<ChangeId>, LocalApplyError<T>> {
    debug!("find_source_vertex");
    let mut source = *txn.find_block_end(&channel, internal_pos(txn, &from, change)?)?;
    debug!("source = {:?}", source);
    if source.start < from.pos && source.end > from.pos {
        assert!(!flag.contains(EdgeFlags::FOLDER));
        ws.missing_context.graphs.split(inode, source, from.pos);
        txn.split_block(channel, &source, from.pos, &mut ws.adjbuf)?;
        source.end = from.pos;
    }
    Ok(source)
}

pub(crate) fn find_target_vertex<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    to: &Vertex<Option<Hash>>,
    change: ChangeId,
    inode: Position<Option<Hash>>,
    flag: EdgeFlags,
    ws: &mut super::Workspace,
) -> Result<Vertex<ChangeId>, LocalApplyError<T>> {
    let to_pos = internal_pos(txn, &to.start_pos(), change)?;
    debug!("find_target_vertex, to = {:?}", to);
    let mut target = *txn.find_block(channel, to_pos)?;
    debug!("target = {:?}", target);
    if target.start < to.start {
        assert!(!flag.contains(EdgeFlags::FOLDER));
        ws.missing_context.graphs.split(inode, target, to.start);
        txn.split_block(channel, &target, to.start, &mut ws.adjbuf)?;
        target.start = to.start;
    }
    Ok(target)
}

fn collect_pseudo_edges<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    apply: &mut super::Workspace,
    inode: Position<Option<Hash>>,
    v: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T>> {
    for e in iter_adjacent(
        txn,
        &channel,
        v,
        EdgeFlags::empty(),
        EdgeFlags::all() - EdgeFlags::DELETED,
    )? {
        let e = e?;
        debug!("collect_pseudo_edges {:?} {:?}", v, e);
        if !e.flag().contains(EdgeFlags::FOLDER) {
            if e.flag().contains(EdgeFlags::PARENT) {
                let p = txn.find_block_end(channel, e.dest())?;
                if is_alive(txn, channel, p)? {
                    apply.parents.insert(*p);
                }
            } else {
                let p = txn.find_block(channel, e.dest())?;
                if e.flag().contains(EdgeFlags::BLOCK)
                    || (p.is_empty() && !e.flag().contains(EdgeFlags::PSEUDO))
                    || is_alive(txn, channel, p).unwrap()
                {
                    apply.children.insert(*p);
                }
            }
        }
        if e.flag().contains(EdgeFlags::PSEUDO) {
            apply.pseudo.push((v, *e, inode));
        }
    }
    Ok(())
}

fn reconnect_pseudo_edges<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    inode: Position<Option<Hash>>,
    ws: &mut super::Workspace,
    target: Vertex<ChangeId>,
) -> Result<(), LocalApplyError<T>> {
    if ws.parents.is_empty() || ws.children.is_empty() {
        return Ok(());
    }

    let (graph, vids) = if let Some(x) = ws.missing_context.graphs.get(inode) {
        x
    } else {
        return Err(LocalApplyError::InvalidChange.into());
    };

    crate::alive::remove_redundant_parents(
        &graph,
        &vids,
        &mut ws.parents,
        &mut ws.missing_context.covered_parents,
        target,
    );
    for &p in ws.parents.iter() {
        ws.missing_context.covered_parents.insert((p, target));
    }

    crate::alive::remove_redundant_children(&graph, &vids, &mut ws.children, target);

    for &p in ws.parents.iter() {
        debug_assert!(is_alive(txn, channel, &p).unwrap());
        for &c in ws.children.iter() {
            if p != c {
                debug_assert!(is_alive(txn, channel, &c).unwrap());
                put_graph_with_rev(txn, channel, EdgeFlags::PSEUDO, p, c, ChangeId::ROOT)?;
            }
        }
    }
    Ok(())
}
fn collect_zombie_context<T: GraphMutTxnT, K>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut crate::missing_context::Workspace,
    inode: Position<Option<Hash>>,
    n: &NewEdge<Option<Hash>>,
    change_id: ChangeId,
    mut known: K,
) -> Result<(), MissingError<T::GraphError>>
where
    K: FnMut(&Hash) -> bool,
{
    if n.flag.contains(EdgeFlags::FOLDER) {
        return Ok(());
    }
    let mut pos = internal_pos(txn, &n.to.start_pos(), change_id)?;
    let end_pos = internal_pos(txn, &n.to.end_pos(), change_id)?;
    let mut unknown_parents = Vec::new();
    while let Ok(&dest_vertex) = txn.find_block(&channel, pos) {
        debug!("collect zombie context: {:?}", dest_vertex);
        for v in iter_adjacent(
            txn,
            channel,
            dest_vertex,
            EdgeFlags::empty(),
            EdgeFlags::all() - EdgeFlags::DELETED,
        )? {
            let v = v?;
            if v.introduced_by() == change_id || v.dest().change.is_root() {
                continue;
            }
            if v.introduced_by().is_root() {
                ws.pseudo.push((dest_vertex, *v));
                continue;
            }
            if v.flag().contains(EdgeFlags::PARENT) {
                // Unwrap ok, since `v` is in the channel.
                let intro = txn.get_external(&v.introduced_by())?.unwrap().into();
                if !known(&intro) {
                    debug!("unknown: {:?}", v);
                    unknown_parents.push((dest_vertex, *v))
                }
            }
        }
        zombify(txn, channel, ws, change_id, inode, n.flag, &unknown_parents)?;
        if dest_vertex.end < end_pos.pos {
            pos.pos = dest_vertex.end
        } else {
            break;
        }
    }
    Ok(())
}

fn zombify<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    ws: &mut crate::missing_context::Workspace,
    change_id: ChangeId,
    inode: Position<Option<Hash>>,
    flag: EdgeFlags,
    unknown: &[(Vertex<ChangeId>, SerializedEdge)],
) -> Result<(), MissingError<T::GraphError>> {
    for &(dest_vertex, edge) in unknown.iter() {
        let p = *txn.find_block_end(channel, edge.dest())?;
        ws.unknown_parents
            .push((dest_vertex, p, inode, edge.flag()));
        let fold = flag & EdgeFlags::FOLDER;
        debug!("zombify p {:?}, dest_vertex {:?}", p, dest_vertex);
        let mut v = p;
        while let Ok(&u) = txn.find_block_end(channel, v.start_pos()) {
            if u != v {
                debug!("u = {:?}, v = {:?}", u, v);
                put_graph_with_rev(
                    txn,
                    channel,
                    EdgeFlags::DELETED | EdgeFlags::BLOCK | fold,
                    u,
                    v,
                    change_id,
                )?;
                v = u
            } else {
                break;
            }
        }
        // Zombify the first chunk of the split.
        for parent in iter_adjacent(
            txn,
            channel,
            v,
            EdgeFlags::PARENT,
            EdgeFlags::all() - EdgeFlags::DELETED,
        )? {
            let parent = parent?;
            if !parent.flag().contains(EdgeFlags::PSEUDO) {
                ws.parents.insert(*parent);
            }
        }
        debug!("ws.parents = {:?}", ws.parents);
        for parent in ws.parents.drain() {
            let parent_dest = *txn.find_block_end(channel, parent.dest())?;
            let mut flag = EdgeFlags::DELETED | EdgeFlags::BLOCK;
            if parent.flag().contains(EdgeFlags::FOLDER) {
                flag |= EdgeFlags::FOLDER
            }
            put_graph_with_rev(txn, channel, flag, parent_dest, v, change_id)?;
        }
    }
    Ok(())
}
