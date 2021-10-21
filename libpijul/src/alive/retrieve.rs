use super::{AliveVertex, Flags, Graph, VertexId};
use crate::pristine::*;
use crate::HashMap;
use std::collections::hash_map::Entry;

pub fn retrieve<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    pos0: Position<ChangeId>,
) -> Result<Graph, TxnErr<T::GraphError>> {
    let now = std::time::Instant::now();
    let mut graph = Graph {
        lines: Vec::new(),
        children: Vec::new(),
        total_bytes: 0,
    };
    let mut cache: HashMap<Position<ChangeId>, VertexId> = HashMap::default();
    graph.lines.push(AliveVertex::DUMMY);
    cache.insert(Position::BOTTOM, VertexId(0));
    graph.lines.push(AliveVertex {
        vertex: pos0.inode_vertex(),
        flags: Flags::empty(),
        children: 0,
        n_children: 0,
        index: 0,
        lowlink: 0,
        scc: 0,
        extra: Vec::new(),
    });
    cache.insert(pos0, VertexId(1));

    let mut stack = vec![VertexId(1)];
    while let Some(vid) = stack.pop() {
        debug!("vid {:?}", vid);
        graph[vid].children = graph.children.len();
        for e in crate::pristine::iter_adjacent(
            txn,
            &channel,
            graph[vid].vertex,
            EdgeFlags::empty(),
            EdgeFlags::PSEUDO | EdgeFlags::BLOCK,
        )? {
            let e = e?;
            let dest_vid = match cache.entry(e.dest()) {
                Entry::Vacant(ent) => {
                    if let Some(alive) = new_vertex(txn, channel, e.dest())? {
                        let n = VertexId(graph.lines.len());
                        ent.insert(n);
                        graph.total_bytes += alive.vertex.len();
                        graph.lines.push(alive);
                        stack.push(n);
                        n
                    } else {
                        continue;
                    }
                }
                Entry::Occupied(e) => *e.get(),
            };
            assert_ne!(graph[vid].vertex.start_pos(), e.dest());
            trace!("child {:?}", dest_vid);
            graph.children.push((Some(*e), dest_vid));
            graph[vid].n_children += 1;
        }
        graph.children.push((None, VertexId::DUMMY));
        graph[vid].n_children += 1;
    }
    crate::TIMERS.lock().unwrap().alive_retrieve += now.elapsed();
    Ok(graph)
}

fn new_vertex<T: GraphTxnT>(
    txn: &T,
    graph: &T::Graph,
    pos: Position<ChangeId>,
) -> Result<Option<AliveVertex>, TxnErr<T::GraphError>> {
    let vertex = *txn.find_block(graph, pos).unwrap();
    if !is_alive(txn, graph, &vertex)? {
        debug!("not alive: {:?}", vertex);
        return Ok(None);
    }
    let mut flags = Flags::empty();
    for e in crate::pristine::iter_adjacent(
        txn,
        graph,
        vertex,
        EdgeFlags::PARENT | EdgeFlags::DELETED | EdgeFlags::BLOCK,
        EdgeFlags::all(),
    )? {
        if e?
            .flag()
            .contains(EdgeFlags::PARENT | EdgeFlags::DELETED | EdgeFlags::BLOCK)
        {
            flags = Flags::ZOMBIE;
            break;
        }
    }
    debug!("flags for {:?}: {:?}", vertex, flags);
    Ok(Some(AliveVertex {
        vertex,
        flags,
        children: 0,
        n_children: 0,
        index: 0,
        lowlink: 0,
        scc: 0,
        extra: Vec::new(),
    }))
}

pub(crate) fn remove_forward_edges<T: GraphMutTxnT>(
    txn: &mut T,
    channel: &mut T::Graph,
    pos: Position<ChangeId>,
) -> Result<(), TxnErr<T::GraphError>> {
    let mut graph = retrieve(txn, channel, pos)?;
    let scc = graph.tarjan(); // SCCs are given here in reverse order.
    let (_, forward_scc) = graph.dfs(&scc);
    let mut forward = Vec::new();
    graph.collect_forward_edges(txn, channel, &scc, &forward_scc, &mut forward)?;
    for &(vertex, edge) in forward.iter() {
        let dest = *txn.find_block(channel, edge.dest()).unwrap();
        debug!(target:"libpijul::forward", "deleting forward edge {:?} {:?} {:?}", vertex, dest, edge);
        del_graph_with_rev(
            txn,
            channel,
            edge.flag(),
            vertex,
            dest,
            edge.introduced_by(),
        )?;
    }
    Ok(())
}
