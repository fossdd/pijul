use super::dfs::{Path, PathElement};
use super::{Flags, Graph, VertexId};
use crate::changestore::ChangeStore;
use crate::output::FileError;
use crate::pristine::*;
use crate::vector2::Vector2;
use crate::vertex_buffer::VertexBuffer;

#[derive(Debug)]
struct ConflictStackElt {
    conflict: Vec<Path>,
    side: usize,
    idx: usize,
    id: usize,
}

fn output_conflict<T: ChannelTxnT, B: VertexBuffer, P: ChangeStore>(
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    line_buf: &mut B,
    graph: &Graph,
    sccs: &Vector2<VertexId>,
    conflict: Path,
) -> Result<(), FileError<P::Error, T>> {
    let mut stack = vec![ConflictStackElt {
        conflict: vec![conflict],
        side: 0,
        idx: 0,
        id: 0,
    }];
    let mut is_zombie = None;
    let mut id = 0;
    while let Some(mut elt) = stack.pop() {
        let n_sides = elt.conflict.len();
        if n_sides > 1 && elt.side == 0 && elt.idx == 0 {
            let txn = txn.read();
            let channel = channel.read();
            elt.conflict.sort_by(|a, b| {
                let a_ = a
                    .path
                    .iter()
                    .map(|a| {
                        a.oldest_vertex(changes, &*txn, &*channel, graph, sccs)
                            .unwrap()
                    })
                    .min()
                    .unwrap();
                let b_ = b
                    .path
                    .iter()
                    .map(|b| {
                        b.oldest_vertex(changes, &*txn, &*channel, graph, sccs)
                            .unwrap()
                    })
                    .min()
                    .unwrap();
                a_.cmp(&b_)
            });
            match elt.conflict[elt.side].path[elt.idx] {
                PathElement::Scc { scc } => {
                    let vid = sccs[scc][0];
                    let ext = txn.get_external(&graph[vid].vertex.change)?.unwrap();
                    line_buf.begin_conflict(id, &[&ext.into()])?;
                }
                _ => {
                    line_buf.begin_conflict(id, &[])?;
                }
            }
        }

        let mut next = None;
        'outer: while elt.side < n_sides {
            if elt.side > 0 && elt.idx == 0 {
                if let Some(id) = is_zombie.take() {
                    line_buf.end_zombie_conflict(id)?;
                }
                match elt.conflict[elt.side].path[elt.idx] {
                    PathElement::Scc { scc } => {
                        let vid = sccs[scc][0];
                        let txn = txn.read();
                        let ext = txn.get_external(&graph[vid].vertex.change)?.unwrap();
                        line_buf.conflict_next(elt.id, &[&ext.into()])?;
                    }
                    _ => {
                        line_buf.conflict_next(elt.id, &[])?;
                    }
                }
            }
            while elt.idx < elt.conflict[elt.side].path.len() {
                match elt.conflict[elt.side].path[elt.idx] {
                    PathElement::Scc { scc } => {
                        output_scc(
                            changes,
                            txn,
                            graph,
                            &sccs[scc],
                            &mut is_zombie,
                            &mut id,
                            line_buf,
                        )?;
                        elt.idx += 1;
                    }
                    PathElement::Conflict { ref mut sides } => {
                        let sides = std::mem::replace(sides, Vec::new());
                        elt.idx += 1;
                        id += 1;
                        next = Some(ConflictStackElt {
                            side: 0,
                            idx: 0,
                            conflict: sides,
                            id,
                        });
                        break 'outer;
                    }
                }
            }
            elt.side += 1;
            elt.idx = 0;
        }

        if elt.side >= n_sides {
            if n_sides > 1 {
                if let Some(id) = is_zombie.take() {
                    line_buf.end_zombie_conflict(id)?;
                }
                line_buf.end_conflict(elt.id)?;
            }
        } else {
            if let Some(id) = is_zombie.take() {
                line_buf.end_zombie_conflict(id)?;
            }
            stack.push(elt);
            stack.push(next.unwrap())
        }
    }
    if let Some(id) = is_zombie.take() {
        line_buf.end_zombie_conflict(id)?;
    }
    Ok(())
}

impl PathElement {
    fn oldest_vertex<T: ChannelTxnT, C: ChangeStore>(
        &self,
        changes: &C,
        txn: &T,
        channel: &T::Channel,
        graph: &Graph,
        sccs: &Vector2<VertexId>,
    ) -> Result<u64, TxnErr<T::GraphError>> {
        match *self {
            PathElement::Scc { ref scc } => {
                let mut min: Option<L64> = None;
                for x in sccs[*scc].iter() {
                    if let Some(t) =
                        txn.get_changeset(txn.changes(&channel), &graph[*x].vertex.change)?
                    {
                        if let Some(ref mut m) = min {
                            *m = (*m).min(*t)
                        } else {
                            min = Some(*t)
                        }
                    } else {
                        if log_enabled!(log::Level::Debug) {
                            let f = std::fs::File::create("debug_oldest").unwrap();
                            graph
                                .debug(changes, txn, txn.graph(channel), false, true, f)
                                .unwrap();
                        }
                        panic!("vertex not in channel: {:?}", graph[*x].vertex)
                    }
                }
                Ok(u64::from_le(min.unwrap().0))
            }
            PathElement::Conflict { ref sides } => {
                let mut min: Option<u64> = None;
                for x in sides.iter() {
                    for y in x.path.iter() {
                        let t = y.oldest_vertex(changes, txn, channel, graph, sccs)?;
                        if let Some(ref mut m) = min {
                            *m = (*m).min(t)
                        } else {
                            min = Some(t)
                        }
                    }
                }
                Ok(min.unwrap())
            }
        }
    }
}

fn output_scc<T: GraphTxnT, B: VertexBuffer, P: ChangeStore>(
    changes: &P,
    txn: &ArcTxn<T>,
    graph: &Graph,
    scc: &[VertexId],
    is_zombie: &mut Option<usize>,
    id: &mut usize,
    vbuf: &mut B,
) -> Result<(), FileError<P::Error, T>> {
    let id_cyclic = *id;
    if scc.len() > 1 {
        vbuf.begin_cyclic_conflict(*id)?;
        *id += 1;
    }
    for &v in scc.iter() {
        let now = std::time::Instant::now();
        if graph[v].flags.contains(Flags::ZOMBIE) {
            if is_zombie.is_none() {
                *is_zombie = Some(*id);
                let txn = txn.read();
                let hash = txn.get_external(&graph[v].vertex.change)?.unwrap();
                vbuf.begin_zombie_conflict(*id, &[&hash.into()])?;
                *id += 1
            }
        } else if let Some(id) = is_zombie.take() {
            vbuf.end_zombie_conflict(id)?;
        }
        crate::TIMERS.lock().unwrap().alive_write += now.elapsed();

        let vertex = graph[v].vertex;

        let get_contents = |buf: &mut [u8]| {
            let now = std::time::Instant::now();
            let result = changes
                .get_contents(
                    |p| txn.read().get_external(&p).unwrap().map(|x| x.into()),
                    vertex,
                    buf,
                )
                .map(|_| ())
                .map_err(FileError::Changestore);
            crate::TIMERS.lock().unwrap().alive_contents += now.elapsed();
            result
        };

        let now = std::time::Instant::now();
        debug!("outputting {:?}", vertex);
        vbuf.output_line(vertex, get_contents)?;
        crate::TIMERS.lock().unwrap().alive_write += now.elapsed();
    }
    let now = std::time::Instant::now();
    if scc.len() > 1 {
        vbuf.end_cyclic_conflict(id_cyclic)?;
    }
    crate::TIMERS.lock().unwrap().alive_write += now.elapsed();
    Ok(())
}

pub fn output_graph<T: ChannelTxnT, B: VertexBuffer, P: ChangeStore>(
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    line_buf: &mut B,
    graph: &mut Graph,
    forward: &mut Vec<super::Redundant>,
) -> Result<(), crate::output::FileError<P::Error, T>> {
    if graph.lines.len() <= 1 {
        return Ok(());
    }
    let now0 = std::time::Instant::now();
    let scc = graph.tarjan(); // SCCs are given here in reverse order.
    let (conflict_tree, forward_scc) = graph.dfs(&scc);
    {
        let txn = txn.read();
        let channel = channel.read();
        graph.collect_forward_edges(&*txn, txn.graph(&*channel), &scc, &forward_scc, forward)?;
    }
    crate::TIMERS.lock().unwrap().alive_graph += now0.elapsed();
    let now1 = std::time::Instant::now();
    debug!("conflict_tree = {:?}", conflict_tree);
    output_conflict(changes, txn, channel, line_buf, graph, &scc, conflict_tree)?;
    crate::TIMERS.lock().unwrap().alive_output += now1.elapsed();
    Ok(())
}
