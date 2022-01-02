use super::{LocalApplyError, Workspace};
use crate::change::{Change, NewVertex};
use crate::pristine::*;
use crate::{ChangeId, EdgeFlags, Hash, Vertex};

pub fn put_newvertex<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    ch: &Change,
    ws: &mut Workspace,
    change: ChangeId,
    n: &NewVertex<Option<Hash>>,
) -> Result<(), LocalApplyError<T>> {
    let vertex = Vertex {
        change,
        start: n.start,
        end: n.end,
    };
    if txn.find_block_end(graph, vertex.end_pos()).is_ok()
        || txn.find_block(graph, vertex.start_pos()).is_ok()
    {
        error!("Invalid change: {:?}", vertex);
        return Err(LocalApplyError::InvalidChange);
    }
    debug!(
        "put_newvertex {:?} {:?} {:?} {:?} {:?}",
        vertex, n.up_context, n.down_context, n.flag, change
    );
    assert!(ws.deleted_by.is_empty());
    for up in n.up_context.iter() {
        let up = internal_pos(txn, up, change)?;
        if put_up_context(txn, graph, ch, ws, up)? && n.flag.contains(EdgeFlags::FOLDER) {
            return Err(LocalApplyError::InvalidChange);
        }
    }
    for down in n.down_context.iter() {
        let down = internal_pos(txn, down, change)?;
        if down.change == change {
            return Err(LocalApplyError::InvalidChange);
        }
        if put_down_context(txn, graph, ch, ws, down)? && !n.flag.contains(EdgeFlags::FOLDER) {
            return Err(LocalApplyError::InvalidChange);
        }
    }
    debug!("deleted by: {:?}", ws.deleted_by);

    let up_flag = n.flag | EdgeFlags::BLOCK | EdgeFlags::DELETED;
    for up in ws.up_context.drain(..) {
        assert_ne!(up, vertex);
        if !n.flag.contains(EdgeFlags::FOLDER) {
            for change in ws.deleted_by.iter() {
                put_graph_with_rev(txn, graph, up_flag, up, vertex, *change)?;
            }
        }
        put_graph_with_rev(txn, graph, n.flag | EdgeFlags::BLOCK, up, vertex, change)?;
    }
    debug!("down_context {:?}", ws.down_context);
    let mut down_flag = n.flag;
    if !n.flag.is_folder() {
        down_flag -= EdgeFlags::BLOCK
    }
    for down in ws.down_context.drain(..) {
        assert_ne!(down, vertex);
        put_graph_with_rev(txn, graph, down_flag, vertex, down, change)?;
        if n.flag.is_folder() {
            ws.missing_context.files.insert(down);
        }
    }
    ws.deleted_by.clear();
    Ok(())
}

fn put_up_context<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    ch: &Change,
    ws: &mut Workspace,
    up: Position<ChangeId>,
) -> Result<bool, LocalApplyError<T>> {
    let up_vertex = if up.change.is_root() {
        Vertex::ROOT
    } else {
        debug!("put_up_context {:?}", up);
        let k = *txn.find_block_end(graph, up)?;
        assert_eq!(k.change, up.change);
        assert!(k.start <= up.pos);
        debug!("k = {:?}", k);
        if k.start < up.pos && k.end > up.pos {
            // The missing context "graphs" are only used at the
            // DELETION stage, check that:
            assert!(ws.missing_context.graphs.0.is_empty());
            txn.split_block(graph, &k, up.pos, &mut ws.adjbuf)?
        }
        Vertex {
            change: k.change,
            start: k.start,
            end: up.pos,
        }
    };
    debug!("up_vertex {:?}", up_vertex);
    let flag0 = EdgeFlags::PARENT | EdgeFlags::BLOCK;
    let flag1 = flag0 | EdgeFlags::DELETED | EdgeFlags::FOLDER;
    let mut is_non_folder = false;
    for parent in iter_adjacent(txn, graph, up_vertex, flag0, flag1)? {
        let parent = parent?;
        is_non_folder |=
            parent.flag() & (EdgeFlags::PARENT | EdgeFlags::FOLDER) == EdgeFlags::PARENT;
        if parent
            .flag()
            .contains(EdgeFlags::PARENT | EdgeFlags::DELETED | EdgeFlags::BLOCK)
        {
            let introduced_by = txn.get_external(&parent.introduced_by())?.unwrap().into();
            if !ch.knows(&introduced_by) {
                ws.deleted_by.insert(parent.introduced_by());
            }
        }
    }
    ws.up_context.push(up_vertex);
    Ok(is_non_folder)
}

fn put_down_context<T: GraphMutTxnT + TreeTxnT>(
    txn: &mut T,
    graph: &mut T::Graph,
    ch: &Change,
    ws: &mut Workspace,
    down: Position<ChangeId>,
) -> Result<bool, LocalApplyError<T>> {
    let k = *txn.find_block(&graph, down)?;
    assert_eq!(k.change, down.change);
    assert!(k.end >= down.pos);
    if k.start < down.pos && k.end > down.pos {
        // The missing context "graphs" are only used at the
        // DELETION stage, check that:
        assert!(ws.missing_context.graphs.0.is_empty());
        txn.split_block(graph, &k, down.pos, &mut ws.adjbuf)?
    }
    let down_vertex = Vertex {
        change: k.change,
        start: down.pos,
        end: k.end,
    };
    debug!("down_vertex {:?}", down_vertex);

    let flag0 = EdgeFlags::PARENT;
    let flag1 = flag0 | EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED;
    let mut is_folder = false;
    for parent in iter_adjacent(txn, &graph, down_vertex, flag0, flag1)? {
        let parent = parent?;
        is_folder |= parent
            .flag()
            .contains(EdgeFlags::PARENT | EdgeFlags::FOLDER);
        if parent.flag().contains(EdgeFlags::PARENT | EdgeFlags::BLOCK) {
            if parent.flag().contains(EdgeFlags::DELETED) {
                let introduced_by = txn.get_external(&parent.introduced_by())?.unwrap().into();
                if !ch.knows(&introduced_by) {
                    ws.deleted_by.insert(parent.introduced_by());
                }
            }
        }
    }
    ws.down_context.push(down_vertex);
    Ok(is_folder)
}
