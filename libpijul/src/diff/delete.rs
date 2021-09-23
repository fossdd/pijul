use super::diff::*;
use super::replace::ConflictContexts;
use super::vertex_buffer::{ConflictMarker, ConflictType, Diff};
use super::{bytes_len, bytes_pos, Line};
use crate::change;
use crate::change::{Atom, EdgeMap, Hunk, LocalByte, NewVertex};
use crate::pristine::*;
use crate::record::Recorded;
use crate::text_encoding::Encoding;

impl Recorded {
    pub(super) fn delete<T: GraphTxnT>(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        diff: &Diff,
        d: &super::diff::D,
        conflict_contexts: &mut ConflictContexts,
        lines_a: &[Line],
        lines_b: &[Line],
        inode: Inode,
        r: usize,
        encoding: &Option<Encoding>,
    ) -> Result<(), TxnErr<T::GraphError>> {
        debug!("delete {:?}: {:?}", r, d[r]);
        self.delete_lines(txn, channel, diff, d, lines_a, lines_b, inode, r, encoding)?;
        let old = d[r].old;
        let len = d[r].old_len;
        self.order_conflict_sides(
            diff,
            d,
            conflict_contexts,
            lines_a,
            lines_b,
            inode,
            old,
            len,
            d[r].new,
            d[r].new_len > 0,
        );
        Ok(())
    }
}

struct Deletion {
    edges: Vec<crate::change::NewEdge<Option<ChangeId>>>,
    resurrect: Vec<crate::change::NewEdge<Option<ChangeId>>>,
}

impl Recorded {
    fn delete_lines<T: GraphTxnT>(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        diff: &Diff,
        d: &super::diff::D,
        lines_a: &[Line],
        lines_b: &[Line],
        inode: Inode,
        r: usize,
        encoding: &Option<Encoding>,
    ) -> Result<(), TxnErr<T::GraphError>> {
        let deletion = delete_lines(txn, channel, diff, d, lines_a, r)?;
        if !deletion.edges.is_empty() {
            self.actions.push(Hunk::Edit {
                change: Atom::EdgeMap(EdgeMap {
                    edges: deletion.edges,
                    inode: diff.inode,
                }),
                local: LocalByte {
                    line: d[r].new + 1,
                    path: diff.path.clone(),
                    inode,
                    byte: Some(bytes_pos(lines_b, d[r].new)),
                },
                encoding: encoding.clone(),
            })
        }
        if !deletion.resurrect.is_empty() {
            self.actions.push(Hunk::ResurrectZombies {
                change: Atom::EdgeMap(EdgeMap {
                    edges: deletion.resurrect,
                    inode: diff.inode,
                }),
                local: LocalByte {
                    line: d[r].new + 1,
                    path: diff.path.clone(),
                    inode,
                    byte: Some(bytes_pos(lines_b, d[r].new)),
                },
                encoding: encoding.clone(),
            })
        }
        Ok(())
    }
}

fn delete_lines<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    diff: &Diff,
    d: &super::diff::D,
    lines_a: &[Line],
    r: usize,
) -> Result<Deletion, TxnErr<T::GraphError>> {
    let old = d[r].old;
    let len = d[r].old_len;
    let mut deletion = Deletion {
        edges: Vec::new(),
        resurrect: Vec::new(),
    };
    let mut pos = bytes_pos(lines_a, old);
    let end_pos = pos + bytes_len(lines_a, old, len);
    let first_vertex = diff.first_vertex_containing(pos).max(1);
    debug!(
        "first_vertex = {:?}, vertex = {:?}",
        first_vertex, diff.pos_a[first_vertex].vertex
    );
    let mut solved_conflict_end = 0;
    let mut i = first_vertex;
    while pos < end_pos {
        debug!("pos = {:?} {:?}", diff.pos_a[i].pos, diff.pos_a[i].vertex);
        let marker = diff.marker.get(&diff.pos_a[i].pos);
        if marker.is_none() || (!diff.pos_a[i].vertex.is_root() && diff.pos_a[i].vertex.is_empty())
        {
            debug!("{:?}", diff.vertex(i, pos, end_pos));
            delete_parents(
                txn,
                channel,
                diff.pos_a[i].vertex,
                diff.vertex(i, pos, end_pos),
                &mut deletion,
            )?
        } else if let Some(ConflictMarker::Begin) = marker {
            debug!(
                "conflict type = {:#?}",
                diff.conflict_ends[diff.pos_a[i].conflict].conflict_type
            );
            if let ConflictType::Zombie = diff.conflict_ends[diff.pos_a[i].conflict].conflict_type {
                solved_conflict_end =
                    solved_conflict_end.max(diff.conflict_ends[diff.pos_a[i].conflict].end_pos)
            }
        } else {
            debug!("conflict: {:?}", marker);
        }
        i += 1;
        if i < diff.pos_a.len() {
            pos = diff.pos_a[i].pos
        } else {
            break;
        }
    }
    if solved_conflict_end > 0 && i < diff.pos_a.len() {
        resurrect_zombies(
            txn,
            channel,
            diff,
            d,
            lines_a,
            r,
            i,
            end_pos,
            solved_conflict_end,
            &mut deletion,
        )?
    }
    Ok(deletion)
}

fn delete_parents<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    graph_key: Vertex<ChangeId>,
    del_key: Vertex<ChangeId>,
    deletion: &mut Deletion,
) -> Result<(), TxnErr<T::GraphError>> {
    for e in iter_adjacent(
        txn,
        &channel,
        graph_key,
        EdgeFlags::PARENT,
        EdgeFlags::all(),
    )? {
        let e = e?;
        if !e.flag().contains(EdgeFlags::PARENT) || e.flag().contains(EdgeFlags::PSEUDO) {
            continue;
        }
        let previous = e.flag() - EdgeFlags::PARENT;
        if graph_key.start != del_key.start
            && !graph_key.is_empty()
            && !e.flag().contains(EdgeFlags::BLOCK)
        {
            continue;
        }
        deletion.edges.push(change::NewEdge {
            previous,
            flag: previous | EdgeFlags::DELETED,
            from: if graph_key.start == del_key.start {
                e.dest().to_option()
            } else {
                del_key.start_pos().to_option()
            },
            to: del_key.to_option(),
            introduced_by: Some(e.introduced_by()),
        })
    }
    Ok(())
}

fn is_conflict_reordering(diff: &Diff, old_bytes: usize, len_bytes: usize) -> bool {
    let mut result = false;
    debug!("conflict reordering {:?} {:?}", old_bytes, len_bytes);
    trace!("markers: {:#?}", diff.marker);
    let mut level = 0;
    for i in old_bytes..old_bytes + len_bytes {
        match diff.marker.get(&i) {
            Some(&ConflictMarker::Next) if level == 0 => result = true,
            Some(&ConflictMarker::Begin) => level += 1,
            Some(&ConflictMarker::End) if level > 0 => level -= 1,
            _ => {}
        }
    }
    debug!("is_conflict_reordering: {:?}", result);
    result
}

impl Recorded {
    fn order_conflict_sides(
        &mut self,
        diff: &Diff,
        dd: &D,
        conflict_contexts: &mut ConflictContexts,
        lines_a: &[Line],
        lines_b: &[Line],
        inode: Inode,
        old: usize,
        len: usize,
        new: usize,
        is_replaced: bool,
    ) {
        let old_bytes = bytes_pos(lines_a, old);
        let len_bytes = bytes_len(lines_a, old, len);
        if !is_conflict_reordering(diff, old_bytes, len_bytes) {
            return;
        }
        let up_context = super::replace::get_up_context(diff, conflict_contexts, lines_a, old);

        let mut contents = self.contents.lock();
        contents.push(0);
        let pos = ChangePosition(contents.len().into());
        contents.push(0);
        let contents_len = contents.len();
        std::mem::drop(contents);

        let down_context = if is_replaced {
            conflict_contexts.reorderings.insert(old, pos);
            Vec::new()
        } else {
            super::replace::get_down_context(
                diff,
                conflict_contexts,
                dd,
                lines_a,
                lines_b,
                old,
                len,
                0,
                0,
                contents_len,
            )
        };
        debug!("Conflict reordering {:?} {:?}", up_context, down_context);
        self.actions.push(Hunk::SolveOrderConflict {
            change: Atom::NewVertex(NewVertex {
                up_context,
                down_context,
                flag: EdgeFlags::empty(),
                start: pos,
                end: pos,
                inode: diff.inode,
            }),
            local: LocalByte {
                line: new + 1,
                path: diff.path.clone(),
                inode,
                byte: Some(bytes_pos(lines_b, new)),
            },
        });
    }
}

fn resurrect_zombies<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    diff: &Diff,
    d: &super::diff::D,
    lines_a: &[Line],
    mut r: usize,
    mut i: usize,
    end_pos: usize,
    solved_conflict_end: usize,
    deletion: &mut Deletion,
) -> Result<(), TxnErr<T::GraphError>> {
    debug!(
        "resurrect_zombies {:?} {:?} {:?} {:?}",
        r, i, end_pos, solved_conflict_end
    );
    debug!("{:#?}", d);
    let mut pos = end_pos;
    if diff.pos_a[i].pos > pos {
        i -= 1;
    }
    while pos < solved_conflict_end {
        r += 1;
        while r < d.len() && d[r].old_len == 0 && bytes_pos(lines_a, d[r].old) < solved_conflict_end
        {
            r += 1
        }
        let next_pos = if r >= d.len() {
            solved_conflict_end
        } else {
            bytes_pos(lines_a, d[r].old).min(solved_conflict_end)
        };
        while i < diff.pos_a.len() {
            if diff.pos_a[i].pos >= next_pos {
                break;
            }
            if diff.pos_a[i].vertex.is_root()
                || (i + 1 < diff.pos_a.len() && diff.pos_a[i + 1].pos <= pos)
            {
                i += 1;
                continue;
            }
            resurrect_zombie(
                txn,
                channel,
                diff.pos_a[i].vertex,
                diff.vertex(i, pos, next_pos),
                deletion,
            )?;
            i += 1
        }
        if r >= d.len() {
            break;
        } else {
            pos = bytes_pos(lines_a, d[r].old) + bytes_len(lines_a, d[r].old, d[r].old_len)
        }
    }
    Ok(())
}

fn resurrect_zombie<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    v: Vertex<ChangeId>,
    target: Vertex<ChangeId>,
    deletion: &mut Deletion,
) -> Result<(), TxnErr<T::GraphError>> {
    debug!("resurrect zombie {:?} {:?}", v, target);
    for e in iter_adjacent(
        txn,
        &channel,
        v,
        EdgeFlags::PARENT,
        EdgeFlags::PARENT | EdgeFlags::DELETED | EdgeFlags::BLOCK,
    )? {
        let e = e?;
        if e.flag().contains(EdgeFlags::PSEUDO) || !e.flag().contains(EdgeFlags::PARENT) {
            continue;
        }
        let previous = e.flag() - EdgeFlags::PARENT;
        let newedge = change::NewEdge {
            previous,
            flag: previous - EdgeFlags::DELETED,
            from: if target.start_pos() == v.start_pos() {
                e.dest().to_option()
            } else {
                target.start_pos().to_option()
            },
            to: target.to_option(),
            introduced_by: Some(e.introduced_by()),
        };
        deletion.resurrect.push(newedge)
    }
    Ok(())
}
