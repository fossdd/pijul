use super::diff::*;
use super::vertex_buffer::{ConflictMarker, Diff};
use super::{bytes_len, bytes_pos, Line};
use crate::change::{Atom, Hunk, LocalByte, NewVertex};
use crate::pristine::{ChangeId, ChangePosition, EdgeFlags, Position, Inode};
use crate::record::Recorded;
use crate::text_encoding::Encoding;
use crate::{HashMap, HashSet};

pub struct ConflictContexts {
    pub up: HashMap<usize, ChangePosition>,
    pub side_ends: HashMap<usize, Vec<ChangePosition>>,
    pub active: HashSet<usize>,
    pub reorderings: HashMap<usize, ChangePosition>,
}

impl ConflictContexts {
    pub fn new() -> Self {
        ConflictContexts {
            side_ends: HashMap::default(),
            up: HashMap::default(),
            active: HashSet::default(),
            reorderings: HashMap::default(),
        }
    }
}

impl Recorded {
    pub(super) fn replace(
        &mut self,
        diff: &Diff,
        conflict_contexts: &mut ConflictContexts,
        lines_a: &[Line],
        lines_b: &[Line],
        inode: Inode,
        dd: &D,
        r: usize,
        encoding: &Option<Encoding>,
    ) {
        let old = dd[r].old;
        let old_len = dd[r].old_len;
        let from_new = dd[r].new;
        let len = dd[r].new_len;
        let up_context = get_up_context(diff, conflict_contexts, lines_a, old);

        let start = self.contents.lock().len();

        let down_context = get_down_context(
            diff,
            conflict_contexts,
            dd,
            lines_a,
            lines_b,
            old,
            old_len,
            from_new,
            len,
            start,
        );

        debug!("old {:?}..{:?}", old, old + old_len);
        trace!("old {:?}", &lines_a[old..(old + old_len)]);
        debug!("new {:?}..{:?}", from_new, from_new + len);
        trace!("new {:?}", &lines_b[from_new..(from_new + len)]);

        let mut contents = self.contents.lock();
        for &line in &lines_b[from_new..(from_new + len)] {
            contents.extend(line.l);
        }
        let end = contents.len();
        if start >= end {
            return;
        }
        contents.push(0);
        std::mem::drop(contents);

        let change = NewVertex {
            up_context,
            down_context,
            flag: EdgeFlags::BLOCK,
            start: ChangePosition(start.into()),
            end: ChangePosition(end.into()),
            inode: diff.inode,
        };
        if old_len > 0 {
            match self.actions.pop() {
                Some(Hunk::Edit {
                    change: c, local, ..
                }) => {
                    if local.line == from_new + 1 {
                        self.actions.push(Hunk::Replacement {
                            change: c,
                            local,
                            replacement: Atom::NewVertex(change),
                            encoding: encoding.clone(),
                        });
                        return;
                    } else {
                        self.actions.push(Hunk::Edit {
                            change: c,
                            local,
                            encoding: encoding.clone(),
                        })
                    }
                }
                Some(c) => self.actions.push(c),
                None => {}
            }
        }
        self.actions.push(Hunk::Edit {
            local: LocalByte {
                line: from_new + 1,
                path: diff.path.clone(),
                inode,
                byte: Some(bytes_pos(lines_b, from_new)),
            },
            change: Atom::NewVertex(change),
            encoding: encoding.clone(),
        });
    }
}

pub(super) fn get_up_context(
    diff: &Diff,
    conflict_contexts: &mut ConflictContexts,
    lines_a: &[Line],
    old: usize,
) -> Vec<Position<Option<ChangeId>>> {
    if let Some(&pos) = conflict_contexts.reorderings.get(&old) {
        return vec![Position { change: None, pos }];
    }
    let old_bytes = if old == 0 {
        return vec![diff.pos_a[0].vertex.end_pos().to_option()];
    } else if old < lines_a.len() {
        bytes_pos(lines_a, old)
    } else {
        diff.contents_a.len()
    };
    debug!("old_bytes {:?}", old_bytes);
    let mut up_context_idx = diff.last_vertex_containing(old_bytes - 1);
    let mut seen_conflict_markers = false;
    loop {
        debug!("up_context_idx = {:?}", up_context_idx);
        debug!("{:?}", diff.marker.get(&diff.pos_a[up_context_idx].pos));
        match diff.marker.get(&diff.pos_a[up_context_idx].pos) {
            None if seen_conflict_markers => {
                return vec![diff.pos_a[up_context_idx].vertex.end_pos().to_option()]
            }
            None => {
                let change = diff.pos_a[up_context_idx].vertex.change;
                let pos = diff.pos_a[up_context_idx].vertex.start;
                let offset = old_bytes - diff.pos_a[up_context_idx].pos;
                debug!("offset {:?} {:?}", pos.0, offset);
                return vec![Position {
                    change: Some(change),
                    pos: ChangePosition(pos.0 + offset),
                }];
            }
            Some(ConflictMarker::End) => {
                debug!("get_up_context_conflict");
                return get_up_context_conflict(diff, conflict_contexts, up_context_idx);
            }
            _ => {
                let conflict = diff.pos_a[up_context_idx].conflict;
                debug!(
                    "conflict = {:?} {:?}",
                    conflict, diff.conflict_ends[conflict]
                );
                if let Some(&pos) = conflict_contexts.up.get(&conflict) {
                    return vec![Position { change: None, pos }];
                }
                seen_conflict_markers = true;
                if diff.conflict_ends[conflict].start > 0 {
                    up_context_idx = diff.conflict_ends[conflict].start - 1
                } else {
                    return vec![diff.pos_a[0].vertex.end_pos().to_option()];
                }
            }
        }
    }
}
fn get_up_context_conflict(
    diff: &Diff,
    conflict_contexts: &mut ConflictContexts,
    mut up_context_idx: usize,
) -> Vec<Position<Option<ChangeId>>> {
    let conflict = diff.pos_a[up_context_idx].conflict;
    let conflict_start = diff.conflict_ends[conflict].start;
    let mut up_context = Vec::new();
    if let Some(ref up) = conflict_contexts.side_ends.get(&up_context_idx) {
        up_context.extend(up.iter().map(|&pos| Position { change: None, pos }));
    }
    let mut on = true;
    conflict_contexts.active.clear();
    conflict_contexts.active.insert(conflict);
    while up_context_idx > conflict_start {
        match diff.marker.get(&diff.pos_a[up_context_idx].pos) {
            None if on => {
                let change = diff.pos_a[up_context_idx].vertex.change;
                let pos = diff.pos_a[up_context_idx].vertex.end;
                up_context.push(Position {
                    change: Some(change),
                    pos,
                });
                on = false
            }
            Some(ConflictMarker::End) if on => {
                conflict_contexts
                    .active
                    .insert(diff.pos_a[up_context_idx].conflict);
            }
            Some(ConflictMarker::Next)
                if conflict_contexts
                    .active
                    .contains(&diff.pos_a[up_context_idx].conflict) =>
            {
                on = true
            }
            _ => {}
        }
        up_context_idx -= 1;
    }
    assert!(!up_context.is_empty());
    up_context
}
pub(super) fn get_down_context(
    diff: &Diff,
    conflict_contexts: &mut ConflictContexts,
    dd: &D,
    lines_a: &[Line],
    lines_b: &[Line],
    old: usize,
    old_len: usize,
    from_new: usize,
    new_len: usize,
    contents_len: usize,
) -> Vec<Position<Option<ChangeId>>> {
    if old + old_len >= lines_a.len() {
        return Vec::new();
    }
    let mut down_context_idx = 1;
    let mut pos_bytes = if old + old_len == 0 {
        0
    } else {
        let pos_bytes = bytes_pos(lines_a, old) + bytes_len(lines_a, old, old_len);
        down_context_idx = diff.first_vertex_containing(pos_bytes);
        pos_bytes
    };
    while down_context_idx < diff.pos_a.len() {
        match diff.marker.get(&(diff.pos_a[down_context_idx].pos)) {
            Some(ConflictMarker::Begin) => {
                return get_down_context_conflict(
                    diff,
                    dd,
                    conflict_contexts,
                    lines_a,
                    lines_b,
                    from_new,
                    new_len,
                    down_context_idx,
                )
            }
            Some(marker) => {
                if let ConflictMarker::Next = marker {
                    let conflict = diff.pos_a[down_context_idx].conflict;
                    down_context_idx = diff.conflict_ends[conflict].end;
                }
                let e = conflict_contexts
                    .side_ends
                    .entry(down_context_idx)
                    .or_default();
                let b_len_bytes = bytes_len(lines_b, from_new, new_len);
                e.push(ChangePosition((contents_len + b_len_bytes).into()));
                down_context_idx += 1
            }
            None => {
                pos_bytes = pos_bytes.max(diff.pos_a[down_context_idx].pos);
                let next_vertex_pos = if down_context_idx + 1 >= diff.pos_a.len() {
                    diff.contents_a.len()
                } else {
                    diff.pos_a[down_context_idx + 1].pos
                };
                while pos_bytes < next_vertex_pos {
                    match dd.is_deleted(lines_a, pos_bytes) {
                        Some(Deleted { replaced: true, .. }) => return Vec::new(),
                        Some(Deleted {
                            replaced: false,
                            next,
                        }) => pos_bytes = next,
                        None => {
                            return vec![diff.position(down_context_idx, pos_bytes).to_option()]
                        }
                    }
                }
                down_context_idx += 1;
            }
        }
    }
    Vec::new()
}
fn get_down_context_conflict(
    diff: &Diff,
    dd: &D,
    conflict_contexts: &mut ConflictContexts,
    lines_a: &[Line],
    lines_b: &[Line],
    from_new: usize,
    new_len: usize,
    mut down_context_idx: usize,
) -> Vec<Position<Option<ChangeId>>> {
    let conflict = diff.pos_a[down_context_idx].conflict;
    let len_bytes = bytes_len(lines_b, from_new, new_len);
    conflict_contexts
        .up
        .insert(conflict, ChangePosition(len_bytes.into()));
    conflict_contexts.active.clear();
    conflict_contexts.active.insert(conflict);
    assert!(!diff.pos_a.is_empty());
    let conflict_end = diff.conflict_ends[conflict].end.min(diff.pos_a.len() - 1);
    let mut down_context = Vec::new();
    let mut on = true;
    let mut pos = diff.pos_a[down_context_idx].pos;
    loop {
        match diff.marker.get(&pos) {
            None if on => match dd.is_deleted(lines_a, pos) {
                Some(Deleted { replaced: true, .. }) => on = false,
                Some(Deleted { next, .. }) => {
                    pos = next;
                    let next_pos = if down_context_idx + 1 < diff.pos_a.len() {
                        diff.pos_a[down_context_idx + 1].pos
                    } else {
                        diff.contents_a.len()
                    };
                    if pos < next_pos {
                        continue;
                    }
                }
                None => {
                    down_context.push(diff.position(down_context_idx, pos).to_option());
                    on = false;
                }
            },
            Some(ConflictMarker::Begin) if on => {
                conflict_contexts
                    .active
                    .insert(diff.pos_a[down_context_idx].conflict);
            }
            Some(ConflictMarker::Next)
                if conflict_contexts
                    .active
                    .contains(&diff.pos_a[down_context_idx].conflict) =>
            {
                on = true
            }
            _ => {}
        }
        down_context_idx += 1;
        if down_context_idx > conflict_end {
            break;
        } else {
            pos = diff.pos_a[down_context_idx].pos
        }
    }
    down_context
}
