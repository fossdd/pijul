use crate::alive::{output_graph, Graph};
use crate::changestore::*;
use crate::pristine::*;
use crate::record::Recorded;
mod diff;
mod split;
mod vertex_buffer;
pub use diff::Algorithm;
mod delete;
mod replace;

#[derive(Debug, Hash, Clone, Copy)]
struct Line<'a> {
    l: &'a [u8],
    cyclic: bool,
    before_end_marker: bool,
    last: bool,
}

impl<'a> PartialEq for Line<'a> {
    fn eq(&self, b: &Self) -> bool {
        if self.before_end_marker && !b.last && b.l.last() == Some(&b'\n') {
            return &b.l[..b.l.len() - 1] == self.l;
        }
        if b.before_end_marker && !self.last && self.l.last() == Some(&b'\n') {
            return &self.l[..self.l.len() - 1] == b.l;
        }
        self.l == b.l && self.cyclic == b.cyclic
    }
}
impl<'a> Eq for Line<'a> {}

#[derive(Debug, Error)]
pub enum DiffError<P: std::error::Error + 'static, T: std::error::Error + 'static> {
    #[error(transparent)]
    Output(#[from] crate::output::FileError<P, T>),
    #[error(transparent)]
    Txn(T),
}

impl<T: std::error::Error + 'static, C: std::error::Error + 'static> std::convert::From<TxnErr<T>>
    for DiffError<C, T>
{
    fn from(e: TxnErr<T>) -> Self {
        DiffError::Txn(e.0)
    }
}

impl Recorded {
    pub(crate) fn diff<T: ChannelTxnT, P: ChangeStore>(
        &mut self,
        changes: &P,
        txn: &T,
        channel: &T::Channel,
        algorithm: Algorithm,
        path: String,
        inode: Position<Option<ChangeId>>,
        a: &mut Graph,
        b: &[u8],
    ) -> Result<(), DiffError<P::Error, T::GraphError>> {
        self.largest_file = self.largest_file.max(b.len() as u64);
        let mut d = vertex_buffer::Diff::new(inode, path.clone(), a);
        output_graph(changes, txn, channel, &mut d, a, &mut self.redundant)?;
        if (std::str::from_utf8(&d.contents_a).is_err() || std::str::from_utf8(&b).is_err())
            && d.contents_a != b
        {
            self.diff_binary(changes, txn, txn.graph(channel), path, inode, a, &b)?;
            return Ok(());
        }
        let lines_a: Vec<Line> = d
            .lines()
            .map(|l| {
                let old_bytes = l.as_ptr() as usize - d.contents_a.as_ptr() as usize;
                let cyclic = if let Err(n) = d
                    .cyclic_conflict_bytes
                    .binary_search(&(old_bytes, std::usize::MAX))
                {
                    n > 0 && {
                        let (a, b) = d.cyclic_conflict_bytes[n - 1];
                        a <= old_bytes && old_bytes < b
                    }
                } else {
                    false
                };
                let before_end_marker = if l.last() != Some(&b'\n') {
                    let next_index =
                        l.as_ptr() as usize + l.len() - d.contents_a.as_ptr() as usize + 1;
                    d.marker.get(&next_index) == Some(&vertex_buffer::ConflictMarker::End)
                } else {
                    false
                };
                debug!("old = {:?}", l);
                Line {
                    l,
                    cyclic,
                    before_end_marker,
                    last: l.as_ptr() as usize + l.len() - d.contents_a.as_ptr() as usize
                        >= d.contents_a.len(),
                }
            })
            .collect();
        let lines_b: Vec<Line> = split::LineSplit::from(&b[..])
            .map(|l| {
                debug!("new: {:?}", l);
                let next_index = l.as_ptr() as usize + l.len() - b.as_ptr() as usize;
                Line {
                    l,
                    cyclic: false,
                    before_end_marker: false,
                    last: next_index >= b.len(),
                }
            })
            .collect();
        trace!("pos = {:?}", d.pos_a);
        trace!("{:?} {:?}", lines_a, lines_b);
        let dd = diff::diff(&lines_a, &lines_b, algorithm);
        let mut conflict_contexts = replace::ConflictContexts::new();
        for r in 0..dd.len() {
            if dd[r].old_len > 0 {
                self.delete(
                    txn,
                    txn.graph(channel),
                    &d,
                    &dd,
                    &mut conflict_contexts,
                    &lines_a,
                    &lines_b,
                    r,
                )?;
            }
            if dd[r].new_len > 0 {
                self.replace(&d, &mut conflict_contexts, &lines_a, &lines_b, &dd, r);
            }
        }
        debug!("Diff ended");
        Ok(())
    }

    fn diff_binary<T: GraphTxnT, C: ChangeStore>(
        &mut self,
        changes: &C,
        txn: &T,
        channel: &T::Graph,
        path: String,
        inode: Position<Option<ChangeId>>,
        ret: &crate::alive::Graph,
        b: &[u8],
    ) -> Result<(), TxnErr<T::GraphError>> {
        self.has_binary_files = true;
        use crate::change::{Atom, EdgeMap, Hunk, Local, NewEdge, NewVertex};

        let mut contents = self.contents.lock().unwrap();
        let pos = contents.len();
        contents.extend_from_slice(&b[..]);
        let pos_end = contents.len();
        contents.push(0);
        std::mem::drop(contents);

        let mut edges = Vec::new();
        let mut deleted = Vec::new();
        for v in ret.lines.iter() {
            debug!("v.vertex = {:?}, inode = {:?}", v.vertex, inode);
            if Some(v.vertex.change) == inode.change && v.vertex.end == inode.pos {
                continue;
            }
            for e in iter_adjacent(txn, channel, v.vertex, EdgeFlags::PARENT, EdgeFlags::all())? {
                let e = e?;
                if e.flag().contains(EdgeFlags::PSEUDO) {
                    continue;
                }
                if e.flag().contains(EdgeFlags::FOLDER) {
                    if log_enabled!(log::Level::Debug) {
                        let f = std::fs::File::create("debug_diff_binary").unwrap();
                        ret.debug(changes, txn, channel, false, true, f).unwrap();
                    }
                    panic!("e.flag.contains(EdgeFlags::FOLDER)");
                }
                if e.flag().contains(EdgeFlags::PARENT) {
                    if e.flag().contains(EdgeFlags::DELETED) {
                        deleted.push(NewEdge {
                            previous: e.flag() - EdgeFlags::PARENT,
                            flag: e.flag() - EdgeFlags::PARENT,
                            from: e.dest().to_option(),
                            to: v.vertex.to_option(),
                            introduced_by: Some(e.introduced_by()),
                        })
                    } else {
                        let previous = e.flag() - EdgeFlags::PARENT;
                        edges.push(NewEdge {
                            previous,
                            flag: previous | EdgeFlags::DELETED,
                            from: e.dest().to_option(),
                            to: v.vertex.to_option(),
                            introduced_by: Some(e.introduced_by()),
                        })
                    }
                }
            }
        }
        // Kill all of `ret`, add `b` instead.
        if !deleted.is_empty() {
            self.actions.push(Hunk::Edit {
                local: Local {
                    line: 0,
                    path: path.clone(),
                },
                change: Atom::EdgeMap(EdgeMap {
                    edges: deleted,
                    inode,
                }),
            })
        }
        self.actions.push(Hunk::Replacement {
            local: Local { line: 0, path },
            change: Atom::EdgeMap(EdgeMap { edges, inode }),
            replacement: Atom::NewVertex(NewVertex {
                up_context: vec![inode],
                down_context: Vec::new(),
                flag: EdgeFlags::empty(),
                start: ChangePosition(pos.into()),
                end: ChangePosition(pos_end.into()),
                inode,
            }),
        });
        Ok(())
    }
}
fn bytes_pos(chunks: &[Line], old: usize) -> usize {
    debug!("bytes pos {:?} {:?}", old, chunks[old]);
    chunks[old].l.as_ptr() as usize - chunks[0].l.as_ptr() as usize
}
fn bytes_len(chunks: &[Line], old: usize, len: usize) -> usize {
    if let Some(p) = chunks.get(old + len) {
        p.l.as_ptr() as usize - chunks[old].l.as_ptr() as usize
    } else if old + len > 0 {
        chunks[old + len - 1].l.as_ptr() as usize + chunks[old + len - 1].l.len()
            - chunks[old].l.as_ptr() as usize
    } else {
        chunks[old + len].l.as_ptr() as usize - chunks[old].l.as_ptr() as usize
    }
}
