use crate::pristine::*;
use crate::vertex_buffer;
use crate::{HashMap, HashSet};

pub(super) struct Diff {
    pub inode: Position<Option<ChangeId>>,
    pub path: String,
    pub contents_a: Vec<u8>,
    pub pos_a: Vec<Vertex>,
    pub missing_eol: HashSet<usize>,
    pub marker: HashMap<usize, ConflictMarker>,
    conflict_stack: Vec<Conflict>,
    pub conflict_ends: Vec<ConflictEnds>,
    pub cyclic_conflict_bytes: Vec<(usize, usize)>,
}

#[derive(Debug, Clone)]
pub struct Conflict {
    pub counter: usize,
    pub side: usize,
    pub conflict_type: ConflictType,
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictType {
    Root,
    Order,
    Zombie,
    Cyclic,
}

#[derive(Debug)]
pub struct ConflictEnds {
    pub start: usize,
    pub end: usize,
    pub end_pos: usize,
    pub conflict_type: ConflictType,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConflictMarker {
    Begin,
    Next,
    End,
}

#[derive(Debug)]
pub struct Vertex {
    pub pos: usize,
    pub vertex: crate::pristine::Vertex<ChangeId>,
    pub before_conflict: bool,
    pub conflict: usize,
}

impl Diff {
    pub fn new(
        inode: Position<Option<ChangeId>>,
        path: String,
        graph: &crate::alive::Graph,
    ) -> Self {
        Diff {
            inode,
            path,
            pos_a: Vec::with_capacity(2 * graph.len_vertices()),
            contents_a: Vec::with_capacity(graph.len_bytes()),
            missing_eol: HashSet::default(),
            conflict_ends: vec![ConflictEnds {
                start: 0,
                end: 0,
                end_pos: 0,
                conflict_type: ConflictType::Root,
            }],
            marker: HashMap::default(),
            conflict_stack: vec![Conflict {
                counter: 0,
                side: 0,
                conflict_type: ConflictType::Root,
            }],
            cyclic_conflict_bytes: Vec::new(),
        }
    }
}

impl Diff {
    pub fn vertex(
        &self,
        i: usize,
        pos: usize,
        end_pos: usize,
    ) -> crate::pristine::Vertex<ChangeId> {
        let mut v = self.pos_a[i].vertex;
        assert!(!v.is_root());
        if pos > self.pos_a[i].pos {
            v.start = ChangePosition(self.pos_a[i].vertex.start.0 + (pos - self.pos_a[i].pos))
        }
        if i + 1 >= self.pos_a.len() || end_pos < self.pos_a[i + 1].pos {
            v.end = ChangePosition(self.pos_a[i].vertex.start.0 + (end_pos - self.pos_a[i].pos))
        }
        v
    }
    pub fn position(&self, i: usize, pos: usize) -> crate::pristine::Position<ChangeId> {
        let mut v = self.pos_a[i].vertex.start_pos();
        if pos > self.pos_a[i].pos {
            v.pos = ChangePosition(self.pos_a[i].vertex.start.0 + (pos - self.pos_a[i].pos))
        }
        v
    }
}

impl Diff {
    fn begin_conflict_(&mut self, conflict_type: ConflictType) {
        self.conflict_stack.push(Conflict {
            counter: self.conflict_ends.len(),
            side: 0,
            conflict_type,
        });
        let len = match self.contents_a.last() {
            Some(&b'\n') | None => self.contents_a.len(),
            _ => {
                self.missing_eol.insert(self.contents_a.len());
                self.contents_a.len() + 1
            }
        };
        self.conflict_ends.push(ConflictEnds {
            start: self.pos_a.len(),
            end: self.pos_a.len(),
            end_pos: len,
            conflict_type,
        });
        self.marker.insert(len, ConflictMarker::Begin);
    }
}

impl vertex_buffer::VertexBuffer for Diff {
    fn output_line<E, C>(&mut self, v: crate::pristine::Vertex<ChangeId>, c: C) -> Result<(), E>
    where
        E: From<std::io::Error>,
        C: FnOnce(&mut [u8]) -> Result<(), E>,
    {
        if v == crate::pristine::Vertex::BOTTOM {
            return Ok(());
        }
        let len = self.contents_a.len();
        self.contents_a.resize(len + (v.end - v.start), 0);
        c(&mut self.contents_a[len..])?;
        self.pos_a.push(Vertex {
            pos: len,
            vertex: v,
            before_conflict: false,
            conflict: self.conflict_stack.last().unwrap().counter,
        });
        Ok(())
    }

    fn begin_conflict(&mut self, id: usize, side: &[&Hash]) -> Result<(), std::io::Error> {
        self.begin_conflict_(ConflictType::Order);
        self.output_conflict_marker(vertex_buffer::START_MARKER, id, side)
    }

    fn begin_cyclic_conflict(&mut self, id: usize) -> Result<(), std::io::Error> {
        let len = self.contents_a.len();
        self.begin_conflict_(ConflictType::Cyclic);
        self.cyclic_conflict_bytes.push((len, len));
        self.output_conflict_marker(vertex_buffer::START_MARKER, id, &[])
    }

    fn begin_zombie_conflict(
        &mut self,
        id: usize,
        add_del: &[&Hash],
    ) -> Result<(), std::io::Error> {
        self.begin_conflict_(ConflictType::Zombie);
        self.output_conflict_marker(vertex_buffer::START_MARKER, id, add_del)
    }

    fn end_conflict(&mut self, id: usize) -> Result<(), std::io::Error> {
        let len = match self.contents_a.last() {
            Some(&b'\n') | None => self.contents_a.len(),
            _ => {
                self.missing_eol.insert(self.contents_a.len());
                self.contents_a.len() + 1
            }
        };
        let chunk = self.pos_a.len();
        self.output_conflict_marker(vertex_buffer::END_MARKER, id, &[])?;
        let conflict = self.conflict_stack.pop().unwrap();
        self.marker.insert(len, ConflictMarker::End);
        self.conflict_ends[conflict.counter].end_pos = len;
        self.conflict_ends[conflict.counter].end = chunk;
        Ok(())
    }
    fn end_cyclic_conflict(&mut self, id: usize) -> Result<(), std::io::Error> {
        debug!("end_cyclic_conflict");
        self.end_conflict(id)?;
        self.cyclic_conflict_bytes.last_mut().unwrap().1 = self.contents_a.len();
        Ok(())
    }

    fn conflict_next(&mut self, id: usize, side: &[&Hash]) -> Result<(), std::io::Error> {
        let len = match self.contents_a.last() {
            Some(&b'\n') | None => self.contents_a.len(),
            _ => {
                self.missing_eol.insert(self.contents_a.len());
                self.contents_a.len() + 1
            }
        };
        self.conflict_stack.last_mut().unwrap().side += 1;
        self.marker.insert(len, ConflictMarker::Next);
        self.output_conflict_marker(vertex_buffer::SEPARATOR, id, side)
    }

    fn output_conflict_marker(
        &mut self,
        marker: &str,
        id: usize,
        sides: &[&Hash],
    ) -> Result<(), std::io::Error> {
        if let Some(line) = self.pos_a.last_mut() {
            line.before_conflict = true
        }
        debug!(
            "output_conflict_marker {:?} {:?}",
            self.contents_a.last(),
            marker
        );
        match self.contents_a.last() {
            Some(&b'\n') | None => {}
            _ => self.contents_a.push(b'\n'),
        }
        let pos = self.contents_a.len();
        use std::io::Write;
        write!(self.contents_a, "{} {}", marker, id)?;
        for side in sides {
            let h = side.to_base32();
            write!(self.contents_a, " [{}]", h.split_at(8).0)?;
        }
        self.contents_a.write_all(b"\n")?;

        self.pos_a.push(Vertex {
            pos,
            vertex: crate::pristine::Vertex::ROOT,
            before_conflict: false,
            conflict: self.conflict_stack.last().unwrap().counter,
        });
        Ok(())
    }
}

impl Diff {
    pub fn last_vertex_containing(&self, pos: usize) -> usize {
        match self.pos_a.binary_search_by(|l| l.pos.cmp(&pos)) {
            Ok(mut i) => loop {
                if i + 1 >= self.pos_a.len() {
                    return i;
                }
                if self.pos_a[i].pos == self.pos_a[i + 1].pos {
                    i += 1
                } else {
                    return i;
                }
            },
            Err(i) => {
                assert!(i > 0);
                i - 1
            }
        }
    }
    pub fn first_vertex_containing(&self, pos: usize) -> usize {
        match self.pos_a.binary_search_by(|l| l.pos.cmp(&pos)) {
            Ok(mut i) => loop {
                if i == 0 {
                    return 0;
                }
                if self.pos_a[i].pos == self.pos_a[i - 1].pos {
                    i -= 1
                } else {
                    return i;
                }
            },
            Err(i) => {
                assert!(i > 0);
                let len = self.pos_a[i-1].vertex.end - self.pos_a[i-1].vertex.start;
                if pos < self.pos_a[i-1].pos + len || len == 0 {
                    i - 1
                } else {
                    i
                }
            }
        }
    }
}
