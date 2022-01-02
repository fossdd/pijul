use crate::pristine::{ChangeId, SerializedEdge, Vertex};
use crate::{HashMap, HashSet};

mod debug;
mod dfs;
mod output;
pub mod retrieve;
mod tarjan;
pub use output::*;
pub use retrieve::*;

#[derive(Debug, Clone)]
pub struct AliveVertex {
    pub vertex: Vertex<ChangeId>,
    flags: Flags,
    pub children: usize,
    pub n_children: usize,
    index: usize,
    lowlink: usize,
    pub scc: usize,
    pub extra: Vec<(Option<SerializedEdge>, VertexId)>,
}

pub struct Redundant {
    pub(crate) v: Vertex<ChangeId>,
    pub(crate) e: SerializedEdge,
}

bitflags! {
    struct Flags: u8 {
        const ZOMBIE = 4;
        const VISITED = 2;
        const ONSTACK = 1;
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct VertexId(pub usize);

impl VertexId {
    pub const DUMMY: VertexId = VertexId(0);
}

impl AliveVertex {
    const DUMMY: AliveVertex = AliveVertex {
        vertex: Vertex::BOTTOM,
        flags: Flags::empty(),
        children: 0,
        n_children: 0,
        index: 0,
        lowlink: 0,
        scc: 0,
        extra: Vec::new(),
    };

    pub fn new(vertex: Vertex<ChangeId>) -> Self {
        AliveVertex {
            vertex,
            flags: Flags::empty(),
            children: 0,
            n_children: 0,
            index: 0,
            lowlink: 0,
            scc: 0,
            extra: Vec::new(),
        }
    }
}
#[derive(Debug)]
pub struct Graph {
    pub lines: Vec<AliveVertex>,
    pub children: Vec<(Option<SerializedEdge>, VertexId)>,
    total_bytes: usize,
}

impl Graph {
    pub fn len_vertices(&self) -> usize {
        self.lines.len()
    }
    pub fn len_bytes(&self) -> usize {
        self.total_bytes
    }
}

impl std::ops::Index<VertexId> for Graph {
    type Output = AliveVertex;
    fn index(&self, idx: VertexId) -> &Self::Output {
        self.lines.index(idx.0)
    }
}
impl std::ops::IndexMut<VertexId> for Graph {
    fn index_mut(&mut self, idx: VertexId) -> &mut Self::Output {
        self.lines.index_mut(idx.0)
    }
}

impl Graph {
    pub fn push_child_to_last(&mut self, e: Option<SerializedEdge>, j: VertexId) {
        let line = self.lines.last_mut().unwrap();
        self.children.push((e, j));
        line.n_children += 1;
    }

    pub fn children<'a>(
        &'a self,
        i: VertexId,
    ) -> impl Iterator<Item = &'a (Option<SerializedEdge>, VertexId)> {
        let line = &self[i];
        (&self.children[line.children..line.children + line.n_children])
            .iter()
            .chain(self[i].extra.iter())
    }

    fn child(&self, i: VertexId, j: usize) -> &(Option<SerializedEdge>, VertexId) {
        let line = &self[i];
        if j < line.n_children {
            &self.children[self[i].children + j]
        } else {
            &line.extra[j - line.n_children]
        }
    }
}

pub(crate) fn remove_redundant_children(
    graph: &Graph,
    vids: &HashMap<Vertex<ChangeId>, crate::alive::VertexId>,
    vertices: &mut HashSet<Vertex<ChangeId>>,
    target: Vertex<ChangeId>,
) {
    let mut min = std::usize::MAX;
    let mut stack = Vec::new();
    for p in vertices.iter() {
        let vid = if let Some(vid) = vids.get(p) {
            *vid
        } else {
            continue;
        };
        min = min.min(graph[vid].scc);
        stack.push(vid);
    }
    let target_scc = if let Some(&target) = vids.get(&target) {
        graph[target].scc
    } else {
        std::usize::MAX
    };
    let mut visited = HashSet::default();
    while let Some(p) = stack.pop() {
        if !visited.insert(p) {
            continue;
        }
        for &(_, child) in graph.children(p) {
            if graph[p].scc < target_scc && graph[p].scc != graph[child].scc {
                assert!(graph[p].scc > graph[child].scc);
                vertices.remove(&graph[child].vertex);
            }
            if graph[child].scc >= min {
                stack.push(child);
            }
        }
    }
}

pub(crate) fn remove_redundant_parents(
    graph: &Graph,
    vids: &HashMap<Vertex<ChangeId>, crate::alive::VertexId>,
    vertices: &mut HashSet<Vertex<ChangeId>>,
    covered: &mut HashSet<(Vertex<ChangeId>, Vertex<ChangeId>)>,
    target: Vertex<ChangeId>,
) {
    let mut min = std::usize::MAX;
    let mut stack = Vec::new();
    for p in vertices.iter() {
        let vid = if let Some(vid) = vids.get(p) {
            *vid
        } else {
            continue;
        };
        min = min.min(graph[vid].scc);
        stack.push((vid, false));
    }
    stack.sort_by(|(a, _), (b, _)| graph[*a].scc.cmp(&graph[*b].scc));
    let target_scc = if let Some(&target) = vids.get(&target) {
        graph[target].scc
    } else {
        0
    };
    let mut visited = HashSet::default();
    while let Some((p, _)) = stack.pop() {
        if !visited.insert(p) {
            continue;
        }
        if graph[p].scc > target_scc
            && (vertices.contains(&graph[p].vertex) || covered.contains(&(graph[p].vertex, target)))
        {
            for (pp, pp_on_path) in stack.iter() {
                if graph[*pp].scc != graph[p].scc && *pp_on_path {
                    vertices.remove(&graph[*pp].vertex);
                    covered.insert((graph[*pp].vertex, target));
                }
            }
        }
        stack.push((p, true));
        for &(_, child) in graph.children(p) {
            if graph[child].scc >= min {
                stack.push((child, false));
            }
            if graph[p].scc > target_scc
                && graph[child].scc != graph[p].scc
                && covered.contains(&(graph[child].vertex, target))
            {
                assert!(graph[child].scc < graph[p].scc);
                vertices.remove(&graph[p].vertex);
                covered.insert((graph[p].vertex, target));
            }
        }
    }
}
