use super::{Graph, VertexId};
use crate::pristine::*;
use crate::vector2::Vector2;
use crate::HashSet;

#[derive(Debug)]
pub(super) struct Path {
    pub path: Vec<PathElement>,
    pub sccs: HashSet<usize>,
    pub end: usize,
}

#[derive(Debug)]
pub(super) enum PathElement {
    Scc { scc: usize },
    Conflict { sides: Vec<Path> },
}

impl Path {
    fn new() -> Self {
        Path {
            path: Vec::new(),
            sccs: HashSet::default(),
            end: 0,
        }
    }
}

struct DFS {
    visits: Vec<Visits>,
    counter: usize,
}

#[derive(Clone, Debug)]
struct Visits {
    first: usize,
    last: usize,
}

impl DFS {
    pub fn new(n: usize) -> Self {
        DFS {
            visits: vec![Visits { first: 0, last: 0 }; n],
            counter: 1,
        }
    }
}

#[derive(Debug)]
struct State {
    n_scc: usize,
    descendants: Option<(usize, usize)>,
    current_path: Path,
    current_path_len: usize,
    return_values: Vec<Path>,
}

fn init_stack(n: usize) -> Vec<State> {
    let mut stack = Vec::with_capacity(n);
    stack.push(State {
        n_scc: n - 1,
        descendants: None,
        current_path: Path::new(),
        current_path_len: 0,
        return_values: Vec::new(),
    });
    stack
}

#[test]
fn test4165() {
    env_logger::try_init().unwrap_or(());
    use super::*;
    use crate::pristine::*;
    let mut graph = Graph {
        lines: vec![
            AliveVertex {
                vertex: Vertex {
                    change: ChangeId(1u64.into()),
                    start: ChangePosition(0u64.into()),
                    end: ChangePosition(1u64.into())
                },
                flags: super::Flags::empty(),
                children: 0,
                n_children: 0,
                index: 0,
                lowlink: 0,
                scc: 0,
                extra: Vec::new(),
            };
            13
        ],
        children: Vec::new(),
        total_bytes: 0,
    };
    for i in 0..13 {
        graph.lines[i].vertex.change = ChangeId((i as u64).into());
    }
    for (i, &children) in [
        &[][..],
        &[4, 2, 6, 7, 5, 3][..],
        &[12, 4, 7, 6][..],
        &[5][..],
        &[10, 8, 9][..],
        &[6, 7, 4][..],
        &[4, 7][..],
        &[4][..],
        &[10][..],
        &[10][..],
        &[11][..],
        &[0][..],
        &[4][..],
    ]
    .iter()
    .enumerate()
    {
        graph.lines[i].children = graph.children.len();
        graph.lines[i].n_children = children.len();
        for &chi in children.iter() {
            graph.children.push((
                Some(
                    (Edge {
                        dest: graph.lines[chi].vertex.start_pos(),
                        flag: EdgeFlags::empty(),
                        introduced_by: ChangeId(4165u64.into()),
                    })
                    .into(),
                ),
                VertexId(chi),
            ))
        }
    }
    let scc = graph.tarjan();
    for i in 0..scc.len() {
        for &j in scc[i].iter() {
            graph[j].scc = i
        }
    }
    let mut f = std::fs::File::create("debug4165").unwrap();
    graph.debug_raw(&mut f).unwrap();
    println!("{:#?}", graph.dfs(&scc))
}

impl Graph {
    pub(super) fn dfs(&mut self, scc: &Vector2<VertexId>) -> (Path, HashSet<(usize, usize)>) {
        let mut dfs = DFS::new(scc.len());
        let mut stack = init_stack(scc.len());
        let mut forward_scc = HashSet::default();
        let mut regular_scc = HashSet::default();
        let mut return_value = None;
        let mut descendants = Vector2::with_capacities(scc.len(), scc.len());
        'recursion: while let Some(mut state) = stack.pop() {
            debug!("dfs state = {:?}", state);
            let (i, mut j) = if let Some(n) = state.descendants {
                n
            } else {
                first_visit(self, &mut dfs, scc, &mut descendants, &mut state)
            };
            debug!("i = {:?}, j = {:?}", i, j);
            let scc_vertices: Vec<_> = scc[state.n_scc].iter().map(|x| &self[*x]).collect();
            debug!("scc_vertices = {:?}", scc_vertices);
            while j > 0 {
                let child = descendants[i][j - 1];
                let scc_child: Vec<_> = scc[child].iter().map(|x| &self[*x]).collect();
                debug!("dfs child = {:?} {:?}", child, scc_child);

                if dfs.visits[state.n_scc].first < dfs.visits[child].first {
                    // This is a forward edge.
                    if child > 0 && !regular_scc.contains(&(state.n_scc, child)) {
                        debug!("forward edge");
                        forward_scc.insert((state.n_scc, child));
                    }
                } else if dfs.visits[child].first == 0 {
                    // Regular edge.
                    regular_scc.insert((state.n_scc, child));
                    debug!("regular edge, return_value {:?}", return_value);
                    if let Some(return_value) = return_value.take() {
                        state.return_values.push(return_value)
                    }
                    recurse(state, (i, j), child, &mut stack);
                    continue 'recursion;
                } else {
                    // Cross edge.
                    regular_scc.insert((state.n_scc, child));
                    debug!("cross edge");
                }
                j -= 1
            }
            return_value = Some(if let Some(return_value_) = return_value.take() {
                dfs.visits[state.n_scc].last = dfs.counter;
                dfs.counter += 1;
                if state.return_values.is_empty() {
                    return_value_
                } else {
                    state.return_values.push(return_value_);
                    make_conflict(&mut state)
                }
            } else {
                state.current_path
            });
            debug!("end of loop, returning {:?}", return_value);
        }
        (return_value.unwrap_or_else(Path::new), forward_scc)
    }
}

fn first_visit(
    graph: &Graph,
    dfs: &mut DFS,
    scc: &Vector2<VertexId>,
    descendants: &mut Vector2<usize>,
    state: &mut State,
) -> (usize, usize) {
    assert_eq!(dfs.visits[state.n_scc].first, 0);
    dfs.visits[state.n_scc].first = dfs.counter;
    dfs.counter += 1;
    state
        .current_path
        .path
        .push(PathElement::Scc { scc: state.n_scc });
    state.current_path.sccs.insert(state.n_scc);
    let i = descendants.len();
    descendants.push();
    let mut descendants_end = 0;
    for cousin in scc[state.n_scc].iter() {
        for &(_, n_child) in graph.children(*cousin) {
            let child_component = graph[n_child].scc;
            if child_component > state.n_scc {
                panic!("{} > {}", child_component, state.n_scc);
            } else if child_component == state.n_scc {
                debug!("cyclic component {:?}", child_component);
                continue;
            }
            if dfs.visits[child_component].first == 0 {
                descendants.push_to_last(child_component)
            } else {
                descendants_end = descendants_end.max(child_component);
            }
        }
    }
    state.current_path.end = descendants_end;
    let d = descendants.last_mut().unwrap();
    d.sort_unstable();
    debug!(
        "first visit, n_scc = {:?}, state.current_path = {:?}, descendants = {:?}",
        state.n_scc, state.current_path, d
    );
    (i, d.len())
}

fn recurse(mut state: State, (i, j): (usize, usize), child: usize, stack: &mut Vec<State>) {
    let current_path = std::mem::replace(&mut state.current_path, Path::new());
    let len = stack.len();
    stack.push(State {
        descendants: Some((i, j - 1)),
        ..state
    });
    stack.push(State {
        n_scc: child,
        descendants: None,
        current_path_len: current_path.path.len(),
        current_path,
        return_values: Vec::new(),
    });
    debug!("recursing {:?}", &stack[len..]);
}

fn make_conflict(state: &mut State) -> Path {
    let mut main_path = state.return_values[0]
        .path
        .split_off(state.current_path_len + 1);
    std::mem::swap(&mut state.return_values[0].path, &mut main_path);
    debug!(
        "make_conflict {:#?} {:#?}",
        state.return_values[0].path, main_path
    );
    state.return_values.sort_by(|a, b| a.end.cmp(&b.end));
    let sccs = state
        .return_values
        .iter()
        .flat_map(|side| side.sccs.iter())
        .copied()
        .collect();
    let mut conflict_sides = Vec::new();
    while let Some(side) = state.return_values.pop() {
        debug!("side = {:#?}", side);
        let main_side = if let Some(n) = state
            .return_values
            .iter()
            .position(|side_| side_.sccs.contains(&side.end))
        {
            n
        } else {
            conflict_sides.push(side);
            continue;
        };
        if let PathElement::Conflict { ref mut sides, .. } = state.return_values[main_side].path[0]
        {
            if sides[0].end == side.end {
                sides.push(side);
                continue;
            }
        }
        create_nested_conflict(&mut state.return_values[main_side], side);
    }
    if conflict_sides.len() > 1 {
        main_path.push(PathElement::Conflict {
            sides: conflict_sides,
        })
    } else {
        main_path.extend(conflict_sides.pop().unwrap().path.into_iter())
    }
    Path {
        path: main_path,
        sccs,
        end: 0,
    }
}

fn create_nested_conflict(main_side: &mut Path, side: Path) {
    let end = main_side
        .path
        .iter()
        .position(|v| match v {
            PathElement::Scc { ref scc } => *scc == side.end,
            PathElement::Conflict { ref sides } => {
                sides.iter().any(|side_| side_.sccs.contains(&side.end))
            }
        })
        .unwrap();
    let mut v = vec![PathElement::Conflict { sides: Vec::new() }];
    v.extend(main_side.path.drain(end..));
    let side0 = std::mem::replace(&mut main_side.path, v);
    let mut sccs0 = HashSet::default();
    for elt in side0.iter() {
        match *elt {
            PathElement::Scc { scc } => {
                sccs0.insert(scc);
            }
            PathElement::Conflict { ref sides } => {
                for side in sides {
                    for &scc in side.sccs.iter() {
                        sccs0.insert(scc);
                    }
                }
            }
        }
    }
    main_side.sccs.extend(side.sccs.iter().copied());
    main_side.path[0] = PathElement::Conflict {
        sides: vec![
            Path {
                path: side0,
                sccs: sccs0,
                end: side.end,
            },
            side,
        ],
    };
}

impl Graph {
    pub(super) fn collect_forward_edges<T: GraphTxnT>(
        &self,
        txn: &T,
        channel: &T::Graph,
        scc: &Vector2<VertexId>,
        forward_scc: &HashSet<(usize, usize)>,
        forward: &mut Vec<super::Redundant>,
    ) -> Result<(), TxnErr<T::GraphError>> {
        for &(a, b) in forward_scc.iter() {
            for cousin in scc[a].iter() {
                for &(edge, n_child) in self.children(*cousin) {
                    if self[n_child].scc != b {
                        continue;
                    }
                    if let Some(edge) = edge {
                        if edge.flag().contains(EdgeFlags::PSEUDO)
                            && !crate::pristine::test_edge(
                                txn,
                                channel,
                                Position {
                                    change: self[*cousin].vertex.change,
                                    pos: self[*cousin].vertex.start,
                                },
                                edge.dest(),
                                EdgeFlags::DELETED,
                                EdgeFlags::DELETED,
                            )?
                        {
                            forward.push(super::Redundant {
                                v: self[*cousin].vertex,
                                e: edge,
                            })
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
