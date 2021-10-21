use super::{Flags, Graph, VertexId};
use crate::vector2::*;
use std::cmp::min;
impl Graph {
    pub(crate) fn tarjan(&mut self) -> Vector2<VertexId> {
        if self.lines.len() <= 1 {
            let mut sccs = Vector2::with_capacities(self.lines.len(), self.lines.len());
            sccs.push();
            sccs.push_to_last(VertexId(0));
            return sccs;
        }
        let mut call_stack = vec![(VertexId(1), 0, true)];

        let mut index = 0;
        let mut stack = Vec::new();
        let mut scc = Vector2::new();
        'recursion: while let Some((n_l, i, first_visit)) = call_stack.pop() {
            if first_visit {
                let l = &mut self[n_l];
                l.index = index;
                l.lowlink = index;
                l.flags = l.flags | Flags::ONSTACK | Flags::VISITED;
                stack.push(n_l);
                index += 1;
            } else {
                let &(_, n_child) = self.child(n_l, i);
                self[n_l].lowlink = self[n_l].lowlink.min(self[n_child].lowlink);
            }

            for j in i..self[n_l].n_children + self[n_l].extra.len() {
                let n_child = if j < self[n_l].n_children {
                    self.child(n_l, j).1
                } else {
                    self[n_l].extra[j - self[n_l].n_children].1
                };
                if !self[n_child].flags.contains(Flags::VISITED) {
                    call_stack.push((n_l, j, false));
                    call_stack.push((n_child, 0, true));
                    continue 'recursion;
                } else if self[n_child].flags.contains(Flags::ONSTACK) {
                    self[n_l].lowlink = min(self[n_l].lowlink, self[n_child].index)
                }
            }

            if self[n_l].index == self[n_l].lowlink {
                let n_scc = scc.len();
                scc.push();
                loop {
                    match stack.pop() {
                        None => break,
                        Some(n_p) => {
                            self[n_p].scc = n_scc;
                            self[n_p].flags ^= Flags::ONSTACK;
                            scc.push_to_last(n_p);
                            if n_p == n_l {
                                break;
                            }
                        }
                    }
                }
            }
        }
        scc
    }
}
