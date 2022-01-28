use super::{Graph, VertexId};
use crate::changestore::*;
use crate::pristine::{Base32, GraphTxnT, Position};
use crate::{HashMap, HashSet};
use std::io::Write;

impl Graph {
    /// Write a graph to an `std::io::Write` in GraphViz (dot) format.
    #[allow(dead_code)]
    pub fn debug<W: Write, T: GraphTxnT, P: ChangeStore>(
        &self,
        changes: &P,
        txn: &T,
        channel: &T::Graph,
        add_others: bool,
        introduced_by: bool,
        mut w: W,
    ) -> Result<(), std::io::Error> {
        writeln!(w, "digraph {{")?;
        let mut buf = Vec::new();
        let mut cache = HashMap::default();
        if add_others {
            for (line, i) in self.lines.iter().zip(0..) {
                cache.insert(
                    Position {
                        change: line.vertex.change,
                        pos: line.vertex.start,
                    },
                    i,
                );
            }
        }
        let mut others = HashSet::default();
        for (line, i) in self.lines.iter().zip(0..) {
            buf.resize(line.vertex.end - line.vertex.start, 0);
            changes
                .get_contents(
                    |h| txn.get_external(&h).unwrap().map(|x| x.into()),
                    line.vertex,
                    &mut buf,
                )
                .unwrap();
            let contents = &buf;
            // Produce an escaped string.
            let contents = format!(
                "{:?}",
                if let Ok(contents) = std::str::from_utf8(contents) {
                    contents.chars().take(100).collect()
                } else {
                    "<INVALID UTF8>".to_string()
                }
            );
            // Remove the quotes around the escaped string.
            let contents = contents.split_at(contents.len() - 1).0.split_at(1).1;
            writeln!(
                w,
                "n_{}[label=\"{}({}): {}.[{};{}[: {}\"];",
                i,
                i,
                line.scc,
                line.vertex.change.to_base32(),
                line.vertex.start.0,
                line.vertex.end.0,
                contents
            )?;

            if add_others && !line.vertex.is_root() {
                for v in crate::pristine::iter_adj_all(txn, &channel, line.vertex).unwrap() {
                    let v = v.unwrap();
                    if let Some(dest) = cache.get(&v.dest()) {
                        writeln!(
                            w,
                            "n_{} -> n_{}[color=red,label=\"{:?}{}{}\"];",
                            i,
                            dest,
                            v.flag().bits(),
                            if introduced_by { " " } else { "" },
                            if introduced_by {
                                v.introduced_by().to_base32()
                            } else {
                                String::new()
                            }
                        )?;
                    } else {
                        if !others.contains(&v.dest()) {
                            others.insert(v.dest());
                            writeln!(
                                w,
                                "n_{}_{}[label=\"{}.{}\",color=red];",
                                v.dest().change.to_base32(),
                                v.dest().pos.0,
                                v.dest().change.to_base32(),
                                v.dest().pos.0
                            )?;
                        }
                        writeln!(
                            w,
                            "n_{} -> n_{}_{}[color=red,label=\"{:?}{}{}\"];",
                            i,
                            v.dest().change.to_base32(),
                            v.dest().pos.0,
                            v.flag().bits(),
                            if introduced_by { " " } else { "" },
                            if introduced_by {
                                v.introduced_by().to_base32()
                            } else {
                                String::new()
                            }
                        )?;
                    }
                }
            }
            for &(edge, VertexId(j)) in (self.children
                [line.children..line.children + line.n_children])
                .iter()
                .chain(line.extra.iter())
            {
                if let Some(ref edge) = edge {
                    writeln!(
                        w,
                        "n_{}->n_{}[label=\"{:?}{}{}\"];",
                        i,
                        j,
                        edge.flag().bits(),
                        if introduced_by { " " } else { "" },
                        if introduced_by {
                            edge.introduced_by().to_base32()
                        } else {
                            String::new()
                        }
                    )?
                } else {
                    writeln!(w, "n_{}->n_{}[label=\"none\"];", i, j)?
                }
            }
        }
        writeln!(w, "}}")?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn debug_raw<W: Write>(&self, mut w: W) -> Result<(), std::io::Error> {
        writeln!(w, "digraph {{")?;
        for (line, i) in self.lines.iter().zip(0..) {
            // Remove the quotes around the escaped string.
            writeln!(
                w,
                "n_{}[label=\"{}(scc {}): {}.[{};{}[\"];",
                i,
                i,
                line.scc,
                line.vertex.change.to_base32(),
                line.vertex.start.0,
                line.vertex.end.0,
            )?;

            for &(edge, VertexId(j)) in self.children
                [line.children..line.children + line.n_children]
                .iter()
                .chain(line.extra.iter())
            {
                if let Some(ref edge) = edge {
                    writeln!(
                        w,
                        "n_{}->n_{}[label=\"{:?} {}\"];",
                        i,
                        j,
                        edge.flag().bits(),
                        edge.introduced_by().to_base32()
                    )?
                } else {
                    writeln!(w, "n_{}->n_{}[label=\"none\"];", i, j)?
                }
            }
        }
        writeln!(w, "}}")?;
        Ok(())
    }
}
