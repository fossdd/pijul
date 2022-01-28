use crate::pristine::*;
use crate::{HashMap, HashSet};

/// The following is an unrolled DFS, where each alive vertex is
/// inserted into each "alive set" along the current path (which is
/// recognised by looking at the visited vertices on the stack).
pub(crate) fn find_alive_down<'a, T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    vertex0: Vertex<ChangeId>,
    cache: &'a mut HashMap<Vertex<ChangeId>, Option<HashSet<Vertex<ChangeId>>>>,
) -> Result<&'a Option<HashSet<Vertex<ChangeId>>>, BlockError<T::GraphError>> {
    let mut stack: Vec<(_, Option<HashSet<Vertex<ChangeId>>>)> = vec![(
        SerializedEdge::empty(vertex0.start_pos(), ChangeId::ROOT),
        None,
    )];
    let mut visited = HashSet::default();
    while let Some((elt, alive)) = stack.pop() {
        if let Some(alive) = alive {
            // We've gone through all the descendants, put this in the
            // cache.
            let vertex = txn.find_block(&channel, elt.dest())?;
            cache.insert(*vertex, Some(alive.clone()));
            if stack.is_empty() {
                // Done!
                assert_eq!(vertex0.start_pos(), vertex.start_pos());
                return Ok(cache.get(&vertex).unwrap());
            }
            continue;
        } else {
            if !visited.insert(elt.dest()) {
                continue;
            }
            stack.push((elt, Some(HashSet::new())));
        }
        let vertex = txn.find_block(&channel, elt.dest())?;
        if let Some(c) = cache.get(vertex) {
            for st in stack.iter_mut() {
                if let Some(ref mut st) = st.1 {
                    if let Some(c) = c {
                        st.extend(c.iter().cloned());
                    } else {
                        st.insert(*vertex);
                    }
                }
            }
            continue;
        }
        debug!("elt = {:?}, vertex = {:?}", elt, vertex);
        let elt_index = stack.len();
        for v in iter_adj_all(txn, &channel, *vertex)? {
            let v = v?;
            if v.flag().contains(EdgeFlags::FOLDER) {
                continue;
            }
            debug!("v = {:?}", v);
            if v.flag().contains(EdgeFlags::PARENT) {
                if (v.flag().contains(EdgeFlags::BLOCK) || vertex.is_empty())
                    && !v.flag().contains(EdgeFlags::DELETED)
                    && !v.flag().contains(EdgeFlags::PSEUDO)
                {
                    if *vertex == vertex0 {
                        // vertex0 is alive.
                        stack.truncate(elt_index);
                        let (_, alive) = stack.pop().unwrap();
                        let alive = alive.unwrap();
                        assert!(alive.is_empty());
                        cache.insert(vertex0, None);
                        return Ok(cache.get(&vertex0).unwrap());
                    } else {
                        // vertex is alive, insert it into all the
                        // alive sets on the current DFS path
                        // (including `vertex`).
                        for st in stack.iter_mut() {
                            if let Some(ref mut st) = st.1 {
                                st.insert(*vertex);
                            }
                        }
                        stack.truncate(elt_index);
                        break;
                    }
                }
            } else {
                stack.push((*v, None))
            }
        }
    }
    unreachable!()
}

pub fn find_alive_up<'a, T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    files: &mut HashSet<Vertex<ChangeId>>,
    vertex0: Vertex<ChangeId>,
    change: ChangeId,
    cache: &'a mut HashMap<
        Vertex<ChangeId>,
        (Option<HashSet<Vertex<ChangeId>>>, HashSet<Vertex<ChangeId>>),
    >,
) -> Result<&'a Option<HashSet<Vertex<ChangeId>>>, BlockError<T::GraphError>> {
    debug!("find alive up: {:?}", vertex0);
    let mut stack: Vec<(
        _,
        Option<(HashSet<Vertex<ChangeId>>, HashSet<Vertex<ChangeId>>)>,
    )> = vec![(
        SerializedEdge::empty(vertex0.end_pos(), ChangeId::ROOT),
        None,
    )];
    let mut visited = HashSet::default();

    while let Some((elt, alive)) = stack.pop() {
        if elt.dest().is_root() {
            continue;
        }
        if let Some((alive, files_)) = alive {
            let vertex = *txn.find_block_end(&channel, elt.dest())?;
            cache.insert(vertex, (Some(alive), files_));
            if stack.is_empty() {
                // Done!
                assert_eq!(vertex.end_pos(), vertex0.end_pos());
                return Ok(&cache.get(&vertex).unwrap().0);
            }
            continue;
        } else {
            if !visited.insert(elt.dest()) {
                continue;
            }
            stack.push((elt, Some((HashSet::new(), HashSet::new()))));
        };
        let vertex = *txn.find_block_end(&channel, elt.dest())?;
        debug!("vertex = {:?}", vertex);
        if let Some((c, d)) = cache.get(&vertex) {
            debug!("Cached: {:?} {:?}", c, d);
            for st in stack.iter_mut() {
                if let Some((ref mut al, ref mut f)) = st.1 {
                    if let Some(c) = c {
                        al.extend(c.iter().cloned());
                    } else {
                        al.insert(vertex);
                    }
                    f.extend(d.iter().cloned());
                    files.extend(d.iter().cloned());
                }
            }
            continue;
        }
        debug!("find_alive_up: elt = {:?}, vertex = {:?}", elt, vertex);
        debug!("stack = {:?}", stack);
        let elt_index = stack.len();
        let mut is_file = false; // Is this the "inode" vertex of a file?
        let mut it = iter_adj_all(txn, &channel, vertex)?;
        while let Some(v) = it.next() {
            let v = v?;
            debug!("find_alive_up: v = {:?} change = {:?}", v, change);
            if !v.flag().is_parent() {
                is_file |= !v.flag().is_folder();
                continue;
            }
            if v.flag() & EdgeFlags::pseudof() == EdgeFlags::PSEUDO {
                continue;
            }
            if !v.flag().is_deleted() {
                if vertex == vertex0 {
                    // vertex0 is alive.
                    stack.truncate(elt_index);
                    let (_, alive) = stack.pop().unwrap();
                    let (alive, _) = alive.unwrap();
                    assert!(alive.is_empty());
                    cache.insert(vertex0, (None, HashSet::new()));
                    return Ok(&cache.get(&vertex0).unwrap().0);
                }
                if v.flag().is_folder() {
                    for e in it {
                        let e = e?;
                        is_file |= !e.flag().intersects(EdgeFlags::parent_folder())
                    }
                    if is_file {
                        debug!("is alive + is file {:?}", vertex);
                        for st in stack.iter_mut() {
                            if let Some((ref mut al, ref mut fi)) = st.1 {
                                al.insert(vertex);
                                fi.insert(vertex);
                            }
                        }
                        files.insert(vertex);
                    }
                    break;
                } else if v.flag().is_block() || vertex.is_empty() {
                    debug!("is alive {:?}", vertex);
                    for st in stack.iter_mut() {
                        if let Some((ref mut st, _)) = st.1 {
                            st.insert(vertex);
                        }
                    }
                    stack.truncate(elt_index);
                    break;
                }
            }
            if v.flag().is_folder() {
                if is_file {
                    debug!("is pseudo-alive folder {:?}", vertex);
                    for st in stack.iter_mut() {
                        if let Some((ref mut al, ref mut fi)) = st.1 {
                            al.insert(vertex);
                            fi.insert(vertex);
                        }
                    }
                    files.insert(vertex);
                }
                break;
            } else {
                stack.push((*v, None))
            }
        }
        debug!("is_file = {:?}", is_file);
    }
    unreachable!()
}
