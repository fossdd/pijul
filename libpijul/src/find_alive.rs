use crate::pristine::*;
use crate::{HashMap, HashSet};
use std::cell::RefCell;
use std::rc::Rc;

type Alive = Rc<RefCell<HashSet<Vertex<ChangeId>>>>;

pub(crate) fn find_alive_down<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    vertex0: Vertex<ChangeId>,
    cache: &mut HashMap<Vertex<ChangeId>, Alive>,
) -> Result<Alive, BlockError<T::GraphError>> {
    let mut stack = vec![SerializedEdge::empty(vertex0.start_pos(), ChangeId::ROOT)];
    let mut visited = HashSet::default();
    let alive = Rc::new(RefCell::new(HashSet::new()));
    while let Some(elt) = stack.pop() {
        if !visited.insert(elt.dest()) {
            continue;
        }
        let vertex = txn.find_block(&channel, elt.dest())?;
        if let Some(c) = cache.get(vertex) {
            alive.borrow_mut().extend(c.borrow().iter().cloned());
            continue;
        } else {
            cache.insert(*vertex, alive.clone());
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
                        assert!(alive.borrow().is_empty());
                        return Ok(alive);
                    } else {
                        alive.borrow_mut().insert(*vertex);
                        stack.truncate(elt_index);
                        break;
                    }
                }
            } else {
                stack.push(*v)
            }
        }
    }
    Ok(alive)
}

pub fn find_alive_up<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    files: &mut HashSet<Vertex<ChangeId>>,
    vertex0: Vertex<ChangeId>,
    change: ChangeId,
    cache: &mut HashMap<Vertex<ChangeId>, (Alive, Alive)>,
) -> Result<Alive, BlockError<T::GraphError>> {
    debug!("find alive up: {:?}", vertex0);
    let alive = Rc::new(RefCell::new(HashSet::default()));
    let files_ = Rc::new(RefCell::new(HashSet::default()));
    let mut stack = vec![SerializedEdge::empty(vertex0.end_pos(), ChangeId::ROOT)];
    let mut visited = HashSet::default();

    while let Some(elt) = stack.pop() {
        if elt.dest().is_root() {
            continue;
        }
        if !visited.insert(elt.dest()) {
            continue;
        }
        let vertex = *txn.find_block_end(&channel, elt.dest())?;
        debug!("vertex = {:?}", vertex);
        let is_cached = if let Some((c, f)) = cache.get(&vertex) {
            alive.borrow_mut().extend(c.borrow().iter().cloned());
            files_.borrow_mut().extend(f.borrow().iter().cloned());
            files.extend(f.borrow().iter().cloned());
            // We're not continuing here, since the while loop below
            // needs to insert stuff into `files` and `files_`.
            true
        } else {
            cache.insert(vertex, (alive.clone(), files_.clone()));
            false
        };
        debug!("find_alive_up: elt = {:?}, vertex = {:?}", elt, vertex);
        debug!("stack = {:?}", stack);
        let elt_index = stack.len();
        let mut is_file = false;
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
                if v.flag().is_folder() {
                    for e in it {
                        let e = e?;
                        is_file |= !e.flag().intersects(EdgeFlags::parent_folder())
                    }
                    if is_file && vertex != vertex0 {
                        debug!("is alive + is file {:?}", vertex);
                        alive.borrow_mut().insert(vertex);
                        files_.borrow_mut().insert(vertex);
                        files.insert(vertex);
                    }
                    break;
                } else if v.flag().is_block() || vertex.is_empty() {
                    if vertex != vertex0 {
                        debug!("is alive {:?}", vertex);
                        alive.borrow_mut().insert(vertex);
                    }
                    stack.truncate(elt_index);
                    break;
                }
            }
            if v.flag().is_folder() {
                if is_file && vertex != vertex0 {
                    debug!("is alive {:?}", vertex);
                    alive.borrow_mut().insert(vertex);
                    files_.borrow_mut().insert(vertex);
                    files.insert(vertex);
                }
                break;
            } else if !is_cached {
                stack.push(*v)
            }
        }
        debug!("is_file = {:?}", is_file);
    }
    Ok(alive)
}
