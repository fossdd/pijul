use crate::change::*;
use crate::changestore::*;
use crate::pristine::*;
use crate::small_string::*;

pub fn undo_file_addition<T: TreeMutTxnT>(
    txn: &mut T,
    change_id: ChangeId,
    new_vertex: &NewVertex<Option<Hash>>,
) -> Result<(), TreeErr<T::TreeError>> {
    if new_vertex.start == new_vertex.end {
        let pos = Position {
            change: change_id,
            pos: new_vertex.start,
        };
        if let Some(&inode) = txn.get_revinodes(&pos, None)? {
            del_inodes_with_rev(txn, &inode, &pos)?;
        }
    }
    Ok(())
}

pub fn undo_file_deletion<T: ChannelTxnT + TreeMutTxnT + TreeTxnT, P: ChangeStore>(
    txn: &mut T,
    changes: &P,
    channel: &T::Channel,
    change_id: ChangeId,
    newedges: &EdgeMap<Option<Hash>>,
    salt: u64,
) -> Result<(), super::UnrecordError<P::Error, T>> {
    for e in newedges.edges.iter().rev() {
        assert!(!e.flag.contains(EdgeFlags::PARENT));
        let source =
            *txn.find_block_end(txn.graph(channel), internal_pos(txn, &e.from, change_id)?)?;
        if e.flag.contains(EdgeFlags::FOLDER) && e.to.start_pos() == e.to.end_pos() {
            let dest = internal_pos(txn, &e.to.start_pos(), change_id)?;
            restore(txn, changes, channel, source, dest, salt)?
        }
    }
    Ok(())
}

fn restore<T: ChannelTxnT + TreeMutTxnT + TreeTxnT, P: ChangeStore>(
    txn: &mut T,
    changes: &P,
    channel: &T::Channel,
    source: Vertex<ChangeId>,
    dest: Position<ChangeId>,
    salt: u64,
) -> Result<(), super::UnrecordError<P::Error, T>> {
    let mut stack = vec![(source, dest)];
    let mut return_value = Some(Inode::ROOT);
    while let Some((source, dest)) = stack.pop() {
        debug!("source = {:?}, dest = {:?}", source, dest);
        if let Some(parent_inode) = return_value {
            if parent_inode != Inode::ROOT {
                return_value = restore_inode(txn, changes, source, dest, parent_inode, salt)?;
                continue;
            }
        } else {
            continue;
        }
        let mut source_parent = None;
        for e in iter_adjacent(
            txn,
            txn.graph(channel),
            source,
            EdgeFlags::PARENT | EdgeFlags::FOLDER,
            EdgeFlags::all(),
        )? {
            let e = e?;
            if e.flag().contains(EdgeFlags::PARENT | EdgeFlags::FOLDER) {
                source_parent = Some(e.dest());
                break;
            }
        }
        let source_parent = source_parent.unwrap();

        if source_parent.change.is_root() {
            return_value = restore_inode(txn, changes, source, dest, Inode::ROOT, salt)?;
        } else if let Some(&inode) = txn.get_revinodes(&source_parent, None)? {
            return_value = restore_inode(txn, changes, source, dest, inode, salt)?;
        } else {
            let grandparent = find_youngest_parent(txn, channel, source_parent.inode_vertex())?;
            if grandparent.is_root() || grandparent.start == grandparent.end {
                return_value = restore_inode(txn, changes, source, dest, Inode::ROOT, salt)?;
            } else {
                stack.push((source, dest));
                stack.push((grandparent, source_parent));
            }
        }
    }
    Ok(())
}

fn restore_inode<T: TreeMutTxnT + GraphTxnT + TreeTxnT, P: ChangeStore>(
    txn: &mut T,
    changes: &P,
    source: Vertex<ChangeId>,
    dest: Position<ChangeId>,
    parent_inode: Inode,
    salt: u64,
) -> Result<Option<Inode>, super::UnrecordError<P::Error, T>> {
    let mut name = vec![0; source.end - source.start];
    let FileMetadata {
        basename, metadata, ..
    } = changes
        .get_file_meta(
            |h| txn.get_external(&h).unwrap().map(From::from),
            source,
            &mut name,
        )
        .map_err(super::UnrecordError::Changestore)?;
    let basename = SmallString::from_str(basename);
    if let Some(inode) = txn.get_revinodes(&dest, None)? {
        Ok(Some(*inode))
    } else {
        let file_id = OwnedPathId {
            parent_inode,
            basename,
        };
        if txn.get_tree(&file_id, None)?.is_none() {
            let inode = crate::fs::create_new_inode(txn, &file_id, salt)?;
            put_tree_with_rev(txn, &file_id, &inode)?;
            put_inodes_with_rev(txn, &inode, &dest)?;
            if metadata.is_dir() {
                let id = OwnedPathId {
                    parent_inode: inode,
                    basename: SmallString::new(),
                };
                txn.put_tree(&id, &inode)?;
            }
            Ok(Some(inode))
        } else {
            Ok(None)
        }
    }
}

fn find_youngest_parent<T: ChannelTxnT>(
    txn: &T,
    channel: &T::Channel,
    current: Vertex<ChangeId>,
) -> Result<Vertex<ChangeId>, BlockError<T::GraphError>> {
    let mut next = None;
    for e in iter_adjacent(
        txn,
        txn.graph(channel),
        current,
        EdgeFlags::FOLDER | EdgeFlags::PARENT,
        EdgeFlags::FOLDER | EdgeFlags::PARENT | EdgeFlags::DELETED | EdgeFlags::BLOCK,
    )? {
        let e = e?;
        if !e.flag().contains(EdgeFlags::FOLDER | EdgeFlags::PARENT) {
            continue;
        }
        if e.flag().contains(EdgeFlags::DELETED) {
            debug!("deleted: {:?}", e.introduced_by());
            let age = txn
                .get_changeset(txn.changes(channel), &e.introduced_by())?
                .unwrap();
            if let Some((ref mut age0, ref mut v)) = next {
                if age > *age0 {
                    *age0 = age;
                    *v = e.dest()
                }
            } else {
                next = Some((age, e.dest()))
            }
        } else {
            next = Some((&L64(0), e.dest()));
            break;
        }
    }
    Ok(*txn.find_block_end(txn.graph(channel), next.unwrap().1)?)
}

pub fn undo_file_reinsertion<
    P: ChangeStore,
    T: GraphTxnT + TreeMutTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>,
>(
    txn: &mut T,
    change_id: ChangeId,
    newedges: &EdgeMap<Option<Hash>>,
) -> Result<(), super::UnrecordError<P::Error, T>> {
    for e in newedges.edges.iter() {
        assert!(!e.flag.contains(EdgeFlags::PARENT));
        if e.to.start_pos() == e.to.end_pos() {
            let position = internal_pos(txn, &e.to.start_pos(), change_id)?;
            if let Some(&inode) = txn.get_revinodes(&position, None)? {
                del_inodes_with_rev(txn, &inode, &position)?;
            }
        }
    }
    Ok(())
}
