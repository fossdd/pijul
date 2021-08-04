//! Output the pristine to the working copy, synchronising file
//! changes (file additions, deletions and renames) in the process.
use super::{collect_children, OutputError, OutputItem, PristineOutputError};
use crate::alive::retrieve;
use crate::changestore::ChangeStore;
use crate::fs::{create_new_inode, inode_filename};
use crate::pristine::*;
use crate::small_string::SmallString;
use crate::working_copy::WorkingCopy;
use crate::{alive, path, vertex_buffer};
use crate::{HashMap, HashSet};

use std::collections::hash_map::Entry;
use std::sync::Arc;

/// A structure representing a file with conflicts.
#[derive(Debug, PartialEq, Eq)]
pub enum Conflict {
    Name {
        path: String,
    },
    ZombieFile {
        path: String,
    },
    MultipleNames {
        pos: Position<ChangeId>,
        path: String,
    },
    Zombie {
        path: String,
        line: usize,
    },
    Cyclic {
        path: String,
        line: usize,
    },
    Order {
        path: String,
        line: usize,
    },
}

/// Output updates the working copy after applying changes, including
/// the graph-file correspondence.
///
/// **WARNING:** This overwrites the working copy, cancelling any
/// unrecorded change.
pub fn output_repository_no_pending<
    T: MutTxnT + Send + Sync + 'static,
    R: WorkingCopy + Send + Clone + Sync + 'static,
    P: ChangeStore + Send + Clone + 'static,
>(
    repo: &R,
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    prefix: &str,
    output_name_conflicts: bool,
    if_modified_since: Option<std::time::SystemTime>,
    n_workers: usize,
    salt: u64,
) -> Result<Vec<Conflict>, OutputError<P::Error, T::GraphError, R::Error>>
where
    T::Channel: Send + Sync + 'static,
{
    output_repository(
        repo,
        changes,
        txn.clone(),
        channel.clone(),
        ChangeId::ROOT,
        &mut crate::path::components(prefix),
        output_name_conflicts,
        if_modified_since,
        n_workers,
        salt,
    )
}

fn output_loop<
    T: TreeMutTxnT + ChannelMutTxnT + GraphMutTxnT<GraphError = <T as TreeTxnT>::TreeError>,
    R: WorkingCopy + Clone + 'static,
    P: ChangeStore + Clone + Send,
>(
    repo: &R,
    changes: &P,
    txn: ArcTxn<T>,
    channel: ChannelRef<T>,
    work: Arc<crossbeam_deque::Injector<(OutputItem, String, Option<String>)>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    t: usize,
) -> Result<Vec<Conflict>, OutputError<P::Error, T::GraphError, R::Error>> {
    use crossbeam_deque::*;
    // let backoff = crossbeam_utils::Backoff::new();
    // let w: Worker<(OutputItem, String)> = Worker::new_fifo();
    let mut conflicts = Vec::new();
    loop {
        match work.steal() {
            Steal::Success((item, path, tmp)) => {
                info!("Outputting {:?} (tmp {:?}), on thread {}", path, tmp, t);
                let path = tmp.as_deref().unwrap_or(&path);
                output_item::<_, _, R>(
                    txn.clone(),
                    channel.clone(),
                    changes,
                    &item,
                    &mut conflicts,
                    &repo,
                    path,
                )?;
                debug!("setting permissions for {:?}", path);
                repo.set_permissions(path, item.meta.permissions())
                    .map_err(OutputError::WorkingCopy)?;
                debug!("output {:?}", path);
            }
            Steal::Retry => {}
            Steal::Empty => {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
    Ok(conflicts)
}

fn output_repository<
    'a,
    T: TreeMutTxnT
        + ChannelMutTxnT
        + GraphMutTxnT<GraphError = <T as TreeTxnT>::TreeError>
        + Send
        + Sync
        + 'static,
    R: WorkingCopy + Clone + Send + Sync + 'static,
    P: ChangeStore + Send + Clone + 'static,
    I: Iterator<Item = &'a str>,
>(
    repo: &R,
    changes: &P,
    txn: ArcTxn<T>,
    channel: ChannelRef<T>,
    pending_change_id: ChangeId,
    prefix: &mut I,
    output_name_conflicts: bool,
    if_modified_after: Option<std::time::SystemTime>,
    n_workers: usize,
    salt: u64,
) -> Result<Vec<Conflict>, OutputError<P::Error, T::TreeError, R::Error>>
where
    T::Channel: Send + Sync + 'static,
{
    let work = Arc::new(crossbeam_deque::Injector::new());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut threads = Vec::new();
    for t in 0..n_workers - 1 {
        let repo = repo.clone();
        let work = work.clone();
        let stop = stop.clone();
        let txn = txn.clone();
        let channel = channel.clone();
        let changes = changes.clone();
        threads.push(std::thread::spawn(move || {
            output_loop(&repo, &changes, txn, channel, work, stop, t + 1)
        }))
    }

    let mut conflicts = Vec::new();
    let mut files = HashMap::default();
    let mut next_files = HashMap::default();
    let mut next_prefix_basename = prefix.next();
    let mut is_first_none = true;
    if next_prefix_basename.is_none() {
        let dead = {
            let txn_ = txn.read();
            let channel = channel.read();
            let graph = txn_.graph(&*channel);
            collect_dead_files(&*txn_, graph, pending_change_id, Inode::ROOT)?
        };
        debug!("dead (line {}) = {:?}", line!(), dead);
        if !dead.is_empty() {
            let mut txn = txn.write();
            kill_dead_files::<T, R, P>(&mut *txn, &channel, &repo, &dead)?;
        }
        is_first_none = false;
    }
    {
        let txn = txn.read();
        let channel = channel.read();
        collect_children(
            &*txn,
            &*changes,
            txn.graph(&*channel),
            Position::ROOT,
            Inode::ROOT,
            "",
            None,
            next_prefix_basename,
            &mut files,
        )?;
    }
    debug!("done collecting: {:?}", files);
    let mut done_inodes = HashSet::default();
    let mut done_vertices: HashMap<_, (Vertex<ChangeId>, String)> = HashMap::default();
    // Actual moves is used to avoid a situation where have two files
    // a and b, first rename a -> b, and then b -> c.
    let mut actual_moves = Vec::new();
    while !files.is_empty() {
        debug!("files {:?}", files.len());
        next_files.clear();
        next_prefix_basename = prefix.next();

        for (a, mut b) in files.drain() {
            debug!("files: {:?} {:?}", a, b);
            {
                let txn = txn.read();
                let channel = channel.read();
                b.sort_unstable_by(|u, v| {
                    txn.get_changeset(txn.changes(&channel), &u.0.change)
                        .unwrap()
                        .cmp(
                            &txn.get_changeset(txn.changes(&channel), &v.0.change)
                                .unwrap(),
                        )
                });
            }
            let mut is_first_name = true;
            for (name_key, mut output_item) in b {
                let name_entry = match done_vertices.entry(output_item.pos) {
                    Entry::Occupied(e) => {
                        debug!("pos already visited: {:?} {:?}", a, output_item.pos);
                        if e.get().0 != name_key {
                            conflicts.push(Conflict::MultipleNames {
                                pos: output_item.pos,
                                path: e.get().1.clone(),
                            });
                        }
                        continue;
                    }
                    Entry::Vacant(e) => e,
                };

                let output_item_inode = {
                    let txn = txn.read();
                    if let Some(inode) = txn.get_revinodes(&output_item.pos, None)? {
                        Some((*inode, *txn.get_inodes(inode, None)?.unwrap()))
                    } else {
                        None
                    }
                };

                if let Some((inode, _)) = output_item_inode {
                    if !done_inodes.insert(inode) {
                        debug!("inode already visited: {:?} {:?}", a, inode);
                        continue;
                    }
                }
                let name = if !is_first_name {
                    if output_name_conflicts {
                        let name = make_conflicting_name(&a, name_key);
                        conflicts.push(Conflict::Name { path: name.clone() });
                        name
                    } else {
                        debug!("not outputting {:?} {:?}", a, name_key);
                        conflicts.push(Conflict::Name {
                            path: a.to_string(),
                        });
                        break;
                    }
                } else {
                    is_first_name = false;
                    a.clone()
                };
                let file_name = path::file_name(&name).unwrap();
                path::push(&mut output_item.path, file_name);

                name_entry.insert((name_key, output_item.path.clone()));

                if let Some(ref mut tmp) = output_item.tmp {
                    path::push(tmp, file_name);
                }
                let path = std::mem::replace(&mut output_item.path, String::new());
                let mut tmp = output_item.tmp.take();
                let inode = move_or_create::<T, R, P>(
                    txn.clone(),
                    &repo,
                    &output_item,
                    output_item_inode,
                    &path,
                    &mut tmp,
                    &file_name,
                    &mut actual_moves,
                    salt,
                )?;
                debug!("inode = {:?}", inode);
                if next_prefix_basename.is_none() && is_first_none {
                    let dead = {
                        let txn_ = txn.read();
                        let channel = channel.read();
                        collect_dead_files(&*txn_, txn_.graph(&*channel), pending_change_id, inode)?
                    };
                    debug!("dead (line {}) = {:?}", line!(), dead);
                    if !dead.is_empty() {
                        let mut txn = txn.write();
                        kill_dead_files::<T, R, P>(&mut *txn, &channel, &repo, &dead)?;
                    }
                    is_first_none = false;
                }
                if output_item.meta.is_dir() {
                    let tmp_ = tmp.as_deref().unwrap_or(&path);
                    repo.create_dir_all(tmp_)
                        .map_err(OutputError::WorkingCopy)?;
                    {
                        let txn = txn.read();
                        let channel = channel.read();
                        collect_children(
                            &*txn,
                            &*changes,
                            txn.graph(&*channel),
                            output_item.pos,
                            inode,
                            &path,
                            tmp.as_deref(),
                            next_prefix_basename,
                            &mut next_files,
                        )?;
                    }
                    debug!("setting permissions for {:?}", path);
                    repo.set_permissions(tmp_, output_item.meta.permissions())
                        .map_err(OutputError::WorkingCopy)?;
                } else {
                    if needs_output(repo, if_modified_after, &path) {
                        work.push((output_item.clone(), path.clone(), tmp.clone()));
                    } else {
                        debug!("Not outputting {:?}", path)
                    }
                }
                if output_item.is_zombie {
                    conflicts.push(Conflict::ZombieFile {
                        path: name.to_string(),
                    })
                }
            }
        }
        std::mem::swap(&mut files, &mut next_files);
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let o = output_loop(repo, changes, txn, channel, work, stop, 0);
    for t in threads {
        conflicts.extend(t.join().unwrap()?.into_iter());
    }
    conflicts.extend(o?.into_iter());
    for (a, b) in actual_moves.iter() {
        repo.rename(a, b).map_err(OutputError::WorkingCopy)?
    }
    Ok(conflicts)
}

fn make_conflicting_name(name: &str, name_key: Vertex<ChangeId>) -> String {
    let parent = path::parent(name).unwrap();
    let basename = path::file_name(name).unwrap();
    let mut parent = parent.to_string();
    path::push(
        &mut parent,
        &format!("{}.{}", basename, &name_key.change.to_base32()),
    );
    parent
}

fn needs_output<R: WorkingCopy>(
    repo: &R,
    if_modified_after: Option<std::time::SystemTime>,
    path: &str,
) -> bool {
    if let Some(m) = if_modified_after {
        if let Ok(last) = repo.modified_time(path) {
            debug!("modified_after: {:?} {:?}", m, last);
            return last.duration_since(m).is_ok();
        }
    }
    true
}

fn move_or_create<T: TreeMutTxnT, R: WorkingCopy, C: ChangeStore>(
    txn: ArcTxn<T>,
    repo: &R,
    output_item: &OutputItem,
    output_item_inode: Option<(Inode, Position<ChangeId>)>,
    path: &str,
    tmp: &mut Option<String>,
    file_name: &str,
    actual_moves: &mut Vec<(String, String)>,
    salt: u64,
) -> Result<Inode, OutputError<C::Error, T::TreeError, R::Error>> {
    let file_id = OwnedPathId {
        parent_inode: output_item.parent,
        basename: SmallString::from_str(&file_name),
    };
    debug!("move_or_create {:?}", file_id);

    if let Some((inode, _)) = output_item_inode {
        // If the file already exists, find its
        // current name and rename it if that name
        // is different.
        let txn_ = txn.read();
        if let Some(ref current_name) = inode_filename(&*txn_, inode)? {
            debug!("current_name = {:?}, path = {:?}", current_name, path);
            if current_name != path {
                std::mem::drop(txn_);
                let mut txn_ = txn.write();
                let parent = txn_.get_revtree(&inode, None)?.unwrap().to_owned();
                debug!("parent = {:?}, inode = {:?}", parent, inode);
                del_tree_with_rev(&mut *txn_, &parent, &inode)?;

                let mut tmp_path = path.to_string();
                crate::path::pop(&mut tmp_path);

                let s = {
                    let mut c = [0u8; 16];
                    unsafe { *(c.as_mut_ptr() as *mut Position<ChangeId>) = output_item.pos }
                    data_encoding::BASE32_NOPAD.encode(blake3::hash(&c).as_bytes())
                };
                crate::path::push(&mut tmp_path, &s);
                repo.rename(&current_name, &tmp_path)
                    .map_err(OutputError::WorkingCopy)?;

                if let Some(ref mut tmp) = tmp {
                    crate::path::push(tmp, &s);
                } else {
                    *tmp = Some(tmp_path.clone());
                }
                actual_moves.push((tmp_path, path.to_string()));

                // If the new location is overwriting an existing one,
                // actually overwrite.
                if let Some(&inode) = txn_.get_tree(&file_id, None)? {
                    crate::fs::rec_delete(&mut *txn_, &file_id, inode, true)
                        .map_err(PristineOutputError::Fs)?;
                }
                put_inodes_with_rev(&mut *txn_, &inode, &output_item.pos)?;
                put_tree_with_rev(&mut *txn_, &file_id, &inode)?;
                // The directory marker is necessarily already there,
                // since the path is in the tree.
                if output_item.meta.is_dir() {
                    let path_id = OwnedPathId {
                        parent_inode: inode,
                        basename: SmallString::new(),
                    };
                    assert_eq!(txn_.get_tree(&path_id, None).unwrap(), Some(&inode))
                }
            }
        } else {
            debug!("no current name, inserting {:?} {:?}", file_id, inode);
            std::mem::drop(txn_);
            let mut txn_ = txn.write();
            if let Some(&inode) = txn_.get_tree(&file_id, None)? {
                crate::fs::rec_delete(&mut *txn_, &file_id, inode, true)
                    .map_err(PristineOutputError::Fs)?;
            }
            put_inodes_with_rev(&mut *txn_, &inode, &output_item.pos)?;
            put_tree_with_rev(&mut *txn_, &file_id, &inode)?;
            if output_item.meta.is_dir() {
                let path_id = OwnedPathId {
                    parent_inode: inode,
                    basename: SmallString::new(),
                };
                txn_.put_tree(&path_id, &inode)?;
            }
        }
        Ok(inode)
    } else {
        let mut txn_ = txn.write();
        if let Some(&inode) = txn_.get_tree(&file_id, None)? {
            crate::fs::rec_delete(&mut *txn_, &file_id, inode, true)
                .map_err(PristineOutputError::Fs)?;
        }
        let inode = create_new_inode(&mut *txn_, &file_id, salt)?;
        debug!(
            "created new inode {:?} {:?} {:?}",
            inode, output_item.pos, file_id
        );
        put_inodes_with_rev(&mut *txn_, &inode, &output_item.pos)?;
        put_tree_with_rev(&mut *txn_, &file_id, &inode)?;
        if output_item.meta.is_dir() {
            let path_id = OwnedPathId {
                parent_inode: inode,
                basename: SmallString::new(),
            };
            txn_.put_tree(&path_id, &inode)?;
        }
        Ok(inode)
    }
}

fn output_item<T: ChannelMutTxnT + GraphMutTxnT, P: ChangeStore, W: WorkingCopy>(
    txn: ArcTxn<T>,
    channel: ChannelRef<T>,
    changes: &P,
    output_item: &OutputItem,
    conflicts: &mut Vec<Conflict>,
    repo: &W,
    path: &str,
) -> Result<(), OutputError<P::Error, T::GraphError, W::Error>> {
    let mut forward = Vec::new();
    {
        let txn = txn.read();
        let channel = channel.read();
        let mut l = retrieve(&*txn, txn.graph(&*channel), output_item.pos)?;
        let w = repo.write_file(&path).map_err(OutputError::WorkingCopy)?;
        let mut f = vertex_buffer::ConflictsWriter::new(w, &path, conflicts);
        alive::output_graph(changes, &*txn, &*channel, &mut f, &mut l, &mut forward)
            .map_err(PristineOutputError::from)?;
    }
    if forward.is_empty() {
        return Ok(());
    }
    let mut txn = txn.write();
    let mut channel = channel.write();
    for &(vertex, edge) in forward.iter() {
        // Unwrap ok since `edge` is in the channel.
        let dest = *txn.find_block(txn.graph(&*channel), edge.dest()).unwrap();
        debug!("deleting forward edge {:?} {:?} {:?}", vertex, dest, edge);
        del_graph_with_rev(
            &mut *txn,
            T::graph_mut(&mut *channel),
            edge.flag(),
            vertex,
            dest,
            edge.introduced_by(),
        )?;
    }
    Ok(())
}

fn is_alive_or_zombie<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    a: &Vertex<ChangeId>,
) -> Result<bool, TxnErr<T::GraphError>> {
    if a.is_root() {
        return Ok(true);
    }
    for e in iter_adjacent(
        txn,
        channel,
        *a,
        EdgeFlags::PARENT,
        EdgeFlags::all() - EdgeFlags::DELETED,
    )? {
        let e = e?;
        let zf = EdgeFlags::pseudof();
        if (e.flag() & zf != EdgeFlags::PSEUDO)
            && (e.flag().contains(EdgeFlags::BLOCK) || a.is_empty())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn collect_dead_files<T: TreeTxnT + GraphTxnT<GraphError = <T as TreeTxnT>::TreeError>>(
    txn: &T,
    channel: &T::Graph,
    pending_change_id: ChangeId,
    inode: Inode,
) -> Result<HashMap<OwnedPathId, (Inode, Option<String>)>, TxnErr<T::GraphError>> {
    let mut inodes = vec![(inode, false)];
    let mut next_inodes = Vec::new();
    let mut dead = HashMap::default();
    while !inodes.is_empty() {
        for (inode, parent_is_dead) in inodes.drain(..) {
            for x in txn.iter_tree(
                &OwnedPathId {
                    parent_inode: inode,
                    basename: SmallString::new(),
                },
                None,
            )? {
                let (id, inode_) = x?;
                assert!(id.parent_inode >= inode);
                if id.parent_inode > inode {
                    break;
                }
                let is_dead = parent_is_dead
                    || (!id.basename.is_empty() && {
                        if let Some(vertex) = txn.get_inodes(&inode_, None)? {
                            vertex.change != pending_change_id
                                && !is_alive_or_zombie(txn, channel, &vertex.inode_vertex())?
                        } else {
                            true
                        }
                    });
                if is_dead {
                    dead.insert(id.to_owned(), (*inode_, inode_filename(txn, *inode_)?));
                }
                if *inode_ != inode {
                    next_inodes.push((*inode_, is_dead))
                }
            }
        }
        std::mem::swap(&mut inodes, &mut next_inodes)
    }
    Ok(dead)
}

fn kill_dead_files<
    T: ChannelTxnT<GraphError = T::TreeError> + TreeMutTxnT,
    W: WorkingCopy + Clone,
    C: ChangeStore,
>(
    txn: &mut T,
    channel: &ChannelRef<T>,
    repo: &W,
    dead: &HashMap<OwnedPathId, (Inode, Option<String>)>,
) -> Result<(), OutputError<C::Error, T::TreeError, W::Error>> {
    let channel = channel.read();
    for (fileid, (inode, ref name)) in dead.iter() {
        debug!("killing {:?} {:?} {:?}", fileid, inode, name);
        del_tree_with_rev(txn, &fileid, inode)?;
        // In case this is a directory, we also need to delete the marker:
        let file_id_ = OwnedPathId {
            parent_inode: *inode,
            basename: SmallString::new(),
        };
        txn.del_tree(&file_id_, Some(&inode))?;

        if let Some(&vertex) = txn.get_inodes(inode, None)? {
            debug!("kill_dead_files {:?} {:?}", inode, vertex);
            del_inodes_with_rev(txn, inode, &vertex)?;
            if txn
                .get_graph(txn.graph(&*channel), &vertex.inode_vertex(), None)
                .map_err(|x| OutputError::Pristine(x.into()))?
                .is_some()
            {
                if let Some(name) = name {
                    repo.remove_path(&name, false)
                        .map_err(OutputError::WorkingCopy)?
                }
            }
        }
    }
    Ok(())
}
