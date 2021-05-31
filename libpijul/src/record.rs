//! Hunk a change from a pristine and a working copy.
use crate::alive::retrieve;
use crate::change::*;
use crate::changestore::ChangeStore;
use crate::diff;
pub use crate::diff::Algorithm;
use crate::path::{components, Components};
use crate::pristine::*;
use crate::small_string::SmallString;
use crate::working_copy::WorkingCopy;
use crate::{HashMap, HashSet};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Error)]
pub enum RecordError<
    C: std::error::Error + 'static,
    W: std::error::Error,
    T: std::error::Error + 'static,
> {
    #[error("Changestore error: {0}")]
    Changestore(C),
    #[error("Working copy error: {0}")]
    WorkingCopy(W),
    #[error("System time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error(transparent)]
    Txn(T),
    #[error(transparent)]
    Diff(#[from] diff::DiffError<C, T>),
    #[error("Path not in repository: {0}")]
    PathNotInRepo(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl<
        C: std::error::Error + 'static,
        W: std::error::Error + 'static,
        T: std::error::Error + 'static,
    > std::convert::From<TxnErr<T>> for RecordError<C, W, T>
{
    fn from(e: TxnErr<T>) -> Self {
        RecordError::Txn(e.0)
    }
}

impl<
        C: std::error::Error + 'static,
        W: std::error::Error + 'static,
        T: std::error::Error + 'static,
    > std::convert::From<crate::output::FileError<C, T>> for RecordError<C, W, T>
{
    fn from(e: crate::output::FileError<C, T>) -> Self {
        match e {
            crate::output::FileError::Changestore(e) => RecordError::Changestore(e),
            crate::output::FileError::Io(e) => RecordError::Io(e),
            crate::output::            FileError::Txn(t) => RecordError::Txn(t),
        }
    }
}

/// A change in the process of being recorded. This is typically
/// created using `Builder::new`.
pub struct Builder {
    pub(crate) rec: Vec<Arc<Mutex<Recorded>>>,
    recorded_inodes: Arc<Mutex<HashMap<Inode, Position<Option<ChangeId>>>>>,
    deleted_vertices: Arc<Mutex<HashSet<Position<ChangeId>>>>,
    pub force_rediff: bool,
    pub ignore_missing: bool,
    pub contents: Arc<Mutex<Vec<u8>>>,
}

#[derive(Debug)]
struct Parent {
    basename: String,
    metadata: InodeMetadata,
    parent: Position<Option<ChangeId>>,
}

/// The result of recording a change:
pub struct Recorded {
    /// The "byte contents" of the change.
    pub contents: Arc<Mutex<Vec<u8>>>,
    /// The current records, to be lated converted into change operations.
    pub actions: Vec<Hunk<Option<ChangeId>, Local>>,
    /// The updates that need to be made to the ~tree~ and ~revtree~
    /// tables when this change is applied to the local repository.
    pub updatables: HashMap<usize, InodeUpdate>,
    /// The size of the largest file that was recorded in this change.
    pub largest_file: u64,
    /// Whether we have recorded binary files.
    pub has_binary_files: bool,
    /// Timestamp of the oldest changed file. If nothing changed,
    /// returns now().
    pub oldest_change: std::time::SystemTime,
    /// Redundant edges found during the comparison.
    pub redundant: Vec<(Vertex<ChangeId>, SerializedEdge)>,
    /// Force a re-diff
    force_rediff: bool,
    deleted_vertices: Arc<Mutex<HashSet<Position<ChangeId>>>>,
    recorded_inodes: Arc<Mutex<HashMap<Inode, Position<Option<ChangeId>>>>>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            rec: Vec::new(),
            recorded_inodes: Arc::new(Mutex::new(HashMap::default())),
            force_rediff: false,
            ignore_missing: false,
            deleted_vertices: Arc::new(Mutex::new(HashSet::default())),
            contents: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Builder {
    /// Initialise a `Builder`.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn recorded(&mut self) -> Arc<Mutex<Recorded>> {
        let m = Arc::new(Mutex::new(self.recorded_()));
        self.rec.push(m.clone());
        m
    }

    fn recorded_(&self) -> Recorded {
        Recorded {
            contents: self.contents.clone(),
            actions: Vec::new(),
            updatables: HashMap::default(),
            largest_file: 0,
            has_binary_files: false,
            oldest_change: std::time::SystemTime::UNIX_EPOCH,
            redundant: Vec::new(),
            force_rediff: self.force_rediff,
            deleted_vertices: self.deleted_vertices.clone(),
            recorded_inodes: self.recorded_inodes.clone(),
        }
    }

    /// Finish the recording.
    pub fn finish(mut self) -> Recorded {
        if self.rec.is_empty() {
            self.recorded();
        }
        let mut it = self.rec.into_iter();
        let mut result = if let Ok(rec) = Arc::try_unwrap(it.next().unwrap()) {
            rec.into_inner().unwrap()
        } else {
            unreachable!()
        };
        for rec in it {
            let rec = if let Ok(rec) = Arc::try_unwrap(rec) {
                rec.into_inner().unwrap()
            } else {
                unreachable!()
            };
            let off = result.actions.len();
            result.actions.extend(rec.actions.into_iter());
            for (a, b) in rec.updatables {
                result.updatables.insert(a + off, b);
            }
            result.largest_file = result.largest_file.max(rec.largest_file);
            result.has_binary_files |= rec.has_binary_files;
            if result.oldest_change == std::time::UNIX_EPOCH
                || (rec.oldest_change > std::time::UNIX_EPOCH
                    && rec.oldest_change < result.oldest_change)
            {
                result.oldest_change = rec.oldest_change
            }
            result.redundant.extend(rec.redundant.into_iter())
        }
        debug!(
            "result = {:?}, updatables = {:?}",
            result.actions, result.updatables
        );
        result
    }
}

/// An account of the files that have been added, moved or deleted, as
/// returned by record, and used by apply (when applying a change
/// created locally) to update the trees and inodes databases.
#[derive(Debug, Hash, PartialEq, Eq)]
pub enum InodeUpdate {
    Add {
        /// Inode vertex in the graph.
        pos: ChangePosition,
        /// `Inode` added by this file addition.
        inode: Inode,
    },
    Deleted {
        /// `Inode` of the deleted file.
        inode: Inode,
    },
}

#[derive(Debug, Clone)]
struct RecordItem {
    v_papa: Position<Option<ChangeId>>,
    papa: Inode,
    inode: Inode,
    basename: String,
    full_path: String,
    metadata: InodeMetadata,
}

impl RecordItem {
    fn root() -> Self {
        RecordItem {
            inode: Inode::ROOT,
            papa: Inode::ROOT,
            v_papa: Position::OPTION_ROOT,
            basename: String::new(),
            full_path: String::new(),
            metadata: InodeMetadata::new(0, true),
        }
    }
}

const CHECK_UTF8: usize = 1000;

/// Ignore inodes that are in another channel
fn get_inodes_<T: ChannelTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>>(
    txn: Arc<RwLock<T>>,
    channel: ChannelRef<T>,
    inode: &Inode,
) -> Result<Option<Position<ChangeId>>, TxnErr<T::GraphError>> {
    let txn = txn.read().unwrap();
    let channel = channel.r.read().unwrap();
    Ok(get_inodes(&*txn, &*channel, inode)?.map(|x| *x))
}

fn get_inodes<'a, T: ChannelTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>>(
    txn: &'a T,
    channel: &T::Channel,
    inode: &Inode,
) -> Result<Option<&'a Position<ChangeId>>, TxnErr<T::GraphError>> {
    if let Some(vertex) = txn.get_inodes(inode, None)? {
        if let Some(e) = iter_adjacent(
            txn,
            txn.graph(channel),
            vertex.inode_vertex(),
            EdgeFlags::PARENT,
            EdgeFlags::all(),
        )?
        .next()
        {
            if e?.flag().is_alive_parent() {
                return Ok(Some(vertex));
            }
        }
        Ok(None)
    } else {
        Ok(None)
    }
}

struct Tasks {
    stop: bool,
    t: VecDeque<(
        RecordItem,
        Position<ChangeId>,
        Arc<Mutex<Recorded>>,
        Option<Position<Option<ChangeId>>>,
    )>,
}

impl Builder {
    pub fn record<
        T,
        W: WorkingCopy + Send + Sync + 'static,
        C: ChangeStore + Clone + Send + 'static,
    >(
        &mut self,
        txn: Arc<RwLock<T>>,
        diff_algorithm: diff::Algorithm,
        channel: ChannelRef<T>,
        working_copy: Arc<W>,
        changes: &C,
        prefix: &str,
        _n_workers: usize,
    ) -> Result<(), RecordError<C::Error, W::Error, T::GraphError>>
    where
        T: ChannelMutTxnT
            + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>
            + Send
            + Sync
            + 'static,
        T::Channel: Send + Sync,
        <W as WorkingCopy>::Error: 'static,
    {
        let work = Arc::new(Mutex::new(Tasks {
            t: VecDeque::new(),
            stop: false,
        }));
        let mut workers: Vec<std::thread::JoinHandle<()>> = Vec::new();
        for t in 0..0 {
            // n_workers - 1 {
            let working_copy = working_copy.clone();
            let changes = changes.clone();
            let channel = channel.clone();
            let work = work.clone();
            let txn = txn.clone();
            workers.push(std::thread::spawn(move || loop {
                let (w, stop) = {
                    let mut work = work.lock().unwrap();
                    (work.t.pop_front(), work.stop)
                };
                if let Some((item, vertex, rec, new_papa)) = w {
                    // This parent has changed.
                    info!("record existing file {:?} on thread {:?}", item, t);
                    rec.lock()
                        .unwrap()
                        .record_existing_file(
                            txn.clone(),
                            diff_algorithm,
                            channel.clone(),
                            working_copy.clone(),
                            &changes,
                            &item,
                            new_papa,
                            vertex,
                        )
                        .unwrap();
                } else if stop {
                    info!("stop {:?}", t);
                    break;
                } else {
                    info!("yield {:?}", t);
                    std::thread::park_timeout(std::time::Duration::from_secs(1));
                }
            }))
        }

        let now = std::time::Instant::now();
        let mut stack = vec![(RecordItem::root(), components(prefix))];
        while let Some((mut item, mut components)) = stack.pop() {
            debug!("stack.pop() = Some({:?})", item);

            // Check for moves and file conflicts.
            let vertex: Option<Position<Option<ChangeId>>> = self
                .recorded_inodes
                .lock()
                .unwrap()
                .get(&item.inode)
                .cloned();
            let vertex = if let Some(vertex) = vertex {
                vertex
            } else if item.inode == Inode::ROOT {
                self.recorded_inodes
                    .lock()
                    .unwrap()
                    .insert(Inode::ROOT, Position::OPTION_ROOT);
                debug!("TAKING LOCK {}", line!());
                let txn = txn.read().unwrap();
                debug!("TAKEN");
                let channel = channel.r.read().unwrap();
                debug!("TAKEN 2");
                self.delete_obsolete_children(
                    &*txn,
                    txn.graph(&channel),
                    working_copy.as_ref(),
                    changes,
                    &item.full_path,
                    Position::ROOT,
                )?;
                Position::OPTION_ROOT
            } else if let Some(vertex) = get_inodes_(txn.clone(), channel.clone(), &item.inode)? {
                {
                    let mut txn = txn.write().unwrap();
                    let mut channel = channel.r.write().unwrap();
                    let mut graph = txn.graph(&mut *channel);
                    self.delete_obsolete_children(
                        &mut *txn,
                        &mut graph,
                        working_copy.as_ref(),
                        changes,
                        &item.full_path,
                        vertex,
                    )?;
                }

                let rec = self.recorded();
                let new_papa = {
                    let mut recorded = self.recorded_inodes.lock().unwrap();
                    recorded.insert(item.inode, vertex.to_option());
                    recorded.get(&item.papa).cloned()
                };
                let mut work = work.lock().unwrap();
                work.t.push_back((item.clone(), vertex, rec, new_papa));
                std::mem::drop(work);
                for t in workers.iter() {
                    t.thread().unpark()
                }

                vertex.to_option()
            } else {
                let rec = self.recorded();
                debug!("TAKING LOCK {}", line!());
                let mut rec = rec.lock().unwrap();
                match rec.add_file(working_copy.as_ref(), item.clone()) {
                    Ok(Some(vertex)) => {
                        // Path addition (maybe just a single directory).
                        self.recorded_inodes
                            .lock()
                            .unwrap()
                            .insert(item.inode, vertex);
                        vertex
                    }
                    _ => continue,
                }
            };

            // Move on to the next step.
            debug!("TAKING LOCK {}", line!());
            let txn = txn.read().unwrap();
            let channel = channel.r.read().unwrap();
            self.push_children::<_, _, C>(
                &*txn,
                &*channel,
                working_copy.as_ref(),
                &mut item,
                &mut components,
                vertex,
                &mut stack,
                prefix,
            )?;
        }

        info!("stop work");
        work.lock().unwrap().stop = true;
        for t in workers.iter() {
            t.thread().unpark()
        }
        loop {
            let w = {
                let mut work = work.lock().unwrap();
                debug!("waiting, stop = {:?}", work.stop);
                work.t.pop_front()
            };
            if let Some((item, vertex, rec, new_papa)) = w {
                // This parent has changed.
                info!("record existing file {:?}", item);
                rec.lock()
                    .unwrap()
                    .record_existing_file(
                        txn.clone(),
                        diff_algorithm,
                        channel.clone(),
                        working_copy.clone(),
                        changes,
                        &item,
                        new_papa,
                        vertex,
                    )
                    .unwrap();
            } else {
                break;
            }
        }
        for (n, t) in workers.into_iter().enumerate() {
            debug!("WAITING {:?}", n);
            t.join().unwrap()
        }
        crate::TIMERS.lock().unwrap().record += now.elapsed();
        info!("record done");
        Ok(())
    }

    fn delete_obsolete_children<
        T: GraphTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>,
        W: WorkingCopy,
        C: ChangeStore,
    >(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        working_copy: &W,
        changes: &C,
        full_path: &str,
        v: Position<ChangeId>,
    ) -> Result<(), RecordError<C::Error, W::Error, T::GraphError>>
    where
        <W as WorkingCopy>::Error: 'static,
    {
        if self.ignore_missing {
            return Ok(());
        }
        let f0 = EdgeFlags::FOLDER | EdgeFlags::BLOCK;
        let f1 = f0 | EdgeFlags::PSEUDO;
        debug!("delete_obsolete_children, v = {:?}", v);
        for child in iter_adjacent(txn, channel, v.inode_vertex(), f0, f1)? {
            let child = child?;
            let child = txn.find_block(channel, child.dest()).unwrap();
            for grandchild in iter_adjacent(txn, channel, *child, f0, f1)? {
                let grandchild = grandchild?;
                debug!("grandchild {:?}", grandchild);
                let needs_deletion =
                    if let Some(inode) = txn.get_revinodes(&grandchild.dest(), None)? {
                        debug!("inode = {:?} {:?}", inode, txn.get_revtree(inode, None));
                        if let Some(path) = crate::fs::inode_filename(txn, *inode)? {
                            working_copy.file_metadata(&path).is_err()
                        } else {
                            true
                        }
                    } else {
                        true
                    };
                if needs_deletion {
                    let mut name = Vec::new();
                    changes
                        .get_contents(
                            |p| txn.get_external(&p).unwrap().map(From::from),
                            *child,
                            &mut name,
                        )
                        .map_err(RecordError::Changestore)?;
                    let mut full_path = full_path.to_string();
                    if name.len() > 2 {
                        if let Ok(name) = std::str::from_utf8(&name[2..]) {
                            if !full_path.is_empty() {
                                full_path.push('/');
                            }
                            full_path.push_str(name);
                        }
                    }
                    // delete recursively.
                    let rec = self.recorded();
                    let mut rec = rec.lock().unwrap();
                    rec.record_deleted_file(
                        txn,
                        &channel,
                        working_copy,
                        &full_path,
                        grandchild.dest(),
                    )?
                }
            }
        }
        Ok(())
    }

    fn push_children<
        'a,
        T: ChannelTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>,
        W: WorkingCopy,
        C: ChangeStore,
    >(
        &mut self,
        txn: &T,
        channel: &T::Channel,
        working_copy: &W,
        item: &mut RecordItem,
        components: &mut Components<'a>,
        vertex: Position<Option<ChangeId>>,
        stack: &mut Vec<(RecordItem, Components<'a>)>,
        prefix: &str,
    ) -> Result<(), RecordError<C::Error, W::Error, T::GraphError>>
    where
        <W as crate::working_copy::WorkingCopy>::Error: 'static,
    {
        debug!("push_children, item = {:?}", item);
        let comp = components.next();
        let full_path = item.full_path.clone();
        let fileid = OwnedPathId {
            parent_inode: item.inode,
            basename: SmallString::new(),
        };
        let mut has_matching_children = false;
        for x in txn.iter_tree(&fileid, None)? {
            let (fileid_, child_inode) = x?;
            debug!("push_children {:?} {:?}", fileid_, child_inode);
            if fileid_.parent_inode < fileid.parent_inode || fileid_.basename.is_empty() {
                continue;
            } else if fileid_.parent_inode > fileid.parent_inode {
                break;
            }
            if let Some(comp) = comp {
                if comp != fileid_.basename.as_str() {
                    continue;
                }
            }
            has_matching_children = true;
            let basename = fileid_.basename.as_str().to_string();
            let full_path = if full_path.is_empty() {
                basename.clone()
            } else {
                full_path.clone() + "/" + &basename
            };
            debug!("fileid_ {:?} child_inode {:?}", fileid_, child_inode);
            if let Ok(meta) = working_copy.file_metadata(&full_path) {
                stack.push((
                    RecordItem {
                        papa: item.inode,
                        inode: *child_inode,
                        v_papa: vertex,
                        basename,
                        full_path,
                        metadata: meta,
                    },
                    components.clone(),
                ));
            } else if let Some(vertex) = get_inodes(txn, &channel, child_inode)? {
                let rec = self.recorded();
                let mut rec = rec.lock().unwrap();
                rec.record_deleted_file(txn, txn.graph(channel), working_copy, &full_path, *vertex)?
            }
        }
        if comp.is_some() && !has_matching_children {
            debug!("comp = {:?}", comp);
            return Err(RecordError::PathNotInRepo(prefix.to_string()));
        }
        Ok(())
    }
}

fn modified_since_last_commit<T: ChannelTxnT, W: WorkingCopy>(
    txn: &T,
    channel: &T::Channel,
    working_copy: &W,
    prefix: &str,
) -> Result<bool, std::time::SystemTimeError> {
    if let Ok(last_modified) = working_copy.modified_time(prefix) {
        debug!(
            "last_modified = {:?}, channel.last = {:?}",
            last_modified
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            txn.last_modified(channel)
        );
        Ok(last_modified
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            >= txn.last_modified(channel))
    } else {
        Ok(true)
    }
}

impl Recorded {
    fn add_file<W: WorkingCopy>(
        &mut self,
        working_copy: &W,
        item: RecordItem,
    ) -> Result<Option<Position<Option<ChangeId>>>, W::Error> {
        debug!("record_file_addition {:?}", item);
        let meta = working_copy.file_metadata(&item.full_path)?;
        let mut contents = self.contents.lock().unwrap();
        let name_start = ChangePosition(contents.len().into());
        // Push the metadata, big-endian.
        contents.push((meta.0 >> 8) as u8);
        contents.push((meta.0 & 0xff) as u8);
        //
        contents.extend(item.basename.as_bytes());
        let name_end = ChangePosition(contents.len().into());
        contents.push(0);
        let inode_pos = ChangePosition(contents.len().into());
        contents.push(0);

        let contents = if meta.is_file() {
            let start = ChangePosition(contents.len().into());
            working_copy.read_file(&item.full_path, &mut contents)?;
            let end = ChangePosition(contents.len().into());
            self.largest_file = self.largest_file.max(end.0.as_u64() - start.0.as_u64());
            self.has_binary_files |= {
                let s = start.0.into();
                let e = end.0.as_usize().min(s + CHECK_UTF8 + 4);
                let utf8 = std::str::from_utf8(&contents[s..e]);
                debug!("utf8 = {:?}", utf8);
                match utf8 {
                    Err(e) => e.valid_up_to() < CHECK_UTF8,
                    Ok(_) => false,
                }
            };
            contents.push(0);
            if end > start {
                Some(Atom::NewVertex(NewVertex {
                    up_context: vec![Position {
                        change: None,
                        pos: inode_pos,
                    }],
                    down_context: vec![],
                    start,
                    end,
                    flag: EdgeFlags::BLOCK,
                    inode: Position {
                        change: None,
                        pos: inode_pos,
                    },
                }))
            } else {
                None
            }
        } else {
            None
        };
        self.actions.push(Hunk::FileAdd {
            add_name: Atom::NewVertex(NewVertex {
                up_context: vec![item.v_papa],
                down_context: vec![],
                start: name_start,
                end: name_end,
                flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                inode: item.v_papa,
            }),
            add_inode: Atom::NewVertex(NewVertex {
                up_context: vec![Position {
                    change: None,
                    pos: name_end,
                }],
                down_context: vec![],
                start: inode_pos,
                end: inode_pos,
                flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                inode: item.v_papa,
            }),
            contents,
            path: item.full_path.clone(),
        });
        debug!("{:?}", self.actions.last().unwrap());
        self.updatables.insert(
            self.actions.len(),
            InodeUpdate::Add {
                inode: item.inode,
                pos: inode_pos,
            },
        );
        if meta.is_dir() {
            Ok(Some(Position {
                change: None,
                pos: inode_pos,
            }))
        } else {
            Ok(None)
        }
    }

    fn record_existing_file<
        T: ChannelTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>,
        W: WorkingCopy,
        C: ChangeStore,
    >(
        &mut self,
        txn: Arc<RwLock<T>>,
        diff_algorithm: diff::Algorithm,
        channel: ChannelRef<T>,
        working_copy: Arc<W>,
        changes: &C,
        item: &RecordItem,
        new_papa: Option<Position<Option<ChangeId>>>,
        vertex: Position<ChangeId>,
    ) -> Result<(), RecordError<C::Error, W::Error, T::GraphError>>
    where
        <W as crate::working_copy::WorkingCopy>::Error: 'static,
    {
        debug!(
            "record_existing_file {:?}: {:?} {:?}",
            item.full_path, item.inode, vertex
        );
        // Former parent(s) of vertex
        let mut former_parents = Vec::new();
        let f0 = EdgeFlags::FOLDER | EdgeFlags::PARENT;
        let f1 = EdgeFlags::all();
        let mut is_deleted = true;
        let txn_ = txn.read().unwrap();
        let channel_ = channel.read().unwrap();
        for name_ in iter_adjacent(
            &*txn_,
            txn_.graph(&*channel_),
            vertex.inode_vertex(),
            f0,
            f1,
        )? {
            debug!("name_ = {:?}", name_);
            let name_ = name_?;
            if !name_.flag().contains(EdgeFlags::PARENT) {
                debug!("continue");
                continue;
            }
            if name_.flag().contains(EdgeFlags::DELETED) {
                debug!("is_deleted {:?}: {:?}", item.full_path, name_);
                is_deleted = true;
                break;
            }
            let name_dest = txn_
                .find_block_end(txn_.graph(&*channel_), name_.dest())
                .unwrap();
            let mut name = Vec::new();
            changes
                .get_contents(
                    |p| txn_.get_external(&p).unwrap().map(From::from),
                    *name_dest,
                    &mut name,
                )
                .map_err(RecordError::Changestore)?;
            let (metadata, basename) = name.split_at(2);
            let metadata = InodeMetadata::from_basename(metadata);
            let basename = std::str::from_utf8(basename).unwrap().to_string();
            debug!(
                "former basename of {:?}: {:?} {:?}",
                vertex, basename, metadata
            );
            if let Some(v_papa) =
                iter_adjacent(&*txn_, txn_.graph(&*channel_), *name_dest, f0, f1)?.next()
            {
                let v_papa = v_papa?;
                if !v_papa.flag().contains(EdgeFlags::DELETED) {
                    former_parents.push(Parent {
                        basename,
                        metadata,
                        parent: v_papa.dest().to_option(),
                    })
                }
            }
        }
        debug!(
            "record_existing_file: {:?} {:?} {:?}",
            item, former_parents, is_deleted,
        );
        assert!(!former_parents.is_empty());
        if let Ok(new_meta) = working_copy.file_metadata(&item.full_path) {
            debug!("new_meta = {:?}", new_meta);
            if former_parents.len() > 1
                || former_parents[0].basename != item.basename
                || former_parents[0].metadata != item.metadata
                || former_parents[0].parent != item.v_papa
                || is_deleted
            {
                debug!("new_papa = {:?}", new_papa);
                self.record_moved_file::<_, _, W>(
                    changes,
                    &*txn_,
                    &*channel_,
                    &item,
                    vertex,
                    new_papa.unwrap(),
                )?
            }
            if new_meta.is_file()
                && (self.force_rediff
                    || modified_since_last_commit(
                        &*txn_,
                        &*channel_,
                        working_copy.as_ref(),
                        &item.full_path,
                    )?)
            {
                let mut ret = retrieve(&*txn_, txn_.graph(&*channel_), vertex)?;
                let mut b = Vec::new();
                working_copy
                    .read_file(&item.full_path, &mut b)
                    .map_err(RecordError::WorkingCopy)?;
                debug!("diffing…");
                let len = self.actions.len();
                self.diff(
                    changes,
                    &*txn_,
                    &*channel_,
                    diff_algorithm,
                    item.full_path.clone(),
                    vertex.to_option(),
                    &mut ret,
                    &b,
                )?;
                if self.actions.len() > len {
                    if let Ok(last_modified) = working_copy.modified_time(&item.full_path) {
                        if self.oldest_change == std::time::SystemTime::UNIX_EPOCH {
                            self.oldest_change = last_modified;
                        } else {
                            self.oldest_change = self.oldest_change.min(last_modified);
                        }
                    }
                }
                debug!(
                    "new actions: {:?}, total {:?}",
                    &self.actions.len() - len,
                    self.actions.len()
                );
            }
        } else {
            debug!("calling record_deleted_file on {:?}", item.full_path);
            self.record_deleted_file(
                &*txn_,
                txn_.graph(&*channel_),
                working_copy.as_ref(),
                &item.full_path,
                vertex,
            )?
        }
        Ok(())
    }

    fn record_moved_file<T: ChannelTxnT, C: ChangeStore, W: WorkingCopy>(
        &mut self,
        changes: &C,
        txn: &T,
        channel: &T::Channel,
        item: &RecordItem,
        vertex: Position<ChangeId>,
        new_papa: Position<Option<ChangeId>>,
    ) -> Result<(), RecordError<C::Error, W::Error, T::GraphError>>
    where
        <W as crate::working_copy::WorkingCopy>::Error: 'static,
    {
        debug!("record_moved_file {:?}", item);
        let mut contents = self.contents.lock().unwrap();
        let name_start = ChangePosition(contents.len().into());
        // Push the metadata, big-endian.
        contents.push((item.metadata.0 >> 8) as u8);
        contents.push((item.metadata.0 & 0xff) as u8);
        //
        contents.extend(item.basename.as_bytes());
        let name_end = ChangePosition(contents.len().into());
        contents.push(0);
        let name = &contents[name_start.0.as_usize()..name_end.0.as_usize()];
        let mut moved = collect_moved_edges::<_, _, W>(
            txn,
            changes,
            txn.graph(channel),
            new_papa,
            vertex,
            item.metadata,
            name,
        )?;
        debug!("moved = {:#?}", moved);
        if !moved.resurrect.is_empty() {
            moved.resurrect.extend(moved.alive.into_iter());
            if !moved.need_new_name {
                moved.resurrect.extend(moved.edges.drain(..));
            }
            self.actions.push(Hunk::FileUndel {
                undel: Atom::EdgeMap(EdgeMap {
                    edges: moved.resurrect,
                    inode: item.v_papa,
                }),
                contents: None,
                path: item.full_path.clone(),
            });
        }
        if !moved.edges.is_empty() {
            if moved.need_new_name {
                self.actions.push(Hunk::FileMove {
                    del: Atom::EdgeMap(EdgeMap {
                        edges: moved.edges,
                        inode: item.v_papa,
                    }),
                    add: Atom::NewVertex(NewVertex {
                        up_context: vec![item.v_papa],
                        down_context: vec![vertex.to_option()],
                        start: name_start,
                        end: name_end,
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        inode: item.v_papa,
                    }),
                    path: crate::fs::find_path(changes, txn, channel, true, vertex)?.unwrap().0,
                });
            } else {
                self.actions.push(Hunk::SolveNameConflict {
                    name: Atom::EdgeMap(EdgeMap {
                        edges: moved.edges,
                        inode: item.v_papa,
                    }),
                    path: item.full_path.clone(),
                });
                contents.truncate(name_start.0.as_usize())
            }
        } else {
            contents.truncate(name_start.0.as_usize())
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MovedEdges {
    edges: Vec<NewEdge<Option<ChangeId>>>,
    alive: Vec<NewEdge<Option<ChangeId>>>,
    resurrect: Vec<NewEdge<Option<ChangeId>>>,
    need_new_name: bool,
}

fn collect_moved_edges<T: GraphTxnT, C: ChangeStore, W: WorkingCopy>(
    txn: &T,
    changes: &C,
    channel: &T::Graph,
    parent_pos: Position<Option<ChangeId>>,
    current_pos: Position<ChangeId>,
    new_meta: InodeMetadata,
    name: &[u8],
) -> Result<MovedEdges, RecordError<C::Error, W::Error, T::GraphError>>
where
    <W as crate::working_copy::WorkingCopy>::Error: 'static,
{
    debug!("collect_moved_edges {:?}", current_pos);
    let mut moved = MovedEdges {
        edges: Vec::new(),
        alive: Vec::new(),
        resurrect: Vec::new(),
        need_new_name: true,
    };
    let mut del_del = HashMap::default();
    let mut alive = HashMap::default();
    let mut previous_name = Vec::new();
    for parent in iter_adjacent(
        txn,
        channel,
        current_pos.inode_vertex(),
        EdgeFlags::FOLDER | EdgeFlags::PARENT,
        EdgeFlags::all(),
    )? {
        let parent = parent?;
        if !parent
            .flag()
            .contains(EdgeFlags::FOLDER | EdgeFlags::PARENT)
        {
            continue;
        }
        debug!("parent = {:?}", parent);
        let mut parent_was_resurrected = false;
        if !parent.flag().contains(EdgeFlags::PSEUDO) {
            if parent.flag().contains(EdgeFlags::DELETED) {
                moved.resurrect.push(NewEdge {
                    previous: parent.flag() - EdgeFlags::PARENT,
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                    from: parent.dest().to_option(),
                    to: current_pos.inode_vertex().to_option(),
                    introduced_by: Some(parent.introduced_by()),
                });
                parent_was_resurrected = true;
                let v = alive
                    .entry((parent.dest(), current_pos.inode_vertex()))
                    .or_insert_with(Vec::new);
                v.push(None)
            } else {
                let v = alive
                    .entry((parent.dest(), current_pos.inode_vertex()))
                    .or_insert_with(Vec::new);
                v.push(Some(parent.introduced_by()))
            }
        }
        previous_name.clear();
        let parent_dest = txn.find_block_end(channel, parent.dest()).unwrap();
        changes
            .get_contents(
                |p| txn.get_external(&p).unwrap().map(From::from),
                *parent_dest,
                &mut previous_name,
            )
            .map_err(RecordError::Changestore)?;
        debug!(
            "parent_dest {:?} {:?} {:?}",
            parent_dest,
            &previous_name[..2],
            std::str::from_utf8(&previous_name[2..])
        );
        let (parent_meta, parent_name) = previous_name.split_at(2);
        let parent_meta = InodeMetadata::from_basename(parent_meta);
        debug!("new_meta = {:?}, parent_meta = {:?}", new_meta, parent_meta);
        let name_changed =
            (parent_name != &name[2..]) || (new_meta != parent_meta && cfg!(not(windows)));
        for grandparent in iter_adjacent(
            txn,
            channel,
            *parent_dest,
            EdgeFlags::FOLDER | EdgeFlags::PARENT,
            EdgeFlags::all(),
        )? {
            let grandparent = grandparent?;
            if !grandparent
                .flag()
                .contains(EdgeFlags::FOLDER | EdgeFlags::PARENT)
                || grandparent.flag().contains(EdgeFlags::PSEUDO)
            {
                continue;
            }
            debug!("grandparent: {:?}", grandparent);
            let grandparent_dest = txn.find_block_end(channel, grandparent.dest()).unwrap();
            assert_eq!(grandparent_dest.start, grandparent_dest.end);
            debug!(
                "grandparent_dest {:?} {:?}",
                grandparent_dest,
                std::str::from_utf8(&previous_name[2..])
            );
            let grandparent_changed = parent_pos != grandparent.dest().to_option();
            debug!("change = {:?}", grandparent_changed || name_changed);

            if grandparent.flag().contains(EdgeFlags::DELETED) {
                if !grandparent_changed && !name_changed {
                    // We resurrect the name
                    moved.resurrect.push(NewEdge {
                        previous: grandparent.flag() - EdgeFlags::PARENT,
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        from: grandparent.dest().to_option(),
                        to: parent_dest.to_option(),
                        introduced_by: Some(grandparent.introduced_by()),
                    });
                    if !parent_was_resurrected && !parent.flag().contains(EdgeFlags::PSEUDO) {
                        moved.alive.push(NewEdge {
                            previous: parent.flag() - EdgeFlags::PARENT,
                            flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                            from: parent.dest().to_option(),
                            to: current_pos.inode_vertex().to_option(),
                            introduced_by: Some(parent.introduced_by()),
                        })
                    }
                    moved.need_new_name = false
                } else {
                    // Clean up the extra deleted edges.
                    debug!("cleanup");
                    let v = del_del
                        .entry((grandparent.dest(), parent_dest))
                        .or_insert_with(Vec::new);
                    v.push(Some(grandparent.introduced_by()))
                }
            } else if grandparent_changed || name_changed {
                moved.edges.push(NewEdge {
                    previous: parent.flag() - EdgeFlags::PARENT,
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED,
                    from: grandparent.dest().to_option(),
                    to: parent_dest.to_option(),
                    introduced_by: Some(grandparent.introduced_by()),
                });
                // The following is really important in missing context detection:
                if !parent_was_resurrected && !parent.flag().contains(EdgeFlags::PSEUDO) {
                    moved.alive.push(NewEdge {
                        previous: parent.flag() - EdgeFlags::PARENT,
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        from: parent.dest().to_option(),
                        to: current_pos.inode_vertex().to_option(),
                        introduced_by: Some(parent.introduced_by()),
                    })
                }
            } else {
                let v = alive
                    .entry((grandparent.dest(), *parent_dest))
                    .or_insert_with(Vec::new);
                v.push(Some(grandparent.introduced_by()));
                moved.need_new_name = false
            }
        }
    }

    for ((from, to), intro) in del_del {
        if intro.len() > 1 {
            for introduced_by in intro {
                if introduced_by.is_some() {
                    moved.edges.push(NewEdge {
                        previous: EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED,
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED,
                        from: from.to_option(),
                        to: to.to_option(),
                        introduced_by,
                    })
                }
            }
        }
    }

    for ((from, to), intro) in alive {
        if intro.len() > 1 || !moved.resurrect.is_empty() {
            for introduced_by in intro {
                if introduced_by.is_some() {
                    moved.alive.push(NewEdge {
                        previous: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK,
                        from: from.to_option(),
                        to: to.to_option(),
                        introduced_by,
                    })
                }
            }
        }
    }

    Ok(moved)
}

impl Recorded {
    fn record_deleted_file<
        T: GraphTxnT + TreeTxnT<TreeError = <T as GraphTxnT>::GraphError>,
        W: WorkingCopy,
    >(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        working_copy: &W,
        full_path: &str,
        current_vertex: Position<ChangeId>,
    ) -> Result<(), TxnErr<T::GraphError>> {
        debug!("record_deleted_file {:?} {:?}", current_vertex, full_path);
        let mut stack = vec![(current_vertex.inode_vertex(), None)];
        let mut visited = HashSet::default();
        while let Some((vertex, inode)) = stack.pop() {
            debug!("vertex {:?}, inode {:?}", vertex, inode);
            if let Some(path) = tree_path(txn, &vertex.start_pos())? {
                if working_copy.file_metadata(&path).is_ok() {
                    debug!("not deleting {:?}", path);
                    continue;
                }
            }

            // Kill this vertex
            if let Some(inode) = inode {
                self.delete_file_edge(txn, channel, vertex, inode)?
            } else if vertex.start == vertex.end {
                debug!("delete_recursively {:?}", vertex);
                // Killing an inode.
                {
                    let mut deleted_vertices = self.deleted_vertices.lock().unwrap();
                    if !deleted_vertices.insert(vertex.start_pos()) {
                        continue;
                    }
                }
                if let Some(inode) = txn.get_revinodes(&vertex.start_pos(), None)? {
                    debug!(
                        "delete_recursively, vertex = {:?}, inode = {:?}",
                        vertex, inode
                    );
                    self.recorded_inodes
                        .lock()
                        .unwrap()
                        .insert(*inode, vertex.start_pos().to_option());
                    self.updatables
                        .insert(self.actions.len(), InodeUpdate::Deleted { inode: *inode });
                }
                self.delete_inode_vertex(txn, channel, vertex, vertex.start_pos(), full_path)?
            }

            // Move on to the descendants.
            for edge in iter_adjacent(
                txn,
                channel,
                vertex,
                EdgeFlags::empty(),
                EdgeFlags::all() - EdgeFlags::DELETED - EdgeFlags::PARENT,
            )? {
                let edge = edge?;
                debug!("delete_recursively, edge: {:?}", edge);
                let dest = txn
                    .find_block(channel, edge.dest())
                    .expect("delete_recursively, descendants");
                let inode = if inode.is_some() {
                    assert!(!edge.flag().contains(EdgeFlags::FOLDER));
                    inode
                } else if edge.flag().contains(EdgeFlags::FOLDER) {
                    None
                } else {
                    assert_eq!(vertex.start, vertex.end);
                    Some(vertex.start_pos())
                };
                if visited.insert(edge.dest()) {
                    stack.push((*dest, inode))
                }
            }
        }
        Ok(())
    }

    fn delete_inode_vertex<T: GraphTxnT>(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        vertex: Vertex<ChangeId>,
        inode: Position<ChangeId>,
        path: &str,
    ) -> Result<(), TxnErr<T::GraphError>> {
        debug!("delete_inode_vertex {:?}", path);
        let mut edges = Vec::new();
        for parent in iter_adjacent(
            txn,
            channel,
            vertex,
            EdgeFlags::FOLDER | EdgeFlags::PARENT,
            EdgeFlags::all(),
        )? {
            let parent = parent?;
            if !parent.flag().contains(EdgeFlags::PARENT) {
                continue;
            }
            assert!(parent.flag().contains(EdgeFlags::FOLDER));
            let parent_dest = txn.find_block_end(channel, parent.dest()).unwrap();
            for grandparent in iter_adjacent(
                txn,
                channel,
                *parent_dest,
                EdgeFlags::FOLDER | EdgeFlags::PARENT,
                EdgeFlags::all(),
            )? {
                let grandparent = grandparent?;
                if !grandparent.flag().contains(EdgeFlags::PARENT)
                    || grandparent.flag().contains(EdgeFlags::PSEUDO)
                {
                    continue;
                }
                assert!(grandparent.flag().contains(EdgeFlags::PARENT));
                assert!(grandparent.flag().contains(EdgeFlags::FOLDER));
                edges.push(NewEdge {
                    previous: grandparent.flag() - EdgeFlags::PARENT,
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED,
                    from: grandparent.dest().to_option(),
                    to: parent_dest.to_option(),
                    introduced_by: Some(grandparent.introduced_by()),
                });
            }
            if !parent.flag().contains(EdgeFlags::PSEUDO) {
                edges.push(NewEdge {
                    previous: parent.flag() - EdgeFlags::PARENT,
                    flag: EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::DELETED,
                    from: parent.dest().to_option(),
                    to: vertex.to_option(),
                    introduced_by: Some(parent.introduced_by()),
                });
            }
        }
        debug!("deleting {:?}", edges);
        if !edges.is_empty() {
            self.actions.push(Hunk::FileDel {
                del: Atom::EdgeMap(EdgeMap {
                    edges,
                    inode: inode.to_option(),
                }),
                contents: None,
                path: path.to_string(),
            })
        }
        Ok(())
    }

    fn delete_file_edge<T: GraphTxnT>(
        &mut self,
        txn: &T,
        channel: &T::Graph,
        to: Vertex<ChangeId>,
        inode: Position<ChangeId>,
    ) -> Result<(), TxnErr<T::GraphError>> {
        if let Some(Hunk::FileDel {
            ref mut contents, ..
        }) = self.actions.last_mut()
        {
            if contents.is_none() {
                *contents = Some(Atom::EdgeMap(EdgeMap {
                    edges: Vec::new(),
                    inode: inode.to_option(),
                }))
            }
            if let Some(Atom::EdgeMap(mut e)) = contents.take() {
                for parent in iter_adjacent(
                    txn,
                    channel,
                    to,
                    EdgeFlags::PARENT,
                    EdgeFlags::all() - EdgeFlags::DELETED,
                )? {
                    let parent = parent?;
                    if parent.flag().contains(EdgeFlags::PSEUDO) {
                        continue;
                    }
                    assert!(parent.flag().contains(EdgeFlags::PARENT));
                    assert!(!parent.flag().contains(EdgeFlags::FOLDER));
                    e.edges.push(NewEdge {
                        previous: parent.flag() - EdgeFlags::PARENT,
                        flag: (parent.flag() - EdgeFlags::PARENT) | EdgeFlags::DELETED,
                        from: parent.dest().to_option(),
                        to: to.to_option(),
                        introduced_by: Some(parent.introduced_by()),
                    })
                }
                if !e.edges.is_empty() {
                    *contents = Some(Atom::EdgeMap(e))
                }
            }
        }
        Ok(())
    }
}
