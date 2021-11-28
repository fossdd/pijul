use crate::changestore::{ChangeStore, FileMetadata};
use crate::path;
use crate::pristine::*;
use crate::HashMap;

mod output;
pub use output::*;
mod archive;
pub use archive::*;

#[derive(Debug, Error)]
pub enum OutputError<
    ChangestoreError: std::error::Error + 'static,
    Txn: std::error::Error + 'static,
    W: std::error::Error + Send + 'static,
> {
    #[error("Working copy error: {0}")]
    WorkingCopy(W),
    #[error(transparent)]
    Pristine(#[from] PristineOutputError<ChangestoreError, Txn>),
}

#[derive(Debug, Error)]
pub enum PristineOutputError<ChangestoreError: std::error::Error, Txn: std::error::Error + 'static>
{
    #[error(transparent)]
    Txn(Txn),
    #[error("Changestore error: {0}")]
    Changestore(ChangestoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fs(#[from] crate::fs::FsError<Txn>),
}

impl<C: std::error::Error, T: std::error::Error + 'static> From<TxnErr<T>>
    for PristineOutputError<C, T>
{
    fn from(e: TxnErr<T>) -> Self {
        PristineOutputError::Txn(e.0)
    }
}

impl<C: std::error::Error, T: std::error::Error + 'static, W: std::error::Error + Send>
    From<TxnErr<T>> for OutputError<C, T, W>
{
    fn from(e: TxnErr<T>) -> Self {
        OutputError::Pristine(e.into())
    }
}

#[derive(Debug, Error)]
pub enum FileError<ChangestoreError: std::error::Error + 'static, T: std::error::Error + 'static> {
    #[error(transparent)]
    Changestore(ChangestoreError),
    #[error(transparent)]
    Txn(T),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl<C: std::error::Error, T: std::error::Error + 'static> From<FileError<C, T>>
    for PristineOutputError<C, T>
{
    fn from(e: FileError<C, T>) -> Self {
        match e {
            FileError::Changestore(e) => PristineOutputError::Changestore(e),
            FileError::Io(e) => PristineOutputError::Io(e),
            FileError::Txn(t) => PristineOutputError::Txn(t),
        }
    }
}

impl<C: std::error::Error, T: std::error::Error + 'static> From<TxnErr<T>> for FileError<C, T> {
    fn from(e: TxnErr<T>) -> Self {
        FileError::Txn(e.0)
    }
}

#[derive(Debug, Clone)]
struct OutputItem {
    parent: Inode,
    path: String,
    tmp: Option<String>,
    meta: InodeMetadata,
    pos: Position<ChangeId>,
    is_zombie: bool,
}

fn collect_children<T: GraphTxnT, P: ChangeStore>(
    txn: &T,
    changes: &P,
    channel: &T::Graph,
    inode_pos: Position<ChangeId>,
    inode: Inode,
    path: &str,
    tmp: Option<&str>,
    prefix_basename: Option<&str>,
    files: &mut HashMap<String, Vec<(Vertex<ChangeId>, OutputItem)>>,
) -> Result<(), PristineOutputError<P::Error, T::GraphError>> {
    debug!("path = {:?}, inode_pos = {:?}", path, inode_pos);
    debug!("prefix_basename = {:?}", prefix_basename);
    for e in iter_adjacent(
        txn,
        channel,
        inode_pos.inode_vertex(),
        EdgeFlags::FOLDER,
        EdgeFlags::FOLDER | EdgeFlags::PSEUDO | EdgeFlags::BLOCK,
    )? {
        let e = e?;
        debug!("e = {:?}", e);
        let name_vertex = txn.find_block(channel, e.dest()).unwrap();
        if name_vertex.start != name_vertex.end {
            debug!("name_vertex: {:?} {:?}", e, name_vertex);
            collect(
                txn,
                changes,
                channel,
                inode,
                path,
                tmp,
                prefix_basename,
                files,
                name_vertex,
            )?
        } else {
            let inode_pos = iter_adjacent(
                txn,
                channel,
                *name_vertex,
                EdgeFlags::FOLDER,
                EdgeFlags::FOLDER | EdgeFlags::PSEUDO | EdgeFlags::BLOCK,
            )?
            .next()
            .unwrap()?
            .dest();
            for e in iter_adjacent(
                txn,
                channel,
                inode_pos.inode_vertex(),
                EdgeFlags::FOLDER,
                EdgeFlags::FOLDER | EdgeFlags::PSEUDO | EdgeFlags::BLOCK,
            )? {
                let e = e?;
                debug!("e' = {:?}", e);
                let name_vertex = txn.find_block(channel, e.dest()).unwrap();
                collect(
                    txn,
                    changes,
                    channel,
                    inode,
                    path,
                    tmp,
                    prefix_basename,
                    files,
                    name_vertex,
                )?
            }
        }
    }
    Ok(())
}

fn collect<T: GraphTxnT, P: ChangeStore>(
    txn: &T,
    changes: &P,
    channel: &T::Graph,
    inode: Inode,
    path: &str,
    tmp: Option<&str>,
    prefix_basename: Option<&str>,
    files: &mut HashMap<String, Vec<(Vertex<ChangeId>, OutputItem)>>,
    name_vertex: &Vertex<ChangeId>,
) -> Result<(), PristineOutputError<P::Error, T::GraphError>> {
    // First, get the basename of the path we're outputting.
    let mut name_buf = Vec::new();
    let FileMetadata {
        basename,
        metadata: perms,
        ..
    } = changes
        .get_file_meta(
            |h| txn.get_external(&h).unwrap().map(|x| x.into()),
            *name_vertex,
            &mut name_buf,
        )
        .map_err(PristineOutputError::Changestore)?;
    debug!("filename: {:?} {:?}", perms, basename);
    let mut name = path.to_string();
    if let Some(next) = prefix_basename {
        if next != basename {
            debug!("next = {:?} basename = {:?}", next, basename);
            return Ok(());
        }
    }
    path::push(&mut name, basename);
    let child = if let Some(child) = iter_adjacent(
        txn,
        channel,
        *name_vertex,
        EdgeFlags::FOLDER,
        EdgeFlags::FOLDER | EdgeFlags::BLOCK | EdgeFlags::PSEUDO,
    )?
    .next()
    {
        child?
    } else {
        let mut edge = None;
        for e in iter_adjacent(
            txn,
            channel,
            *name_vertex,
            EdgeFlags::FOLDER,
            EdgeFlags::all(),
        )? {
            let e = e?;
            if !e.flag().contains(EdgeFlags::PARENT) {
                edge = Some(e);
                break;
            }
        }
        let e = edge.unwrap();
        let mut f = std::fs::File::create("debug_output").unwrap();
        debug_root(txn, channel, e.dest().inode_vertex(), &mut f, false).unwrap();
        panic!("no child");
    };

    debug!("child: {:?}", child);
    let v = files.entry(name).or_insert_with(Vec::new);
    v.push((
        *name_vertex,
        OutputItem {
            parent: inode,
            path: path.to_string(),
            tmp: tmp.map(String::from),
            meta: perms,
            pos: child.dest(),
            is_zombie: is_zombie(txn, channel, child.dest())?,
        },
    ));
    Ok(())
}

fn is_zombie<T: GraphTxnT>(
    txn: &T,
    channel: &T::Graph,
    pos: Position<ChangeId>,
) -> Result<bool, TxnErr<T::GraphError>> {
    let f = EdgeFlags::FOLDER | EdgeFlags::PARENT | EdgeFlags::DELETED;
    if let Some(n) =
        iter_adjacent(txn, channel, pos.inode_vertex(), f, f | EdgeFlags::BLOCK)?.next()
    {
        n?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub fn output_file<
    T: TreeTxnT + ChannelTxnT,
    C: crate::changestore::ChangeStore,
    V: crate::vertex_buffer::VertexBuffer,
>(
    changes: &C,
    txn: &T,
    channel: &T::Channel,
    v0: Position<ChangeId>,
    out: &mut V,
) -> Result<(), FileError<C::Error, T::GraphError>> {
    let mut forward = Vec::new();
    let mut graph = crate::alive::retrieve(&*txn, txn.graph(&*channel), v0)?;
    crate::alive::output_graph(changes, &*txn, &*channel, out, &mut graph, &mut forward)?;
    Ok(())
}
