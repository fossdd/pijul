use super::*;
use crate::changestore::ChangeStore;
use crate::Conflict;
use crate::{HashMap, HashSet};
use std::collections::hash_map::Entry;

pub trait Archive {
    type File: std::io::Write;
    type Error: std::error::Error;
    fn create_file(&mut self, path: &str, mtime: u64, perm: u16) -> Self::File;
    fn create_dir(&mut self, path: &str, mtime: u64, permissions: u16) -> Result<(), Self::Error>;
    fn close_file(&mut self, f: Self::File) -> Result<(), Self::Error>;
}

#[cfg(feature = "tarball")]
pub struct Tarball<W: std::io::Write> {
    pub archive: tar::Builder<flate2::write::GzEncoder<W>>,
    pub prefix: Option<String>,
    pub buffer: Vec<u8>,
    pub umask: u16,
}

#[cfg(feature = "tarball")]
pub struct File {
    buf: Vec<u8>,
    path: String,
    permissions: u16,
    mtime: u64,
}

#[cfg(feature = "tarball")]
impl std::io::Write for File {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.buf.write(buf)
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[cfg(feature = "tarball")]
impl<W: std::io::Write> Tarball<W> {
    pub fn new(w: W, prefix: Option<String>, umask: u16) -> Self {
        let encoder = flate2::write::GzEncoder::new(w, flate2::Compression::best());
        Tarball {
            archive: tar::Builder::new(encoder),
            buffer: Vec::new(),
            prefix,
            umask,
        }
    }
}

#[cfg(feature = "tarball")]
impl<W: std::io::Write> Archive for Tarball<W> {
    type File = File;
    type Error = std::io::Error;
    fn create_file(&mut self, path: &str, mtime: u64, permissions: u16) -> Self::File {
        self.buffer.clear();
        File {
            buf: std::mem::replace(&mut self.buffer, Vec::new()),
            path: if let Some(ref prefix) = self.prefix {
                prefix.clone() + path
            } else {
                path.to_string()
            },
            mtime,
            permissions: permissions & !self.umask,
        }
    }
    fn create_dir(&mut self, path: &str, mtime: u64, permissions: u16) -> Result<(), Self::Error> {
        let mut header = tar::Header::new_gnu();
        header.set_mode((permissions & !self.umask) as u32);
        header.set_mtime(mtime);
        header.set_entry_type(tar::EntryType::Directory);
        if let Some(ref prefix) = self.prefix {
            let path = prefix.clone() + path;
            self.archive.append_data(&mut header, &path, &[][..])?;
        } else {
            self.archive.append_data(&mut header, &path, &[][..])?;
        }
        Ok(())
    }

    fn close_file(&mut self, file: Self::File) -> Result<(), Self::Error> {
        let mut header = tar::Header::new_gnu();
        header.set_size(file.buf.len() as u64);
        header.set_mode(file.permissions as u32);
        header.set_mtime(file.mtime);
        header.set_cksum();
        self.archive
            .append_data(&mut header, &file.path, &file.buf[..])?;
        self.buffer = file.buf;
        Ok(())
    }
}

#[derive(Error)]
pub enum ArchiveError<
    P: std::error::Error + 'static,
    T: GraphTxnT + TreeTxnT,
    A: std::error::Error + 'static,
> {
    #[error(transparent)]
    A(A),
    #[error(transparent)]
    P(P),
    #[error(transparent)]
    Txn(#[from] TxnErr<T::GraphError>),
    #[error(transparent)]
    Tree(#[from] TreeErr<T::TreeError>),
    #[error(transparent)]
    Unrecord(#[from] crate::unrecord::UnrecordError<P, T>),
    #[error(transparent)]
    Apply(#[from] crate::apply::ApplyError<P, T>),
    #[error("State not found: {:?}", state)]
    StateNotFound { state: crate::pristine::Merkle },
    #[error(transparent)]
    File(#[from] crate::output::FileError<P, T>),
    #[error(transparent)]
    Output(#[from] crate::output::PristineOutputError<P, T>),
}

impl<P: std::error::Error + 'static, T: GraphTxnT + TreeTxnT, A: std::error::Error + 'static>
    std::fmt::Debug for ArchiveError<P, T, A>
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ArchiveError::A(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::P(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::Txn(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::Tree(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::Unrecord(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::Apply(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::File(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::Output(e) => std::fmt::Debug::fmt(e, fmt),
            ArchiveError::StateNotFound { state } => write!(fmt, "State not found: {:?}", state),
        }
    }
}

pub(crate) fn archive<
    'a,
    T: ChannelTxnT + TreeTxnT + DepsTxnT<DepsError = <T as GraphTxnT>::GraphError>,
    P: ChangeStore,
    I: Iterator<Item = &'a str>,
    A: Archive,
>(
    changes: &P,
    txn: &ArcTxn<T>,
    channel: &ChannelRef<T>,
    prefix: &mut I,
    arch: &mut A,
) -> Result<Vec<Conflict>, ArchiveError<P::Error, T, A::Error>> {
    let mut conflicts = Vec::new();
    let mut files = HashMap::default();
    let mut next_files = HashMap::default();
    let mut next_prefix_basename = prefix.next();
    {
        let txn_ = txn.read();
        let channel_ = channel.read();
        collect_children(
            &*txn_,
            changes,
            txn_.graph(&channel_),
            Position::ROOT,
            Inode::ROOT,
            "",
            None,
            next_prefix_basename,
            &mut files,
        )?;
    }
    let mut done: HashMap<_, (Vertex<ChangeId>, String)> = HashMap::default();
    let mut done_inodes = HashSet::default();
    while !files.is_empty() {
        debug!("files {:?}", files.len());
        next_files.clear();
        next_prefix_basename = prefix.next();

        for (a, mut b) in files.drain() {
            debug!("files: {:?} {:?}", a, b);
            {
                let txn_ = txn.read();
                let channel_ = channel.read();
                b.sort_by(|u, v| {
                    txn_.get_changeset(txn_.changes(&channel_), &u.0.change)
                        .unwrap()
                        .cmp(
                            &txn_
                                .get_changeset(txn_.changes(&channel_), &v.0.change)
                                .unwrap(),
                        )
                });
            }
            let mut is_first_name = true;
            for (name_key, mut output_item) in b {
                let txn_ = txn.read();
                let channel_ = channel.read();
                let name_entry = match done.entry(output_item.pos) {
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
                if !done_inodes.insert(output_item.pos) {
                    debug!("inode already visited: {:?} {:?}", a, output_item.pos);
                    continue;
                }
                let name = if !is_first_name {
                    conflicts.push(Conflict::Name {
                        path: a.to_string(),
                    });
                    break;
                } else {
                    is_first_name = false;
                    a.clone()
                };
                let file_name = path::file_name(&name).unwrap();
                path::push(&mut output_item.path, file_name);

                name_entry.insert((name_key, output_item.path.clone()));

                let path = std::mem::replace(&mut output_item.path, String::new());
                let (_, latest_touch) =
                    crate::fs::get_latest_touch(&*txn_, &channel_, &output_item.pos)?;
                let latest_touch = {
                    let ext = txn_.get_external(&latest_touch)?.unwrap();
                    let c = changes.get_header(&ext.into()).map_err(ArchiveError::P)?;
                    c.timestamp.timestamp() as u64
                };
                if output_item.meta.is_dir() {
                    let len = next_files.len();
                    collect_children(
                        &*txn_,
                        changes,
                        txn_.graph(&channel_),
                        output_item.pos,
                        Inode::ROOT, // unused
                        &path,
                        None,
                        next_prefix_basename,
                        &mut next_files,
                    )?;
                    if len == next_files.len() {
                        arch.create_dir(&path, latest_touch, 0o777)
                            .map_err(ArchiveError::A)?;
                    }
                } else {
                    debug!("latest_touch: {:?}", latest_touch);
                    let mut l =
                        crate::alive::retrieve(&*txn_, txn_.graph(&channel_), output_item.pos)?;
                    let perms = if output_item.meta.permissions() & 0o100 != 0 {
                        0o777
                    } else {
                        0o666
                    };
                    let mut f = arch.create_file(&path, latest_touch, perms);
                    {
                        let mut f = crate::vertex_buffer::ConflictsWriter::new(
                            &mut f,
                            &output_item.path,
                            &mut conflicts,
                        );
                        std::mem::drop(channel_);
                        std::mem::drop(txn_);
                        crate::alive::output_graph(
                            changes,
                            txn,
                            channel,
                            &mut f,
                            &mut l,
                            &mut Vec::new(),
                        )?;
                    }
                    arch.close_file(f).map_err(ArchiveError::A)?;
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
    Ok(conflicts)
}
