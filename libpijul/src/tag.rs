use crate::pristine::sanakirja::{Channel, MutTxn, SanakirjaError, P, UP};
use crate::pristine::*;
use crate::HashSet;
use crate::TxnT;
use log::*;
use parking_lot::RwLock;
use serde_derive::*;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Default)]
struct FileHeader {
    version: u64,
    header: u64,
    channel: u64,
    unhashed: u64,
    total: u64,
    offsets: DbOffsets,
    state: Merkle,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DbOffsets {
    internal: u64,
    external: u64,
    graph: u64,
    changes: u64,
    revchanges: u64,
    states: u64,
    tags: u64,
    apply_counter: u64,
    size: u64,
}

pub struct OpenTagFile {
    header: FileHeader,
    file: std::fs::File,
}

#[derive(Debug, Error)]
pub enum TagError {
    #[error("Version mismatch")]
    VersionMismatch,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error(transparent)]
    Zstd(#[from] zstd_seekable::Error),
    #[error(transparent)]
    Txn(SanakirjaError),
    #[error("Synchronisation error")]
    Sync,
}

impl From<TxnErr<SanakirjaError>> for TagError {
    fn from(e: TxnErr<SanakirjaError>) -> Self {
        TagError::Txn(e.0)
    }
}

impl OpenTagFile {
    pub fn open<P: AsRef<Path>>(p: P) -> Result<Self, TagError> {
        let mut file = std::fs::File::open(p)?;
        let mut off = [0u8; std::mem::size_of::<FileHeader>() as usize];
        file.read_exact(&mut off)?;
        let header = bincode::deserialize(&off)?;
        Ok(OpenTagFile { header, file })
    }

    pub fn header(&mut self) -> Result<crate::change::ChangeHeader, TagError> {
        use std::io::{Seek, SeekFrom};
        self.file.seek(SeekFrom::Start(self.header.header))?;
        Ok(bincode::deserialize_from(&mut self.file)?)
    }

    pub fn state(&self) -> Merkle {
        self.header.state.clone()
    }
}

pub const VERSION: u64 = 7;
pub const VERSION_NOENC: u64 = 5;

const BLOCK_SIZE: usize = 4096;

pub fn restore_channel(
    mut tag: OpenTagFile,
    txn: &mut MutTxn<()>,
    name: &str,
) -> Result<ChannelRef<MutTxn<()>>, TagError> {
    use std::io::{Seek, SeekFrom};
    tag.file.seek(SeekFrom::Start(tag.header.channel))?;
    let mut comp = vec![0; (tag.header.unhashed - tag.header.channel) as usize];
    debug!("tag header {:?}", tag.header);
    tag.file.read_exact(&mut comp)?;
    debug!("{:?} {:?}", &comp[..20], comp.len());
    debug!("{:?}", &comp[comp.len() - 20..]);
    let mut buf = vec![0; tag.header.offsets.size as usize];
    zstd_seekable::Seekable::init_buf(&comp)?
        .decompress(&mut buf, 0)
        .unwrap();
    let filetxn = Txn::from_slice(&mut buf);
    let external: ::sanakirja::btree::Db_<ChangeId, SerializedHash, UP<ChangeId, SerializedHash>> =
        ::sanakirja::btree::Db_::from_page(tag.header.offsets.external);
    let mut vi = Vec::new();
    for i in ::sanakirja::btree::iter(&filetxn, &external, None).unwrap() {
        debug!("{:?}", i);
        vi.push(i.unwrap());
    }
    vi.sort();
    debug!("restoring graph {:?}", vi);
    let graph = restore(
        &filetxn,
        txn,
        tag.header.offsets.graph,
        |file_txn, txn, k: &Vertex<ChangeId>, v: &SerializedEdge| {
            let k = if k.change.is_root() {
                *k
            } else {
                debug!("btree get: {:?}", k.change);
                let (kc, h) =
                    ::sanakirja::btree::get(file_txn, &external, &k.change, None)?.unwrap();
                assert_eq!(k.change, *kc);
                Vertex {
                    change: crate::pristine::make_changeid(txn, &h.into())?,
                    ..*k
                }
            };

            let dest = v.dest();
            let dest = {
                if dest.change.is_root() {
                    dest
                } else {
                    let (vd, change) =
                        ::sanakirja::btree::get(file_txn, &external, &dest.change, None)?.unwrap();
                    assert_eq!(v.dest().change, *vd);
                    Position {
                        change: crate::pristine::make_changeid(txn, &change.into())?,
                        ..v.dest()
                    }
                }
            };
            let introduced_by = v.introduced_by();
            let introduced_by = if introduced_by.is_root() {
                introduced_by
            } else {
                let (vi, change) =
                    ::sanakirja::btree::get(file_txn, &external, &introduced_by, None)?.unwrap();
                assert_eq!(introduced_by, *vi);
                crate::pristine::make_changeid(txn, &change.into())?
            };
            let v = Edge {
                dest,
                introduced_by,
                ..v.into()
            };
            Ok((k, v.into()))
        },
    )?;

    debug!("restoring changes");
    let changes = restore(
        &filetxn,
        txn,
        tag.header.offsets.changes,
        |file_txn, txn, k: &ChangeId, v: &L64| {
            let (k_, h) = ::sanakirja::btree::get(file_txn, &external, k, None)?.unwrap();
            assert_eq!(k, k_);
            let k = crate::pristine::make_changeid(txn, &h.into())?;
            Ok((k, *v))
        },
    )?;

    debug!("restoring revchanges");
    let revchanges = restore(
        &filetxn,
        txn,
        tag.header.offsets.revchanges,
        |file_txn, txn, k: &L64, v: &Pair<ChangeId, SerializedMerkle>| {
            let (v0, h) = ::sanakirja::btree::get(file_txn, &external, &v.a, None)?.unwrap();
            assert_eq!(v.a, *v0);
            let v_ = crate::pristine::make_changeid(txn, &h.into())?;
            Ok((
                *k,
                Pair {
                    a: v_,
                    b: v.b.clone(),
                },
            ))
        },
    )?;

    debug!("restoring states");
    let states = restore(
        &filetxn,
        txn,
        tag.header.offsets.states,
        |_, _, k: &SerializedMerkle, v: &L64| Ok((k.clone(), *v)),
    )?;

    debug!("restoring states");
    let tags = restore(
        &filetxn,
        txn,
        tag.header.offsets.tags,
        |_, _, k: &L64, v: &SerializedHash| Ok((*k, *v)),
    )?;

    let name = crate::small_string::SmallString::from_str(name);
    let br = ChannelRef {
        r: Arc::new(RwLock::new(Channel {
            graph,
            changes,
            revchanges,
            states,
            tags,
            apply_counter: tag.header.offsets.apply_counter,
            name: name.clone(),
            last_modified: 0,
            id: {
                let mut rng = rand::thread_rng();
                use rand::Rng;
                let mut m = crate::pristine::RemoteId([0; 16]);
                for m in m.0.iter_mut() {
                    *m = rng.gen()
                }
                m
            },
        })),
    };
    txn.open_channels.lock().insert(name, br.clone());
    Ok(br)
}

struct Txn<'a> {
    data: *mut u8,
    marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Txn<'a> {
    fn from_slice(s: &'a mut [u8]) -> Self {
        Txn {
            data: s.as_mut_ptr(),
            marker: std::marker::PhantomData,
        }
    }
}

impl<'a> ::sanakirja::LoadPage for Txn<'a> {
    type Error = ::sanakirja::CRCError;
    fn load_page(&self, off: u64) -> Result<::sanakirja::CowPage, ::sanakirja::CRCError> {
        Ok(::sanakirja::CowPage {
            data: unsafe { self.data.add(off as usize) },
            offset: off,
        })
    }
}

fn restore<
    K: ::sanakirja::UnsizedStorable + PartialEq,
    V: ::sanakirja::UnsizedStorable + PartialEq,
    P: ::sanakirja::btree::BTreeMutPage<K, V>,
    F,
>(
    file_txn: &Txn,
    txn: &mut crate::pristine::sanakirja::MutTxn<()>,
    pending: u64,
    f: F,
) -> Result<::sanakirja::btree::Db_<K, V, P>, TxnErr<SanakirjaError>>
where
    F: Fn(
        &Txn,
        &mut crate::pristine::sanakirja::MutTxn<()>,
        &K,
        &V,
    ) -> Result<(K, V), TxnErr<SanakirjaError>>,
{
    use ::sanakirja::AllocPage;
    let mut dict = HashSet::default();
    let page = txn.txn.alloc_page()?;
    let result = page.0.offset;
    let mut pending = vec![(pending, page)];
    while let Some((offset, mut new_page_)) = pending.pop() {
        debug!("{:?}", offset);
        let page = ::sanakirja::CowPage {
            data: unsafe { file_txn.data.offset(offset as isize) },
            offset,
        };
        let mut curs = P::cursor_first(&page);
        let mut new_curs = P::cursor_first(&new_page_.0);
        P::init(&mut new_page_);
        unsafe {
            let left = P::left_child(page.as_page(), &curs);
            if left != 0 {
                assert!(dict.insert(left));
                let new_page = txn.txn.alloc_page()?;
                let off = new_page.0.offset;
                P::set_left_child(&mut new_page_, &new_curs, off);
                pending.push((left, new_page));
            }
        }
        while let Some((k_, v_, r)) = P::next(&txn.txn, page.as_page(), &mut curs) {
            let (k, v) = f(file_txn, txn, k_, v_)?;
            assert_eq!(&k, k_);
            assert_eq!(&v, v_);
            let r = if r > 0 {
                assert!(dict.insert(r));
                let new_page = txn.txn.alloc_page()?;
                let off = new_page.0.offset;
                pending.push((r, new_page));
                off
            } else {
                0
            };
            unsafe { P::put_mut(&mut new_page_, &mut new_curs, &k, &v, r) }
            P::move_next(&mut new_curs);
        }
    }
    Ok(::sanakirja::btree::Db_::from_page(result))
}

pub fn from_channel<
    W: std::io::Write,
    T: ::sanakirja::LoadPage<Error = ::sanakirja::Error> + ::sanakirja::RootPage,
>(
    txn: &crate::pristine::sanakirja::GenericTxn<T>,
    channel: &str,
    header: &crate::change::ChangeHeader,
    mut w: W,
) -> Result<Hash, TagError> {
    let out = Vec::with_capacity(1 << 16);
    let (out, offsets, state) = compress_channel(txn, channel, out)?;
    debug!("{:?} {:?}", &out[..20], out.len());
    debug!("{:?}", &out[out.len() - 20..]);
    let mut header_buf = Vec::with_capacity(1 << 10);
    bincode::serialize_into(&mut header_buf, header).unwrap();

    let mut off = FileHeader {
        version: VERSION,
        header: 0,
        channel: 0,
        unhashed: 0,
        total: 0,
        offsets,
        state,
    };
    off.header = bincode::serialized_size(&off)?;
    off.channel = off.header + header_buf.len() as u64;
    off.unhashed = off.channel + out.len() as u64;
    off.total = off.unhashed;
    let mut hasher = Hasher::default();
    let mut off_buf = Vec::with_capacity(off.header as usize);
    bincode::serialize_into(&mut off_buf, &off)?;
    debug!("off_buf = {:?}", off_buf.len());
    w.write_all(&off_buf)?;
    hasher.update(&off_buf);
    debug!("header_buf = {:?}", header_buf.len());
    w.write_all(&header_buf)?;
    hasher.update(&header_buf);
    debug!("out = {:?}", out.len());
    w.write_all(&out)?;
    hasher.update(&out);
    Ok(hasher.finish())
}

const LEVEL: usize = 10;
const PIPE_LEN: usize = 10;

fn compress_channel<
    W: std::io::Write + Send + 'static,
    T: ::sanakirja::LoadPage<Error = ::sanakirja::Error> + ::sanakirja::RootPage,
>(
    txn: &crate::pristine::sanakirja::GenericTxn<T>,
    channel: &str,
    mut to: W,
) -> Result<(W, DbOffsets, Merkle), TagError> {
    debug!("int = {:?}", txn.internal.db);
    let (sender, receiver) = std::sync::mpsc::sync_channel::<Vec<u8>>(PIPE_LEN);
    let (bsender, breceiver) = std::sync::mpsc::sync_channel::<Vec<u8>>(PIPE_LEN);
    for _ in 0..PIPE_LEN {
        bsender.send(vec![0; 4096]).map_err(|_| TagError::Sync)?;
    }

    let t = std::thread::spawn(move || -> Result<(W, usize), TagError> {
        let mut comp = zstd_seekable::SeekableCStream::new(LEVEL, BLOCK_SIZE).unwrap();
        let mut out = [0; BLOCK_SIZE];
        let mut n = 0;
        while let Ok(input) = receiver.recv() {
            n += BLOCK_SIZE;
            let mut input_off = 0;
            let mut output_off = 0;
            while input_off < BLOCK_SIZE as usize {
                let (a, b) = comp
                    .compress(&mut out[output_off..], &input[input_off..])
                    .unwrap();
                output_off += a;
                input_off += b;
            }
            to.write_all(&out[..output_off]).unwrap();
            bsender.send(input).map_err(|_| TagError::Sync)?;
        }
        while let Ok(n) = comp.end_stream(&mut out) {
            if n == 0 {
                break;
            }
            to.write_all(&out[..n])?;
        }
        Ok((to, n))
    });
    let channel = txn.load_channel(channel)?.unwrap();
    let channel = channel.read();
    let mut new = 0;
    debug!("copying internal");
    let internal = copy::<SerializedHash, ChangeId, UP<SerializedHash, ChangeId>, _>(
        txn,
        txn.internal.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    debug!("copying external");
    let external = copy::<ChangeId, SerializedHash, UP<ChangeId, SerializedHash>, _>(
        txn,
        txn.external.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    debug!("copying graph");
    let graph = copy::<Vertex<ChangeId>, SerializedEdge, P<Vertex<ChangeId>, SerializedEdge>, _>(
        txn,
        channel.graph.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    debug!("copying changes");
    let changes = copy::<ChangeId, L64, P<ChangeId, L64>, _>(
        txn,
        channel.changes.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    debug!("copying revchanges");
    let revchanges = copy::<
        L64,
        Pair<ChangeId, SerializedMerkle>,
        UP<L64, Pair<ChangeId, SerializedMerkle>>,
        _,
    >(&txn, channel.revchanges.db, &mut new, &sender, &breceiver)?;
    debug!("copying states");
    let states = copy::<SerializedMerkle, L64, UP<SerializedMerkle, L64>, _>(
        txn,
        channel.states.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    let tags = copy::<L64, SerializedHash, UP<L64, SerializedHash>, _>(
        &txn,
        channel.states.db,
        &mut new,
        &sender,
        &breceiver,
    )?;
    std::mem::drop(sender);
    let (w, n) = t.join().unwrap()?;
    let state = crate::pristine::current_state(txn, &channel)?;
    Ok((
        w,
        DbOffsets {
            internal,
            external,
            graph,
            changes,
            revchanges,
            states,
            tags: tags,
            apply_counter: channel.apply_counter,
            size: n as u64,
        },
        state,
    ))
}

fn copy<
    K: ::sanakirja::UnsizedStorable,
    V: ::sanakirja::UnsizedStorable,
    P: ::sanakirja::btree::BTreeMutPage<K, V>,
    T: ::sanakirja::LoadPage<Error = ::sanakirja::Error> + ::sanakirja::RootPage,
>(
    txn: &crate::pristine::sanakirja::GenericTxn<T>,
    pending_: u64,
    new_page: &mut u64,
    sender: &std::sync::mpsc::SyncSender<Vec<u8>>,
    buffers: &std::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<u64, TagError> {
    let mut dict = HashSet::default();
    let result = *new_page;
    let mut pending = std::collections::VecDeque::new();
    pending.push_back((pending_, *new_page));
    *new_page += BLOCK_SIZE as u64;
    while let Some((old_page_off, new_page_)) = pending.pop_front() {
        let page = txn.txn.load_page(old_page_off).unwrap();
        let mut memory = buffers.recv().map_err(|_| TagError::Sync)?;
        let mut new_page_ = ::sanakirja::MutPage(::sanakirja::CowPage {
            data: memory.as_mut_ptr(),
            offset: new_page_,
        });
        P::init(&mut new_page_);
        let mut curs = P::cursor_first(&page);
        let mut new_curs = P::cursor_first(&new_page_.0);
        unsafe {
            let left = P::left_child(page.as_page(), &curs);
            if left != 0 {
                let new = *new_page;
                *new_page += BLOCK_SIZE as u64;
                P::set_left_child(&mut new_page_, &new_curs, new);
                pending.push_back((left, new));
            }
        }
        while let Some((k, v, r)) = P::next(&txn.txn, page.as_page(), &mut curs) {
            let r = if r > 0 {
                assert!(dict.insert(r));
                let new = *new_page;
                *new_page += BLOCK_SIZE as u64;
                pending.push_back((r, new));
                unsafe {
                    P::set_left_child(&mut new_page_, &curs, new);
                }
                new
            } else {
                0
            };
            debug!("put {:?} {:?}", k, v);
            unsafe { P::put_mut(&mut new_page_, &mut new_curs, k, v, r) }
            P::move_next(&mut new_curs);
        }
        sender.send(memory).unwrap();
    }
    Ok(result)
}
