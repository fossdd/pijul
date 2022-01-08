use super::*;
use crate::small_string::SmallStr;
use std::collections::HashMap;
use std::sync::Mutex;

struct WithOffset<R> {
    off: u64,
    r: R,
}

use std::io::{Seek, SeekFrom};

impl<R: Read> Read for WithOffset<R> {
    fn read(&mut self, b: &mut [u8]) -> Result<usize, std::io::Error> {
        self.r.read(b)
    }
}

impl<R: Seek> Seek for WithOffset<R> {
    fn seek(&mut self, s: SeekFrom) -> Result<u64, std::io::Error> {
        let s = match s {
            SeekFrom::Start(s) => SeekFrom::Start(s + self.off),
            x => x,
        };
        self.r.seek(s)
    }
}

pub struct TagTxn {
    pub(crate) header: FileHeader,
    s: Mutex<zstd_seekable::Seekable<'static, WithOffset<std::fs::File>>>,
    loaded: Mutex<HashMap<u64, Box<[u8; crate::tag::BLOCK_SIZE]>>>,
}

impl std::convert::From<BlockError<::zstd_seekable::Error>> for BlockError<TagError> {
    fn from(e: BlockError<::zstd_seekable::Error>) -> Self {
        match e {
            BlockError::Txn(t) => BlockError::Txn(t.into()),
            BlockError::Block { block } => BlockError::Block { block },
        }
    }
}

impl From<zstd_seekable::Error> for TxnErr<TagError> {
    fn from(e: zstd_seekable::Error) -> Self {
        TxnErr(TagError::Zstd(e))
    }
}

impl TagTxn {
    pub fn new<P: AsRef<std::path::Path>>(p: P, expected: &Merkle) -> Result<Self, TagError> {
        let mut file = std::fs::File::open(p)?;
        let mut off = [0u8; std::mem::size_of::<crate::tag::FileHeader>() as usize];
        file.read_exact(&mut off)?;
        let header: crate::tag::FileHeader =
            bincode::deserialize(&off).map_err(TagError::BincodeDe)?;
        if &header.state != expected {
            return Err(TagError::WrongHash {
                expected: *expected,
                got: header.state,
            });
        }
        let mut ch = OpenTagFile { file, header };
        ch.file.seek(SeekFrom::Start(off.len() as u64))?;
        let s = zstd_seekable::Seekable::init(Box::new(WithOffset {
            r: ch.file,
            off: ch.header.channel,
        }))?;
        Ok(TagTxn {
            header: ch.header,
            loaded: Mutex::new(HashMap::new()),
            s: Mutex::new(s),
        })
    }

    pub fn channel(&self) -> ChannelRef<Self> {
        ChannelRef::new(self.header.offsets.clone())
    }

    /// Clear the cache, freeing memory.
    pub fn clear(&mut self) {
        // This function is only safe because it takes a mutable
        // borrow, and all references returned by the methods on
        // `TagTxn` return immutable borrows of `self`.
        self.loaded.lock().unwrap().clear()
    }
}

impl ::sanakirja::LoadPage for TagTxn {
    type Error = zstd_seekable::Error;
    fn load_page(&self, off: u64) -> Result<::sanakirja::CowPage, Self::Error> {
        use ::sanakirja::CowPage;
        let off_aligned = (off / crate::tag::BLOCK_SIZE as u64) * crate::tag::BLOCK_SIZE as u64;
        let mut l = self.loaded.lock().unwrap();
        let p = if let Some(p) = l.get_mut(&off_aligned) {
            unsafe { p.as_mut_ptr().add((off - off_aligned) as usize) }
        } else {
            let mut buf = Box::new([0; crate::tag::BLOCK_SIZE]);
            self.s
                .lock()
                .unwrap()
                .decompress(&mut buf[..], off_aligned)?;
            let p = unsafe { buf.as_mut_ptr().add((off - off_aligned) as usize) };
            l.insert(off_aligned, buf);
            p
        };
        Ok(CowPage {
            data: p,
            offset: off,
        })
    }
}

impl GraphTxnT for TagTxn {
    type Graph = DbOffsets;
    type GraphError = TagError;

    fn get_graph<'txn>(
        &'txn self,
        db: &Self::Graph,
        key: &Vertex<ChangeId>,
        value: Option<&SerializedEdge>,
    ) -> Result<Option<&'txn SerializedEdge>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::Db;
        let gr: Db<Vertex<ChangeId>, SerializedEdge> = Db::from_page(db.graph);
        Ok(::sanakirja::btree::get(self, &gr, key, value)?.map(|(_, v)| v))
    }

    fn get_external<'txn>(
        &'txn self,
        int: &ChangeId,
    ) -> Result<Option<&'txn SerializedHash>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::UDb;
        let gr: UDb<ChangeId, SerializedHash> = UDb::from_page(self.header.offsets.external);
        Ok(::sanakirja::btree::get(self, &gr, int, None)?.map(|(_, v)| v))
    }

    fn get_internal<'txn>(
        &'txn self,
        int: &SerializedHash,
    ) -> Result<Option<&'txn ChangeId>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::UDb;
        let gr: UDb<SerializedHash, ChangeId> = UDb::from_page(self.header.offsets.internal);
        Ok(::sanakirja::btree::get(self, &gr, int, None)?.map(|(_, v)| v))
    }

    type Adj = crate::pristine::sanakirja::Adj;

    fn init_adj(
        &self,
        g: &Self::Graph,
        key: Vertex<ChangeId>,
        dest: Position<ChangeId>,
        min_flag: EdgeFlags,
        max_flag: EdgeFlags,
    ) -> Result<Self::Adj, TxnErr<Self::GraphError>> {
        let edge = SerializedEdge::new(min_flag, dest.change, dest.pos, ChangeId::ROOT);
        use crate::pristine::sanakirja::Db;
        let gr: Db<Vertex<ChangeId>, SerializedEdge> = Db::from_page(g.graph);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(self, &gr)?;
        cursor.set(self, &key, Some(&edge))?;
        Ok(Self::Adj {
            cursor,
            key,
            min_flag,
            max_flag,
        })
    }

    fn next_adj<'a>(
        &'a self,
        _: &Self::Graph,
        a: &mut Self::Adj,
    ) -> Option<Result<&'a SerializedEdge, TxnErr<Self::GraphError>>> {
        crate::pristine::sanakirja::next_adj(self, a).map(|x| x.map_err(|e| TxnErr(e.into())))
    }

    fn find_block(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        use crate::pristine::sanakirja::Db;
        let gr: Db<Vertex<ChangeId>, SerializedEdge> = Db::from_page(graph.graph);
        Ok(crate::pristine::sanakirja::find_block(self, &gr, p)?)
    }

    fn find_block_end(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        use crate::pristine::sanakirja::Db;
        let gr: Db<Vertex<ChangeId>, SerializedEdge> = Db::from_page(graph.graph);
        Ok(crate::pristine::sanakirja::find_block_end(self, &gr, p)?)
    }
}

impl ChannelTxnT for TagTxn {
    type Channel = DbOffsets;

    fn graph<'a>(&self, c: &'a Self::Channel) -> &'a Self::Graph {
        c
    }
    fn name<'a>(&self, _: &'a Self::Channel) -> &'a str {
        ""
    }
    fn id<'a>(&self, _: &'a Self::Channel) -> Option<&'a RemoteId> {
        None
    }
    fn apply_counter(&self, channel: &Self::Channel) -> u64 {
        channel.apply_counter.into()
    }
    fn last_modified(&self, _: &Self::Channel) -> u64 {
        0
    }
    fn changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Changeset {
        &channel.changes
    }
    fn rev_changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::RevChangeset {
        &channel.revchanges
    }
    fn tags<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Tags {
        &channel.tags
    }

    type Changeset = u64;
    type RevChangeset = u64;
    type Tags = u64;

    type States = u64;
    fn states<'a>(&self, channel: &'a Self::Channel) -> &'a Self::States {
        &channel.states
    }

    fn get_changeset(
        &self,
        channel: &Self::Changeset,
        c: &ChangeId,
    ) -> Result<Option<&L64>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::Db;
        let db: Db<ChangeId, L64> = Db::from_page(*channel);
        match ::sanakirja::btree::get(self, &db, c, None) {
            Ok(Some((k, x))) if k == c => Ok(Some(x)),
            Ok(x) => {
                debug!("get_changeset = {:?}", x);
                Ok(None)
            }
            Err(e) => {
                error!("{:?}", e);
                Err(TxnErr(SanakirjaError::PristineCorrupt).into())
            }
        }
    }

    fn get_revchangeset(
        &self,
        revchanges: &Self::RevChangeset,
        c: &L64,
    ) -> Result<Option<&Pair<ChangeId, SerializedMerkle>>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::UDb;
        let db: UDb<L64, Pair<ChangeId, SerializedMerkle>> = UDb::from_page(*revchanges);
        match ::sanakirja::btree::get(self, &db, c, None) {
            Ok(Some((k, x))) if k == c => Ok(Some(x)),
            Ok(_) => Ok(None),
            Err(e) => {
                error!("{:?}", e);
                Err(TxnErr(SanakirjaError::PristineCorrupt).into())
            }
        }
    }

    type ChangesetCursor = ::sanakirja::btree::cursor::Cursor<ChangeId, L64, P<ChangeId, L64>>;

    fn cursor_changeset<'a>(
        &'a self,
        channel: &Self::Changeset,
        pos: Option<ChangeId>,
    ) -> Result<Cursor<Self, &'a Self, Self::ChangesetCursor, ChangeId, L64>, TxnErr<TagError>>
    {
        use crate::pristine::sanakirja::Db;
        let db: Db<ChangeId, L64> = Db::from_page(*channel);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(self, &db)?;
        if let Some(k) = pos {
            cursor.set(self, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    type RevchangesetCursor = ::sanakirja::btree::cursor::Cursor<
        L64,
        Pair<ChangeId, SerializedMerkle>,
        UP<L64, Pair<ChangeId, SerializedMerkle>>,
    >;

    fn cursor_revchangeset_ref<'a, RT: std::ops::Deref<Target = Self>>(
        txn: RT,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        Cursor<Self, RT, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<TagError>,
    > {
        use crate::pristine::sanakirja::UDb;
        let db: UDb<L64, Pair<ChangeId, SerializedMerkle>> = UDb::from_page(*channel);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(&*txn, &db)?;
        if let Some(k) = pos {
            cursor.set(&*txn, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    fn rev_cursor_revchangeset<'a>(
        &'a self,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        RevCursor<Self, &'a Self, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<TagError>,
    > {
        use crate::pristine::sanakirja::UDb;
        let db: UDb<L64, Pair<ChangeId, SerializedMerkle>> = UDb::from_page(*channel);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(self, &db)?;
        if let Some(ref pos) = pos {
            cursor.set(self, pos, None)?;
        } else {
            cursor.set_last(self)?;
        };
        Ok(RevCursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }

    fn cursor_revchangeset_next(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<TagError>> {
        if let Ok(x) = cursor.next(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }
    fn cursor_revchangeset_prev(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<TagError>> {
        if let Ok(x) = cursor.prev(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }

    fn cursor_changeset_next(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<TagError>> {
        if let Ok(x) = cursor.next(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }
    fn cursor_changeset_prev(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<TagError>> {
        if let Ok(x) = cursor.prev(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }

    fn channel_has_state(
        &self,
        channel: &Self::States,
        m: &SerializedMerkle,
    ) -> Result<Option<L64>, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::UDb;
        let db: UDb<SerializedMerkle, L64> = UDb::from_page(*channel);
        match ::sanakirja::btree::get(self, &db, m, None)? {
            Some((k, v)) if k == m => Ok(Some(*v)),
            _ => Ok(None),
        }
    }

    type TagsCursor = ::sanakirja::btree::cursor::Cursor<
        L64,
        Pair<SerializedMerkle, SerializedMerkle>,
        P<L64, Pair<SerializedMerkle, SerializedMerkle>>,
    >;

    fn is_tagged(&self, tags: &Self::Tags, t: u64) -> Result<bool, TxnErr<Self::GraphError>> {
        use crate::pristine::sanakirja::Db;
        let db: Db<L64, Pair<SerializedMerkle, SerializedMerkle>> = Db::from_page(*tags);
        let t: L64 = t.into();
        match ::sanakirja::btree::get(self, &db, &t, None)? {
            Some((k, _)) => Ok(k == &t),
            _ => Ok(false),
        }
    }

    fn cursor_tags<'txn>(
        &'txn self,
        channel: &Self::Tags,
        k: Option<L64>,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &'txn Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        use crate::pristine::sanakirja::Db;
        let db: Db<L64, Pair<SerializedMerkle, SerializedMerkle>> = Db::from_page(*channel);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(self, &db)?;
        if let Some(k) = k {
            cursor.set(self, &k, None)?;
        }
        Ok(Cursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }
    fn cursor_tags_next(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        if let Ok(x) = cursor.next(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }

    fn cursor_tags_prev(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        if let Ok(x) = cursor.prev(self) {
            Ok(x)
        } else {
            Err(TxnErr(SanakirjaError::PristineCorrupt).into())
        }
    }

    fn iter_tags(
        &self,
        channel: &Self::Tags,
        from: u64,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        self.cursor_tags(channel, Some(from.into()))
    }

    fn rev_iter_tags(
        &self,
        channel: &Self::Tags,
        from: Option<u64>,
    ) -> Result<
        crate::pristine::RevCursor<
            Self,
            &Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        use crate::pristine::sanakirja::Db;
        let db: Db<L64, Pair<SerializedMerkle, SerializedMerkle>> = Db::from_page(*channel);
        let mut cursor = ::sanakirja::btree::cursor::Cursor::new(self, &db)?;
        if let Some(from) = from {
            cursor.set(self, &from.into(), None)?;
        } else {
            cursor.set_last(self)?;
        };
        Ok(RevCursor {
            cursor,
            txn: self,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
            t: std::marker::PhantomData,
        })
    }
}

pub struct WithTag<T> {
    pub txn: T,
    pub tag: txn::TagTxn,
}

impl<T> std::ops::Deref for WithTag<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.txn
    }
}

fn map_cursor<'txn, A, B, C: ?Sized, D: ?Sized, E>(
    c: crate::pristine::Cursor<A, &'txn A, B, C, D>,
    s: &'txn E,
) -> crate::pristine::Cursor<E, &'txn E, B, C, D> {
    crate::pristine::Cursor {
        cursor: c.cursor,
        txn: s,
        t: std::marker::PhantomData,
        k: c.k,
        v: c.v,
    }
}

fn map_revcursor<'txn, A, B, C: ?Sized, D: ?Sized, E>(
    c: crate::pristine::RevCursor<A, &'txn A, B, C, D>,
    s: &'txn E,
) -> crate::pristine::RevCursor<E, &'txn E, B, C, D> {
    crate::pristine::RevCursor {
        cursor: c.cursor,
        txn: s,
        t: std::marker::PhantomData,
        k: c.k,
        v: c.v,
    }
}

impl<T> GraphTxnT for WithTag<T> {
    type Graph = <TagTxn as GraphTxnT>::Graph;
    type GraphError = <TagTxn as GraphTxnT>::GraphError;

    fn get_graph<'txn>(
        &'txn self,
        db: &Self::Graph,
        key: &Vertex<ChangeId>,
        value: Option<&SerializedEdge>,
    ) -> Result<Option<&'txn SerializedEdge>, TxnErr<Self::GraphError>> {
        self.tag.get_graph(db, key, value)
    }

    fn get_external<'txn>(
        &'txn self,
        int: &ChangeId,
    ) -> Result<Option<&'txn SerializedHash>, TxnErr<Self::GraphError>> {
        self.tag.get_external(int)
    }

    fn get_internal<'txn>(
        &'txn self,
        int: &SerializedHash,
    ) -> Result<Option<&'txn ChangeId>, TxnErr<Self::GraphError>> {
        self.tag.get_internal(int)
    }

    type Adj = <TagTxn as GraphTxnT>::Adj;

    fn init_adj(
        &self,
        g: &Self::Graph,
        key: Vertex<ChangeId>,
        dest: Position<ChangeId>,
        min_flag: EdgeFlags,
        max_flag: EdgeFlags,
    ) -> Result<Self::Adj, TxnErr<Self::GraphError>> {
        self.tag.init_adj(g, key, dest, min_flag, max_flag)
    }

    fn next_adj<'a>(
        &'a self,
        a: &Self::Graph,
        b: &mut Self::Adj,
    ) -> Option<Result<&'a SerializedEdge, TxnErr<Self::GraphError>>> {
        self.tag.next_adj(a, b)
    }

    fn find_block(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        self.tag.find_block(graph, p)
    }

    fn find_block_end(
        &self,
        graph: &Self::Graph,
        p: Position<ChangeId>,
    ) -> Result<&Vertex<ChangeId>, BlockError<Self::GraphError>> {
        self.tag.find_block_end(graph, p)
    }
}

impl<T> WithTag<T> {
    pub fn channel(&self) -> ChannelRef<Self> {
        ChannelRef::new(self.tag.header.offsets.clone())
    }
}

impl<T> ChannelTxnT for WithTag<T> {
    type Channel = <TagTxn as ChannelTxnT>::Channel;

    fn graph<'a>(&self, c: &'a Self::Channel) -> &'a Self::Graph {
        self.tag.graph(c)
    }
    fn name<'a>(&self, c: &'a Self::Channel) -> &'a str {
        self.tag.name(c)
    }
    fn id<'a>(&self, c: &'a Self::Channel) -> Option<&'a RemoteId> {
        self.tag.id(c)
    }
    fn apply_counter(&self, channel: &Self::Channel) -> u64 {
        self.tag.apply_counter(channel)
    }
    fn last_modified(&self, c: &Self::Channel) -> u64 {
        self.tag.last_modified(c)
    }
    fn changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Changeset {
        self.tag.changes(channel)
    }
    fn rev_changes<'a>(&self, channel: &'a Self::Channel) -> &'a Self::RevChangeset {
        self.tag.rev_changes(channel)
    }
    fn tags<'a>(&self, channel: &'a Self::Channel) -> &'a Self::Tags {
        self.tag.tags(channel)
    }

    type Changeset = <TagTxn as ChannelTxnT>::Changeset;
    type RevChangeset = <TagTxn as ChannelTxnT>::RevChangeset;
    type Tags = <TagTxn as ChannelTxnT>::Tags;

    fn is_tagged(&self, tags: &Self::Tags, t: u64) -> Result<bool, TxnErr<Self::GraphError>> {
        self.tag.is_tagged(tags, t)
    }

    type States = <TagTxn as ChannelTxnT>::States;

    fn states<'a>(&self, channel: &'a Self::Channel) -> &'a Self::States {
        &channel.states
    }

    fn get_changeset(
        &self,
        channel: &Self::Changeset,
        c: &ChangeId,
    ) -> Result<Option<&L64>, TxnErr<Self::GraphError>> {
        self.tag.get_changeset(channel, c)
    }

    fn get_revchangeset(
        &self,
        revchanges: &Self::RevChangeset,
        c: &L64,
    ) -> Result<Option<&Pair<ChangeId, SerializedMerkle>>, TxnErr<Self::GraphError>> {
        self.tag.get_revchangeset(revchanges, c)
    }

    type ChangesetCursor = <TagTxn as ChannelTxnT>::ChangesetCursor;

    fn cursor_changeset<'a>(
        &'a self,
        channel: &Self::Changeset,
        pos: Option<ChangeId>,
    ) -> Result<Cursor<Self, &'a Self, Self::ChangesetCursor, ChangeId, L64>, TxnErr<TagError>>
    {
        Ok(map_cursor(self.tag.cursor_changeset(channel, pos)?, self))
    }

    type RevchangesetCursor = <TagTxn as ChannelTxnT>::RevchangesetCursor;

    fn cursor_revchangeset_ref<'a, RT: std::ops::Deref<Target = Self>>(
        txn: RT,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        Cursor<Self, RT, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<TagError>,
    > {
        let cursor =
            <TagTxn as ChannelTxnT>::cursor_revchangeset_ref(&txn.deref().tag, channel, pos)?;
        Ok(Cursor {
            cursor: cursor.cursor,
            txn,
            t: std::marker::PhantomData,
            k: std::marker::PhantomData,
            v: std::marker::PhantomData,
        })
    }

    fn rev_cursor_revchangeset<'a>(
        &'a self,
        channel: &Self::RevChangeset,
        pos: Option<L64>,
    ) -> Result<
        RevCursor<Self, &'a Self, Self::RevchangesetCursor, L64, Pair<ChangeId, SerializedMerkle>>,
        TxnErr<TagError>,
    > {
        Ok(map_revcursor(
            self.tag.rev_cursor_revchangeset(channel, pos)?,
            self,
        ))
    }

    fn cursor_revchangeset_next(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<TagError>> {
        self.tag.cursor_revchangeset_next(cursor)
    }
    fn cursor_revchangeset_prev(
        &self,
        cursor: &mut Self::RevchangesetCursor,
    ) -> Result<Option<(&L64, &Pair<ChangeId, SerializedMerkle>)>, TxnErr<TagError>> {
        self.tag.cursor_revchangeset_prev(cursor)
    }

    fn cursor_changeset_next(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<TagError>> {
        self.tag.cursor_changeset_next(cursor)
    }
    fn cursor_changeset_prev(
        &self,
        cursor: &mut Self::ChangesetCursor,
    ) -> Result<Option<(&ChangeId, &L64)>, TxnErr<TagError>> {
        self.tag.cursor_changeset_prev(cursor)
    }

    fn channel_has_state(
        &self,
        channel: &Self::States,
        m: &SerializedMerkle,
    ) -> Result<Option<L64>, TxnErr<Self::GraphError>> {
        self.tag.channel_has_state(channel, m)
    }

    type TagsCursor = <TagTxn as ChannelTxnT>::TagsCursor;
    fn cursor_tags<'txn>(
        &'txn self,
        channel: &Self::Tags,
        k: Option<L64>,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &'txn Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        Ok(map_cursor(self.tag.cursor_tags(channel, k)?, self))
    }
    fn cursor_tags_next(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        self.tag.cursor_tags_next(cursor)
    }

    fn cursor_tags_prev(
        &self,
        cursor: &mut Self::TagsCursor,
    ) -> Result<Option<(&L64, &Pair<SerializedMerkle, SerializedMerkle>)>, TxnErr<Self::GraphError>>
    {
        self.tag.cursor_tags_prev(cursor)
    }

    fn iter_tags(
        &self,
        channel: &Self::Tags,
        from: u64,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        Ok(map_cursor(self.tag.iter_tags(channel, from)?, self))
    }

    fn rev_iter_tags(
        &self,
        channel: &Self::Tags,
        from: Option<u64>,
    ) -> Result<
        crate::pristine::RevCursor<
            Self,
            &Self,
            Self::TagsCursor,
            L64,
            Pair<SerializedMerkle, SerializedMerkle>,
        >,
        TxnErr<Self::GraphError>,
    > {
        Ok(map_revcursor(self.tag.rev_iter_tags(channel, from)?, self))
    }
}

impl<T: TreeTxnT> TreeTxnT for WithTag<T> {
    type TreeError = T::TreeError;
    type Tree = T::Tree;
    type TreeCursor = T::TreeCursor;
    fn get_tree(
        &self,
        a: &PathId,
        b: Option<&Inode>,
    ) -> Result<Option<&Inode>, TreeErr<Self::TreeError>> {
        self.txn.get_tree(a, b)
    }
    fn cursor_tree<'txn>(
        &'txn self,
        a: &Self::Tree,
        b: Option<(&PathId, Option<&Inode>)>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::TreeCursor, PathId, Inode>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.cursor_tree(a, b)?, self))
    }
    fn cursor_tree_next(
        &self,
        m: &mut Self::TreeCursor,
    ) -> Result<Option<(&PathId, &Inode)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_tree_next(m)
    }
    fn cursor_tree_prev(
        &self,
        m: &mut Self::TreeCursor,
    ) -> Result<Option<(&PathId, &Inode)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_tree_prev(m)
    }
    fn iter_tree<'txn>(
        &'txn self,
        a: &PathId,
        b: Option<&Inode>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::TreeCursor, PathId, Inode>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.iter_tree(a, b)?, self))
    }

    type Revtree = T::Revtree;
    type RevtreeCursor = T::RevtreeCursor;

    fn get_revtree(
        &self,
        a: &Inode,
        b: Option<&PathId>,
    ) -> Result<Option<&PathId>, TreeErr<Self::TreeError>> {
        self.txn.get_revtree(a, b)
    }
    fn cursor_revtree<'txn>(
        &'txn self,
        a: &Self::Revtree,
        b: Option<(&Inode, Option<&PathId>)>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::RevtreeCursor, Inode, PathId>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.cursor_revtree(a, b)?, self))
    }
    fn cursor_revtree_next(
        &self,
        m: &mut Self::RevtreeCursor,
    ) -> Result<Option<(&Inode, &PathId)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_revtree_next(m)
    }
    fn cursor_revtree_prev(
        &self,
        m: &mut Self::RevtreeCursor,
    ) -> Result<Option<(&Inode, &PathId)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_revtree_prev(m)
    }
    fn iter_revtree<'txn>(
        &'txn self,
        a: &Inode,
        b: Option<&PathId>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::RevtreeCursor, Inode, PathId>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.iter_revtree(a, b)?, self))
    }

    type Inodes = T::Inodes;
    type InodesCursor = T::InodesCursor;
    type Revinodes = T::Revinodes;
    type RevinodesCursor = T::RevinodesCursor;

    fn get_inodes(
        &self,
        a: &Inode,
        b: Option<&Position<ChangeId>>,
    ) -> Result<Option<&Position<ChangeId>>, TreeErr<Self::TreeError>> {
        self.txn.get_inodes(a, b)
    }
    fn get_revinodes(
        &self,
        a: &Position<ChangeId>,
        b: Option<&Inode>,
    ) -> Result<Option<&Inode>, TreeErr<Self::TreeError>> {
        self.txn.get_revinodes(a, b)
    }

    fn cursor_inodes<'txn>(
        &'txn self,
        a: &Self::Inodes,
        b: Option<(&Inode, Option<&Position<ChangeId>>)>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::InodesCursor, Inode, Position<ChangeId>>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.cursor_inodes(a, b)?, self))
    }

    fn cursor_revinodes<'txn>(
        &'txn self,
        a: &Self::Revinodes,
        b: Option<(&Position<ChangeId>, Option<&Inode>)>,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::RevinodesCursor, Position<ChangeId>, Inode>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.cursor_revinodes(a, b)?, self))
    }

    fn cursor_inodes_next(
        &self,
        m: &mut Self::InodesCursor,
    ) -> Result<Option<(&Inode, &Position<ChangeId>)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_inodes_next(m)
    }

    fn cursor_revinodes_next(
        &self,
        m: &mut Self::RevinodesCursor,
    ) -> Result<Option<(&Position<ChangeId>, &Inode)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_revinodes_next(m)
    }

    fn cursor_inodes_prev(
        &self,
        m: &mut Self::InodesCursor,
    ) -> Result<Option<(&Inode, &Position<ChangeId>)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_inodes_prev(m)
    }

    fn cursor_revinodes_prev(
        &self,
        m: &mut Self::RevinodesCursor,
    ) -> Result<Option<(&Position<ChangeId>, &Inode)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_revinodes_prev(m)
    }

    fn iter_inodes<'txn>(
        &'txn self,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::InodesCursor, Inode, Position<ChangeId>>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.iter_inodes()?, self))
    }

    fn iter_revinodes<'txn>(
        &'txn self,
    ) -> Result<
        crate::pristine::Cursor<Self, &'txn Self, Self::RevinodesCursor, Position<ChangeId>, Inode>,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.iter_revinodes()?, self))
    }

    type Partials = T::Partials;
    type PartialsCursor = T::PartialsCursor;

    fn cursor_partials<'txn>(
        &'txn self,
        a: &Self::Partials,
        b: Option<(&SmallStr, Option<&Position<ChangeId>>)>,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &'txn Self,
            Self::PartialsCursor,
            SmallStr,
            Position<ChangeId>,
        >,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.cursor_partials(a, b)?, self))
    }

    fn iter_partials<'txn>(
        &'txn self,
        channel: &str,
    ) -> Result<
        crate::pristine::Cursor<
            Self,
            &'txn Self,
            Self::PartialsCursor,
            SmallStr,
            Position<ChangeId>,
        >,
        TreeErr<Self::TreeError>,
    > {
        Ok(map_cursor(self.txn.iter_partials(channel)?, self))
    }

    fn cursor_partials_next(
        &self,
        m: &mut Self::PartialsCursor,
    ) -> Result<Option<(&SmallStr, &Position<ChangeId>)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_partials_next(m)
    }

    fn cursor_partials_prev(
        &self,
        m: &mut Self::PartialsCursor,
    ) -> Result<Option<(&SmallStr, &Position<ChangeId>)>, TreeErr<Self::TreeError>> {
        self.txn.cursor_partials_prev(m)
    }
}

impl<T: TreeMutTxnT> TreeMutTxnT for WithTag<T> {
    fn put_inodes(
        &mut self,
        a: &Inode,
        b: &Position<ChangeId>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.put_inodes(a, b)
    }
    fn put_revinodes(
        &mut self,
        a: &Position<ChangeId>,
        b: &Inode,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.put_revinodes(a, b)
    }
    fn del_inodes(
        &mut self,
        a: &Inode,
        b: Option<&Position<ChangeId>>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.del_inodes(a, b)
    }
    fn del_revinodes(
        &mut self,
        a: &Position<ChangeId>,
        b: Option<&Inode>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.del_revinodes(a, b)
    }
    fn put_tree(&mut self, a: &PathId, b: &Inode) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.put_tree(a, b)
    }
    fn put_revtree(&mut self, a: &Inode, b: &PathId) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.put_revtree(a, b)
    }
    fn del_tree(
        &mut self,
        a: &PathId,
        b: Option<&Inode>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.del_tree(a, b)
    }
    fn del_revtree(
        &mut self,
        a: &Inode,
        b: Option<&PathId>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.del_revtree(a, b)
    }
    fn put_partials(
        &mut self,
        a: &str,
        b: Position<ChangeId>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.put_partials(a, b)
    }
    fn del_partials(
        &mut self,
        a: &str,
        b: Option<Position<ChangeId>>,
    ) -> Result<bool, TreeErr<Self::TreeError>> {
        self.txn.del_partials(a, b)
    }
}
