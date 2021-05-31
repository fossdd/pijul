//! A change store is a trait for change storage facilities. Even though
//! changes are normally stored on disk, there are situations (such as
//! an embedded Pijul) where one might want changes in-memory, in a
//! database, or something else.
use crate::change::{Change, ChangeHeader};
use crate::pristine::{ChangeId, Hash, InodeMetadata, Position, Vertex};

#[cfg(feature = "ondisk-repos")]
/// If this crate is compiled with the `ondisk-repos` feature (the
/// default), this module stores changes on the file system, under
/// `.pijul/changes`.
pub mod filesystem;

/// A change store entirely in memory.
pub mod memory;

/// A trait for storing changes and reading from them.
pub trait ChangeStore {
    type Error: std::error::Error
        + std::fmt::Debug
        + Send
        + Sync
        + From<std::str::Utf8Error>
        + From<crate::change::ChangeError>
        + 'static;
    fn has_contents(&self, hash: Hash, change_id: Option<ChangeId>) -> bool;
    fn get_contents<F: Fn(ChangeId) -> Option<Hash>>(
        &self,
        hash: F,
        key: Vertex<ChangeId>,
        buf: &mut Vec<u8>,
    ) -> Result<usize, Self::Error>;
    fn get_header(&self, h: &Hash) -> Result<ChangeHeader, Self::Error> {
        Ok(self.get_change(h)?.hashed.header)
    }
    fn get_contents_ext(
        &self,
        key: Vertex<Option<Hash>>,
        buf: &mut Vec<u8>,
    ) -> Result<usize, Self::Error>;
    fn get_dependencies(&self, hash: &Hash) -> Result<Vec<Hash>, Self::Error> {
        Ok(self.get_change(hash)?.hashed.dependencies)
    }
    fn get_extra_known(&self, hash: &Hash) -> Result<Vec<Hash>, Self::Error> {
        Ok(self.get_change(hash)?.hashed.extra_known)
    }
    fn get_changes(
        &self,
        hash: &Hash,
    ) -> Result<Vec<crate::change::Hunk<Option<Hash>, crate::change::Local>>, Self::Error> {
        Ok(self.get_change(hash)?.hashed.changes)
    }
    fn knows(&self, hash0: &Hash, hash1: &Hash) -> Result<bool, Self::Error> {
        Ok(self.get_change(hash0)?.knows(hash1))
    }
    fn has_edge(
        &self,
        change: Hash,
        from: Position<Option<Hash>>,
        to: Position<Option<Hash>>,
        flags: crate::pristine::EdgeFlags,
    ) -> Result<bool, Self::Error> {
        let change_ = self.get_change(&change)?;
        Ok(change_.has_edge(change, from, to, flags))
    }
    fn change_deletes_position<F: Fn(ChangeId) -> Option<Hash>>(
        &self,
        hash: F,
        change: ChangeId,
        pos: Position<Option<Hash>>,
    ) -> Result<Vec<Hash>, Self::Error>;
    fn save_change(&self, p: &Change) -> Result<Hash, Self::Error>;
    fn del_change(&self, h: &Hash) -> Result<bool, Self::Error>;
    fn get_change(&self, h: &Hash) -> Result<Change, Self::Error>;
    fn get_file_name<'a, F: Fn(ChangeId) -> Option<Hash>>(
        &self,
        hash: F,
        vertex: Vertex<ChangeId>,
        buf: &'a mut Vec<u8>,
    ) -> Result<(InodeMetadata, &'a str), Self::Error> {
        buf.clear();
        self.get_contents(hash, vertex, buf)?;
        assert!(buf.len() >= 2);
        let (a, b) = buf.as_slice().split_at(2);
        Ok((InodeMetadata::from_basename(a), std::str::from_utf8(b)?))
    }
}

impl crate::change::Atom<Option<Hash>> {
    pub fn deletes_pos(&self, pos: Position<Option<Hash>>) -> Vec<Hash> {
        let mut h = Vec::new();
        if let crate::change::Atom::EdgeMap(ref n) = self {
            for edge in n.edges.iter() {
                if edge.to.change == pos.change && edge.to.start <= pos.pos && pos.pos < edge.to.end
                {
                    if let Some(c) = edge.introduced_by {
                        h.push(c)
                    }
                }
            }
        }
        h
    }
}
