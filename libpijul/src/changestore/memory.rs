use super::*;
use crate::change::{Change, ChangeHeader};
use crate::pristine::{ChangeId, Hash, Vertex};
use crate::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
/// A change store in memory, i.e. basically a hash table.
pub struct Memory {
    changes: Arc<RwLock<HashMap<Hash, Change>>>,
    tags: Arc<RwLock<HashMap<crate::Merkle, ChangeHeader>>>,
}

impl Memory {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
    #[error(transparent)]
    Change(#[from] crate::change::ChangeError),
    #[error("Change not found: {:?}", hash)]
    ChangeNotFound { hash: crate::Hash },
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
}

impl ChangeStore for Memory {
    type Error = Error;
    fn has_contents(&self, hash: Hash, _: Option<ChangeId>) -> bool {
        let changes = self.changes.read().unwrap();
        let p = changes.get(&hash).unwrap();
        !p.contents.is_empty()
    }

    fn get_tag_header(&self, h: &crate::Merkle) -> Result<ChangeHeader, Self::Error> {
        let changes = self.tags.read().unwrap();
        Ok(changes.get(&h).unwrap().clone())
    }

    fn get_contents<F: Fn(ChangeId) -> Option<Hash>>(
        &self,
        hash: F,
        key: Vertex<ChangeId>,
        buf: &mut [u8],
    ) -> Result<usize, Self::Error> {
        if key.end <= key.start {
            return Ok(0);
        }
        assert_eq!(buf.len(), key.end - key.start);
        let changes = self.changes.read().unwrap();
        let p = changes.get(&hash(key.change).unwrap()).unwrap();
        let start = key.start.us();
        let end = key.end.us();
        buf.clone_from_slice(&p.contents[start..end]);
        Ok(end - start)
    }
    fn get_contents_ext(
        &self,
        key: Vertex<Option<Hash>>,
        buf: &mut [u8],
    ) -> Result<usize, Self::Error> {
        if let Some(change) = key.change {
            if key.end <= key.start {
                return Ok(0);
            }
            assert_eq!(key.end.us() - key.start.us(), buf.len());
            let changes = self.changes.read().unwrap();
            let p = changes.get(&change).unwrap();
            let start = key.start.us();
            let end = key.end.us();
            buf.clone_from_slice(&p.contents[start..end]);
            Ok(end - start)
        } else {
            Ok(0)
        }
    }
    fn change_deletes_position<F: Fn(ChangeId) -> Option<Hash>>(
        &self,
        hash: F,
        change: ChangeId,
        pos: Position<Option<Hash>>,
    ) -> Result<Vec<Hash>, Self::Error> {
        let changes = self.changes.read().unwrap();
        let change = changes.get(&hash(change).unwrap()).unwrap();
        let mut v = Vec::new();
        for c in change.changes.iter() {
            for c in c.iter() {
                v.extend(c.deletes_pos(pos).into_iter())
            }
        }
        Ok(v)
    }
    fn save_change<
        E: From<Self::Error> + From<ChangeError>,
        F: FnOnce(&mut Change, &Hash) -> Result<(), E>,
    >(
        &self,
        p: &mut Change,
        f: F,
    ) -> Result<Hash, E> {
        let mut w = self.changes.write().unwrap();
        let hash = p.hash().map_err(|e| Self::Error::from(e))?;
        f(p, &hash)?;
        w.insert(hash, p.clone());
        Ok(hash)
    }
    fn del_change(&self, h: &Hash) -> Result<bool, Self::Error> {
        let mut w = self.changes.write().unwrap();
        Ok(w.remove(h).is_some())
    }
    fn get_change(&self, h: &Hash) -> Result<Change, Self::Error> {
        let w = self.changes.read().unwrap();
        if let Some(p) = w.get(h) {
            Ok(p.clone())
        } else {
            Err(Error::ChangeNotFound { hash: *h })
        }
    }
}
