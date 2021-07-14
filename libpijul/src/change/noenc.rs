use super::{Atom, Change, ChangeError, Hashed, Local, LocalChange, Offsets};
use crate::pristine::Hasher;
use crate::Hash;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Hunk<Hash, Local> {
    FileMove {
        del: Atom<Hash>,
        add: Atom<Hash>,
        path: String,
    },
    FileDel {
        del: Atom<Hash>,
        contents: Option<Atom<Hash>>,
        path: String,
    },
    FileUndel {
        undel: Atom<Hash>,
        contents: Option<Atom<Hash>>,
        path: String,
    },
    FileAdd {
        add_name: Atom<Hash>,
        add_inode: Atom<Hash>,
        contents: Option<Atom<Hash>>,
        path: String,
    },
    SolveNameConflict {
        name: Atom<Hash>,
        path: String,
    },
    UnsolveNameConflict {
        name: Atom<Hash>,
        path: String,
    },
    Edit {
        change: Atom<Hash>,
        local: Local,
    },
    Replacement {
        change: Atom<Hash>,
        replacement: Atom<Hash>,
        local: Local,
    },
    SolveOrderConflict {
        change: Atom<Hash>,
        local: Local,
    },
    UnsolveOrderConflict {
        change: Atom<Hash>,
        local: Local,
    },
    ResurrectZombies {
        change: Atom<Hash>,
        local: Local,
    },
}

impl<H, L> From<Hunk<H, L>> for super::Hunk<H, L> {
    fn from(h: Hunk<H, L>) -> Self {
        let encoding = Some(crate::text_encoding::Encoding(encoding_rs::UTF_8));
        match h {
            Hunk::FileMove { del, add, path } => super::Hunk::FileMove { del, add, path },
            Hunk::FileDel {
                del,
                contents,
                path,
            } => super::Hunk::FileDel {
                del,
                contents,
                path,
                encoding,
            },
            Hunk::FileUndel {
                undel,
                contents,
                path,
            } => super::Hunk::FileUndel {
                undel,
                contents,
                path,
                encoding,
            },
            Hunk::FileAdd {
                add_name,
                add_inode,
                contents,
                path,
            } => super::Hunk::FileAdd {
                add_name,
                add_inode,
                contents,
                path,
                encoding,
            },
            Hunk::SolveNameConflict { name, path } => super::Hunk::SolveNameConflict { name, path },
            Hunk::UnsolveNameConflict { name, path } => {
                super::Hunk::UnsolveNameConflict { name, path }
            }
            Hunk::Edit { change, local } => super::Hunk::Edit {
                change,
                local,
                encoding,
            },
            Hunk::Replacement {
                change,
                replacement,
                local,
            } => super::Hunk::Replacement {
                change,
                replacement,
                local,
                encoding,
            },
            Hunk::SolveOrderConflict { change, local } => {
                super::Hunk::SolveOrderConflict { change, local }
            }
            Hunk::UnsolveOrderConflict { change, local } => {
                super::Hunk::UnsolveOrderConflict { change, local }
            }
            Hunk::ResurrectZombies { change, local } => super::Hunk::ResurrectZombies {
                change,
                local,
                encoding,
            },
        }
    }
}

impl Change {
    /// Deserialise a change from the file given as input `file`.
    #[cfg(feature = "zstd")]
    pub(super) fn deserialize_noenc(
        offsets: Offsets,
        mut r: std::fs::File,
        hash: Option<&Hash>,
    ) -> Result<Self, ChangeError> {
        use std::io::Read;
        let mut buf = vec![0u8; (offsets.unhashed_off - Self::OFFSETS_SIZE) as usize];
        r.read_exact(&mut buf)?;

        let hashed: Hashed<Hunk<Option<Hash>, Local>, Author> = {
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut out = vec![0u8; offsets.hashed_len as usize];
            s.decompress(&mut out[..], 0)?;
            let mut hasher = Hasher::default();
            hasher.update(&out);
            let computed_hash = hasher.finish();
            if let Some(hash) = hash {
                if &computed_hash != hash {
                    return Err(super::ChangeError::ChangeHashMismatch {
                        claimed: *hash,
                        computed: computed_hash,
                    });
                }
            }
            bincode::deserialize_from(&out[..])?
        };
        buf.clear();
        buf.resize((offsets.contents_off - offsets.unhashed_off) as usize, 0);
        let unhashed = if buf.is_empty() {
            None
        } else {
            r.read_exact(&mut buf)?;
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut out = vec![0u8; offsets.unhashed_len as usize];
            s.decompress(&mut out[..], 0)?;
            serde_json::from_slice(&out).ok()
        };
        trace!("unhashed = {:?}", unhashed);

        buf.clear();
        buf.resize((offsets.total - offsets.contents_off) as usize, 0);
        let contents = if r.read_exact(&mut buf).is_ok() {
            let mut s = zstd_seekable::Seekable::init_buf(&buf[..])?;
            let mut contents = vec![0u8; offsets.contents_len as usize];
            s.decompress(&mut contents[..], 0)?;
            contents
        } else {
            Vec::new()
        };
        trace!("contents = {:?}", contents);

        Ok(LocalChange {
            offsets,
            hashed: hashed.into(),
            unhashed,
            contents,
        })
    }
}

impl From<Hashed<Hunk<Option<Hash>, Local>, Author>>
    for Hashed<super::Hunk<Option<Hash>, Local>, super::Author>
{
    fn from(hashed: Hashed<Hunk<Option<Hash>, Local>, Author>) -> Self {
        Hashed {
            contents_hash: hashed.contents_hash,
            dependencies: hashed.dependencies,
            extra_known: hashed.extra_known,
            header: hashed.header.into(),
            metadata: hashed.metadata,
            version: hashed.version,
            changes: hashed.changes.into_iter().map(|x| x.into()).collect(),
        }
    }
}

use super::ChangeHeader_;

impl From<ChangeHeader_<Author>> for ChangeHeader_<super::Author> {
    fn from(c: ChangeHeader_<Author>) -> Self {
        ChangeHeader_ {
            message: c.message,
            description: c.description,
            timestamp: c.timestamp,
            authors: c.authors.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct Author {
    pub name: String,
    #[serde(default)]
    pub full_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

impl From<String> for Author {
    fn from(name: String) -> Author {
        Author {
            name,
            ..Author::default()
        }
    }
}

impl From<Author> for super::Author {
    fn from(c: Author) -> Self {
        let mut b = std::collections::BTreeMap::new();
        b.insert("name".to_string(), c.name);
        if let Some(n) = c.full_name {
            b.insert("full_name".to_string(), n);
        }
        if let Some(n) = c.email {
            b.insert("email".to_string(), n);
        }
        super::Author(b)
    }
}
