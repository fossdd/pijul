use super::Base32;
pub(crate) const BLAKE3_BYTES: usize = 32;
pub(crate) const BASE32_BYTES: usize = 53;

/// The external hash of changes.
#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Hash {
    /// None is the hash of the "null change", which introduced a
    /// single root vertex at the beginning of the repository.
    None,
    Blake3([u8; BLAKE3_BYTES]),
}

pub(crate) enum Hasher {
    Blake3(blake3::Hasher),
}

impl Default for Hasher {
    fn default() -> Self {
        Hasher::Blake3(blake3::Hasher::new())
    }
}

impl Hasher {
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        match self {
            Hasher::Blake3(ref mut h) => {
                h.update(bytes);
            }
        }
    }
    pub(crate) fn finish(&self) -> Hash {
        match self {
            Hasher::Blake3(ref h) => {
                let result = h.finalize();
                let mut hash = [0; BLAKE3_BYTES];
                hash.clone_from_slice(result.as_bytes());
                Hash::Blake3(hash)
            }
        }
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{}", self.to_base32())
    }
}

/// Algorithm used to compute change hashes.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum HashAlgorithm {
    None = 0,
    Blake3 = 1,
}

impl Hash {
    pub fn to_bytes(&self) -> [u8; 1 + BLAKE3_BYTES] {
        match *self {
            Hash::None => unimplemented!(),
            Hash::Blake3(ref s) => {
                let mut out = [0; 1 + BLAKE3_BYTES];
                out[0] = HashAlgorithm::Blake3 as u8;
                (&mut out[1..]).clone_from_slice(s);
                out
            }
        }
    }

    pub fn from_bytes(s: &[u8]) -> Option<Self> {
        if s.len() >= 1 + BLAKE3_BYTES && s[0] == HashAlgorithm::Blake3 as u8 {
            let mut out = [0; BLAKE3_BYTES];
            out.clone_from_slice(&s[1..]);
            Some(Hash::Blake3(out))
        } else {
            None
        }
    }

    pub fn from_prefix(s: &str) -> Option<Self> {
        let mut b32 = [b'A'; BASE32_BYTES];
        if s.len() > BASE32_BYTES {
            return None;
        }
        (&mut b32[..s.len()]).clone_from_slice(s.as_bytes());
        let bytes = if let Ok(bytes) = data_encoding::BASE32_NOPAD.decode(&b32) {
            bytes
        } else {
            return None;
        };
        let mut hash = [0; BLAKE3_BYTES];
        hash.clone_from_slice(&bytes[..BLAKE3_BYTES]);
        Some(Hash::Blake3(hash))
    }
}

impl super::Base32 for Hash {
    /// Returns the base-32 representation of a hash.
    fn to_base32(&self) -> String {
        match *self {
            Hash::None => data_encoding::BASE32_NOPAD.encode(&[0]),
            Hash::Blake3(ref s) => {
                let mut hash = [0; 1 + BLAKE3_BYTES];
                hash[BLAKE3_BYTES] = HashAlgorithm::Blake3 as u8;
                (&mut hash[..BLAKE3_BYTES]).clone_from_slice(s);
                data_encoding::BASE32_NOPAD.encode(&hash)
            }
        }
    }

    /// Parses a base-32 string into a hash.
    fn from_base32(s: &[u8]) -> Option<Self> {
        let bytes = if let Ok(s) = data_encoding::BASE32_NOPAD.decode(s) {
            s
        } else {
            return None;
        };
        if bytes == [0] {
            Some(Hash::None)
        } else if bytes.len() == BLAKE3_BYTES + 1
            && bytes[BLAKE3_BYTES] == HashAlgorithm::Blake3 as u8
        {
            let mut hash = [0; BLAKE3_BYTES];
            hash.clone_from_slice(&bytes[..BLAKE3_BYTES]);
            Some(Hash::Blake3(hash))
        } else {
            None
        }
    }
}

impl std::str::FromStr for Hash {
    type Err = crate::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(b) = Self::from_base32(s.as_bytes()) {
            Ok(b)
        } else {
            Err(crate::ParseError { s: s.to_string() })
        }
    }
}

#[test]
fn from_to() {
    let mut h = Hasher::default();
    h.update(b"blabla");
    let h = h.finish();
    assert_eq!(Hash::from_base32(&h.to_base32().as_bytes()), Some(h));
    let h = Hash::None;
    assert_eq!(Hash::from_base32(&h.to_base32().as_bytes()), Some(h));
    let b = data_encoding::BASE32_NOPAD.encode(&[19, 18, 17]);
    assert_eq!(Hash::from_base32(&b.as_bytes()), None);
}

#[derive(Clone, Copy)]
pub struct SerializedHash {
    pub(crate) t: u8,
    h: H,
}

impl std::hash::Hash for SerializedHash {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        self.t.hash(hasher);
        if self.t == HashAlgorithm::Blake3 as u8 {
            unsafe { self.h.blake3.hash(hasher) }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) union H {
    none: (),
    blake3: [u8; 32],
}

pub(crate) const HASH_NONE: SerializedHash = SerializedHash {
    t: HashAlgorithm::None as u8,
    h: H { none: () },
};

use std::cmp::Ordering;

impl PartialOrd for SerializedHash {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        Some(self.t.cmp(&b.t))
    }
}

impl Ord for SerializedHash {
    fn cmp(&self, b: &Self) -> Ordering {
        match self.t.cmp(&b.t) {
            Ordering::Equal => {
                if self.t == HashAlgorithm::Blake3 as u8 {
                    unsafe { self.h.blake3.cmp(&b.h.blake3) }
                } else {
                    Ordering::Equal
                }
            }
            o => o,
        }
    }
}

impl PartialEq for SerializedHash {
    fn eq(&self, b: &Self) -> bool {
        if self.t == HashAlgorithm::Blake3 as u8 && self.t == b.t {
            unsafe { self.h.blake3 == b.h.blake3 }
        } else if self.t == b.t {
            true
        } else {
            false
        }
    }
}
impl Eq for SerializedHash {}

impl std::fmt::Debug for SerializedHash {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        Hash::from(self).fmt(fmt)
    }
}

impl<'a> From<&'a SerializedHash> for Hash {
    fn from(s: &'a SerializedHash) -> Hash {
        if s.t == HashAlgorithm::Blake3 as u8 {
            Hash::Blake3(unsafe { s.h.blake3.clone() })
        } else if s.t == HashAlgorithm::None as u8 {
            Hash::None
        } else {
            panic!("Unknown hash algorithm {:?}", s.t)
        }
    }
}

impl From<SerializedHash> for Hash {
    fn from(s: SerializedHash) -> Hash {
        (&s).into()
    }
}

impl From<Hash> for SerializedHash {
    fn from(s: Hash) -> SerializedHash {
        (&s).into()
    }
}

impl<'a> From<&'a Hash> for SerializedHash {
    fn from(s: &'a Hash) -> Self {
        match s {
            Hash::Blake3(s) => SerializedHash {
                t: HashAlgorithm::Blake3 as u8,
                h: H { blake3: s.clone() },
            },
            Hash::None => SerializedHash {
                t: 0,
                h: H { none: () },
            },
        }
    }
}

impl SerializedHash {
    pub fn size(b: &[u8]) -> usize {
        if b[0] == HashAlgorithm::Blake3 as u8 {
            1 + BLAKE3_BYTES
        } else if b[0] == HashAlgorithm::None as u8 {
            1
        } else {
            panic!("Unknown hash algorithm {:?}", b[0])
        }
    }

    pub unsafe fn size_from_ptr(b: *const u8) -> usize {
        if *b == HashAlgorithm::Blake3 as u8 {
            1 + BLAKE3_BYTES
        } else if *b == HashAlgorithm::None as u8 {
            1
        } else {
            panic!("Unknown hash algorithm {:?}", *b)
        }
    }
}
