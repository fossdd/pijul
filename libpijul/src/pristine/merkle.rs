use super::Base32;
use curve25519_dalek::constants::ED25519_BASEPOINT_POINT;

pub(crate) const BASE32_BYTES: usize = 53;

#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Merkle {
    Ed25519(curve25519_dalek::edwards::EdwardsPoint),
}

impl Default for Merkle {
    fn default() -> Self {
        Merkle::zero()
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum MerkleAlgorithm {
    Ed25519 = 1,
}

impl std::fmt::Debug for Merkle {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{:?}", self.to_base32())
    }
}

impl std::fmt::Debug for SerializedMerkle {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        Merkle::from(self).fmt(fmt)
    }
}

impl std::hash::Hash for Merkle {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        match self {
            Merkle::Ed25519(x) => x.compress().as_bytes().hash(hasher),
        }
    }
}

impl From<&super::Hash> for curve25519_dalek::scalar::Scalar {
    fn from(h: &super::Hash) -> Self {
        match h {
            super::Hash::Blake3(h) => curve25519_dalek::scalar::Scalar::from_bytes_mod_order(*h),
            _ => unreachable!(),
        }
    }
}

impl From<&super::SerializedHash> for curve25519_dalek::scalar::Scalar {
    fn from(h: &super::SerializedHash) -> Self {
        let h: super::Hash = h.into();
        (&h).into()
    }
}

impl From<&Merkle> for curve25519_dalek::scalar::Scalar {
    fn from(h: &Merkle) -> Self {
        match *h {
            Merkle::Ed25519(h) => {
                let h = h.compress();
                curve25519_dalek::scalar::Scalar::from_bytes_mod_order(*h.as_bytes())
            }
        }
    }
}

impl From<&super::SerializedMerkle> for curve25519_dalek::scalar::Scalar {
    fn from(h: &super::SerializedMerkle) -> Self {
        let h: Merkle = h.into();
        (&h).into()
    }
}

impl Merkle {
    pub fn zero() -> Self {
        Merkle::Ed25519(ED25519_BASEPOINT_POINT)
    }
    pub fn next<S: Into<curve25519_dalek::scalar::Scalar>>(&self, h: S) -> Self {
        match self {
            Merkle::Ed25519(ref h0) => {
                let scalar = h.into();
                Merkle::Ed25519(h0 * scalar)
            }
        }
    }
    pub fn to_bytes(&self) -> [u8; 32] {
        match *self {
            Merkle::Ed25519(ref e) => e.compress().to_bytes(),
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
        curve25519_dalek::edwards::CompressedEdwardsY::from_slice(&bytes[..32])
            .decompress()
            .map(Merkle::Ed25519)
    }
}

impl super::Base32 for Merkle {
    fn to_base32(&self) -> String {
        match *self {
            Merkle::Ed25519(ref s) => {
                let mut hash = [0; 33];
                (&mut hash[..32]).clone_from_slice(s.compress().as_bytes());
                hash[32] = MerkleAlgorithm::Ed25519 as u8;
                data_encoding::BASE32_NOPAD.encode(&hash)
            }
        }
    }

    /// Parses a base-32 string into a `Merkle`.
    fn from_base32(s: &[u8]) -> Option<Self> {
        let bytes = if let Ok(b) = data_encoding::BASE32_NOPAD.decode(s) {
            b
        } else {
            return None;
        };
        if bytes.len() == 33 && *bytes.last().unwrap() == MerkleAlgorithm::Ed25519 as u8 {
            curve25519_dalek::edwards::CompressedEdwardsY::from_slice(&bytes[..32])
                .decompress()
                .map(Merkle::Ed25519)
        } else {
            None
        }
    }
}

impl std::str::FromStr for Merkle {
    type Err = crate::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(b) = Self::from_base32(s.as_bytes()) {
            Ok(b)
        } else {
            Err(crate::ParseError { s: s.to_string() })
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedMerkle(pub [u8; 33]);

impl PartialEq<Merkle> for SerializedMerkle {
    fn eq(&self, m: &Merkle) -> bool {
        match m {
            Merkle::Ed25519(q) => {
                (self.0)[0] == MerkleAlgorithm::Ed25519 as u8 && {
                    let q = q.compress();
                    q.as_bytes() == &(self.0)[1..]
                }
            }
        }
    }
}

impl PartialEq<SerializedMerkle> for Merkle {
    fn eq(&self, m: &SerializedMerkle) -> bool {
        m.eq(self)
    }
}

impl<'a> From<&'a Merkle> for SerializedMerkle {
    fn from(m: &'a Merkle) -> Self {
        let mut mm = [0; 33];
        match m {
            Merkle::Ed25519(q) => {
                mm[0] = MerkleAlgorithm::Ed25519 as u8;
                let q = q.compress();
                let q = q.as_bytes();
                (&mut mm[1..]).copy_from_slice(q);
                SerializedMerkle(mm)
            }
        }
    }
}

impl From<Merkle> for SerializedMerkle {
    fn from(m: Merkle) -> Self {
        let mut mm = [0; 33];
        match m {
            Merkle::Ed25519(q) => {
                mm[0] = MerkleAlgorithm::Ed25519 as u8;
                let q = q.compress();
                let q = q.as_bytes();
                (&mut mm[1..]).copy_from_slice(q);
                SerializedMerkle(mm)
            }
        }
    }
}

impl<'a> From<&'a SerializedMerkle> for Merkle {
    fn from(m: &'a SerializedMerkle) -> Self {
        assert_eq!((m.0)[0], MerkleAlgorithm::Ed25519 as u8);
        Merkle::Ed25519(
            curve25519_dalek::edwards::CompressedEdwardsY::from_slice(&(m.0)[1..])
                .decompress()
                .unwrap(),
        )
    }
}

impl From<SerializedMerkle> for Merkle {
    fn from(m: SerializedMerkle) -> Self {
        assert_eq!((m.0)[0], MerkleAlgorithm::Ed25519 as u8);
        Merkle::Ed25519(
            curve25519_dalek::edwards::CompressedEdwardsY::from_slice(&(m.0)[1..])
                .decompress()
                .unwrap(),
        )
    }
}
