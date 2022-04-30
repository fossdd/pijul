use super::L64;
/// A unique identifier for files or directories in the actual
/// file system, to map "files from the graph" to real files.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Inode(pub super::L64);
use byteorder::{BigEndian, ByteOrder};
use std::str::FromStr;

impl std::fmt::Debug for Inode {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, (self.0).0);
        write!(fmt, "Inode({})", data_encoding::BASE32_NOPAD.encode(&b))
    }
}

impl Inode {
    pub const ROOT: Inode = Inode(L64(0u64));
    pub fn is_root(&self) -> bool {
        *self == Inode::ROOT
    }
}

impl FromStr for Inode {
    type Err = <u64 as FromStr>::Err;
    fn from_str(x: &str) -> Result<Self, Self::Err> {
        Ok(x.parse::<u64>()?.into())
    }
}

impl From<u64> for Inode {
    fn from(x: u64) -> Inode {
        Inode(x.into())
    }
}

impl From<Inode> for u64 {
    fn from(x: Inode) -> u64 {
        x.0.into()
    }
}

use super::Base32;

impl Base32 for Inode {
    fn to_base32(&self) -> String {
        let inode: u64 = self.0.into();
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, inode);
        let mut bb = [0; 13];
        data_encoding::BASE32_NOPAD.encode_mut(&b, &mut bb);
        let b = std::str::from_utf8(&bb).unwrap();
        b.to_string()
    }
    fn from_base32(s: &[u8]) -> Option<Self> {
        let mut b = [0; 8];
        if s.len() == 13 && data_encoding::BASE32_NOPAD.decode_mut(s, &mut b).is_ok() {
            Some(Inode(BigEndian::read_u64(&b).into()))
        } else {
            None
        }
    }
}

pub mod inode_base32_serde {
    use super::*;
    use serde::*;

    pub struct InodeDe {}

    impl<'de> serde::de::Visitor<'de> for InodeDe {
        type Value = Inode;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a base32-encoded string")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let mut b = [0; 8];
            if s.len() == 13
                && data_encoding::BASE32_NOPAD
                    .decode_mut(s.as_bytes(), &mut b)
                    .is_ok()
            {
                let b: u64 = BigEndian::read_u64(&b);
                Ok(Inode(b.into()))
            } else {
                Err(de::Error::invalid_value(
                    serde::de::Unexpected::Str(s),
                    &self,
                ))
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Inode, D::Error> {
        d.deserialize_str(InodeDe {})
    }

    pub fn serialize<S: Serializer>(inode: &Inode, s: S) -> Result<S::Ok, S::Error> {
        let inode: u64 = inode.0.into();
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, inode);
        let mut bb = [0; 13];
        data_encoding::BASE32_NOPAD.encode_mut(&b, &mut bb);
        let b = std::str::from_utf8(&bb).unwrap();
        s.serialize_str(b)
    }
}
