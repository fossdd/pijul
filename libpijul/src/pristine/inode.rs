use super::L64;
/// A unique identifier for files or directories in the actual
/// file system, to map "files from the graph" to real files.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Inode(pub(in crate) super::L64);
use byteorder::{BigEndian, ByteOrder};
use std::str::FromStr;

impl std::fmt::Debug for Inode {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, (self.0).0); // self.0.to_slice_le(&mut b);
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
