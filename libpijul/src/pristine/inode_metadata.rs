/// Metadata about an inode, including unix-style permissions and
/// whether this inode is a directory.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[doc(hidden)]
pub struct InodeMetadata(pub u16);
const DIR_BIT: u16 = 0x200;

impl InodeMetadata {
    pub const DIR: Self = InodeMetadata(DIR_BIT);

    /// Create a new file metadata with the given Unix permissions,
    /// and "is directory" bit.
    pub fn new(perm: usize, is_dir: bool) -> Self {
        let mut m = InodeMetadata(0);
        m.set_permissions((perm & 0x1ff) as u16);
        if is_dir {
            m.set_dir()
        } else {
            m.unset_dir()
        }
        m
    }

    pub fn from_basename(b: &[u8]) -> Self {
        use byteorder::ByteOrder;
        InodeMetadata(byteorder::BigEndian::read_u16(b))
    }

    /// Permissions of this inode (as in Unix).
    pub fn permissions(&self) -> u16 {
        self.0 & 0x1ff
    }

    /// Set the permissions to the supplied parameters.
    pub fn set_permissions(&mut self, perm: u16) {
        self.0 |= perm & 0x1ff
    }

    /// Tell whether this `InodeMetadata` is a directory.
    pub fn is_dir(&self) -> bool {
        self.0 & DIR_BIT != 0
    }

    /// Tell whether this `InodeMetadata` is a file.
    pub fn is_file(&self) -> bool {
        self.0 & DIR_BIT == 0
    }

    /// Set the metadata to be a directory.
    pub fn set_dir(&mut self) {
        self.0 |= DIR_BIT
    }

    /// Set the metadata to be a file.
    pub fn unset_dir(&mut self) {
        self.0 &= 0o777
    }
}
