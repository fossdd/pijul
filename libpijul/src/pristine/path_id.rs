use super::inode::*;
use crate::small_string::*;

/// A key in the file tree, i.e. a directory (`parent_inode`) and the
/// name of the child (file or directory).
#[doc(hidden)]
#[derive(Debug, Hash, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct OwnedPathId {
    /// The parent of this path.
    pub parent_inode: Inode,
    /// Name of the file.
    pub basename: SmallString,
}

impl OwnedPathId {
    pub fn inode(parent_inode: Inode) -> Self {
        OwnedPathId {
            parent_inode,
            basename: SmallString::new(),
        }
    }
}

/// A borrow on a [`OwnedPathId`](struct.OwnedPathId.html).
#[derive(Hash, Eq, PartialEq, Ord, PartialOrd)]
#[doc(hidden)]
pub struct PathId {
    pub parent_inode: Inode,
    pub basename: SmallStr,
}

impl std::fmt::Debug for PathId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "{:?}:{}", self.parent_inode, self.basename.as_str())
    }
}

impl PathId {
    /// Make an owned version of this `PathId`.
    pub fn to_owned(&self) -> OwnedPathId {
        OwnedPathId {
            parent_inode: self.parent_inode.clone(),
            basename: self.basename.to_owned(),
        }
    }
}

impl std::ops::Deref for OwnedPathId {
    type Target = PathId;
    fn deref(&self) -> &Self::Target {
        let len = 1 + self.basename.len as usize;
        unsafe {
            std::mem::transmute(std::slice::from_raw_parts(
                self as *const Self as *const u8,
                len,
            ))
        }
    }
}

#[test]
fn pathid() {
    let o = OwnedPathId {
        parent_inode: Inode::ROOT,
        basename: SmallString::from_str("blablabla"),
    };
    use std::ops::Deref;
    println!("{:?} {:?}", o.basename.len, o.deref());
}
