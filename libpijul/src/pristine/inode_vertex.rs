use super::change_id::*;
use super::inode_metadata::*;
use super::vertex::*;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
#[doc(hidden)]
pub struct InodeVertex {
    pub metadata: InodeMetadata,
    pub position: Position<ChangeId>,
}
