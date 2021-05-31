use super::change_id::*;
use super::vertex::*;
use super::L64;

bitflags! {
    /// Possible flags of edges.
    #[derive(Serialize, Deserialize)]
    pub struct EdgeFlags: u8 {
        const BLOCK = 1;
        /// A pseudo-edge, computed when applying the change to
        /// restore connectivity, and/or mark conflicts.
        const PSEUDO = 4;
        /// An edge encoding file system hierarchy.
        const FOLDER = 16;
        /// A "reverse" edge (all edges in the graph have a reverse edge).
        const PARENT = 32;
        /// An edge whose target (if not also `PARENT`) or
        /// source (if also `PARENT`) is marked as deleted.
        const DELETED = 128;
    }
}

impl EdgeFlags {
    #[inline]
    pub(crate) fn db() -> Self {
        Self::DELETED | Self::BLOCK
    }

    #[inline]
    pub(crate) fn bp() -> Self {
        Self::BLOCK | Self::PARENT
    }

    #[inline]
    pub(crate) fn pseudof() -> Self {
        Self::PSEUDO | Self::FOLDER
    }

    #[inline]
    pub(crate) fn alive_children() -> Self {
        Self::BLOCK | Self::PSEUDO | Self::FOLDER
    }

    #[inline]
    pub(crate) fn parent_folder() -> Self {
        Self::PARENT | Self::FOLDER
    }

    #[inline]
    pub(crate) fn is_deleted(&self) -> bool {
        self.contains(EdgeFlags::DELETED)
    }

    #[inline]
    pub fn is_alive_parent(&self) -> bool {
        *self & (EdgeFlags::DELETED | EdgeFlags::PARENT) == EdgeFlags::PARENT
    }
    #[inline]
    pub(crate) fn is_parent(&self) -> bool {
        self.contains(EdgeFlags::PARENT)
    }
    #[inline]
    pub(crate) fn is_folder(&self) -> bool {
        self.contains(EdgeFlags::FOLDER)
    }
    #[inline]
    pub(crate) fn is_block(&self) -> bool {
        self.contains(EdgeFlags::BLOCK)
    }
}

/// The target half of an edge in the repository graph.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[doc(hidden)]
pub struct Edge {
    /// Flags of this edge.
    pub flag: EdgeFlags,
    /// Target of this edge.
    pub dest: Position<ChangeId>,
    /// Change that introduced this edge (possibly as a
    /// pseudo-edge, i.e. not explicitly in the change, but
    /// computed from it).
    pub introduced_by: ChangeId,
}

/// The target half of an edge in the repository graph.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SerializedEdge([super::L64; 3]);

impl std::fmt::Debug for SerializedEdge {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        let pos = self.dest();
        use super::Base32;
        write!(
            fmt,
            "E({:?}, {}[{}], {})",
            self.flag(),
            pos.change.to_base32(),
            (pos.pos.0).0,
            self.introduced_by().to_base32()
        )
    }
}

impl std::ops::SubAssign<EdgeFlags> for SerializedEdge {
    fn sub_assign(&mut self, e: EdgeFlags) {
        let ref mut f = (self.0)[0];
        f.0 = ((u64::from_le(f.0)) & !((e.bits() as u64) << 56)).to_le()
    }
}

impl SerializedEdge {
    pub fn flag(&self) -> EdgeFlags {
        let f = u64::from_le((self.0)[0].0);
        EdgeFlags::from_bits((f >> 56) as u8).unwrap()
    }
    pub fn dest(&self) -> Position<ChangeId> {
        let pos = u64::from_le((self.0[0]).0);
        Position {
            change: ChangeId((self.0)[1]),
            pos: ChangePosition(L64((pos & 0xffffffffffffff).to_le())),
        }
    }
    pub fn introduced_by(&self) -> ChangeId {
        ChangeId((self.0)[2])
    }
}

impl From<SerializedEdge> for Edge {
    fn from(s: SerializedEdge) -> Edge {
        Edge {
            flag: s.flag(),
            dest: s.dest(),
            introduced_by: s.introduced_by(),
        }
    }
}

impl From<Edge> for SerializedEdge {
    fn from(s: Edge) -> SerializedEdge {
        let pos = u64::from_le((s.dest.pos.0).0);
        assert!(pos < 1 << 56);
        SerializedEdge([
            (((s.flag.bits() as u64) << 56) | pos).into(),
            s.dest.change.0,
            s.introduced_by.0,
        ])
    }
}

impl<'a> From<&'a SerializedEdge> for Edge {
    fn from(s: &'a SerializedEdge) -> Edge {
        Edge {
            flag: s.flag(),
            dest: s.dest(),
            introduced_by: s.introduced_by(),
        }
    }
}
impl SerializedEdge {
    pub fn empty(dest: Position<ChangeId>, intro: ChangeId) -> Self {
        SerializedEdge([dest.pos.0, dest.change.0, intro.0])
    }

    pub fn new(flag: EdgeFlags, change: ChangeId, pos: ChangePosition, intro: ChangeId) -> Self {
        let pos = u64::from_le((pos.0).0);
        assert!(pos < 1 << 56);
        SerializedEdge([
            (pos | ((flag.bits() as u64) << 56)).into(),
            change.0,
            intro.0,
        ])
    }
}
