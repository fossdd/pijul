use super::change_id::*;
use super::L64;

/// A node in the repository graph, made of a change internal
/// identifier, and a line identifier in that change.
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct Vertex<H> {
    /// The change that introduced this node.
    pub change: H,
    /// The line identifier of the node in that change. Here,
    /// "line" does not imply anything on the contents of the
    /// chunk.
    pub start: ChangePosition,
    pub end: ChangePosition,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Vertex<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            fmt,
            "V({:?}[{}:{}])",
            self.change,
            (self.start.0).0,
            (self.end.0).0
        )
    }
}

impl Vertex<ChangeId> {
    /// The node at the root of the repository graph.
    pub const ROOT: Vertex<ChangeId> = Vertex {
        change: ChangeId::ROOT,
        start: ChangePosition::ROOT,
        end: ChangePosition::ROOT,
    };

    /// The node at the root of the repository graph.
    pub(crate) const BOTTOM: Vertex<ChangeId> = Vertex {
        change: ChangeId::ROOT,
        start: ChangePosition::BOTTOM,
        end: ChangePosition::BOTTOM,
    };

    /// Is this the root key? (the root key is all 0s).
    pub fn is_root(&self) -> bool {
        self == &Vertex::ROOT
    }

    pub fn to_option(&self) -> Vertex<Option<ChangeId>> {
        Vertex {
            change: Some(self.change),
            start: self.start,
            end: self.end,
        }
    }
}

impl<H: Clone> Vertex<H> {
    /// Convenience function to get the start position of a
    /// [`Vertex<ChangeId>`](struct.Vertex.html) as a
    /// [`Position`](struct.Position.html).
    pub fn start_pos(&self) -> Position<H> {
        Position {
            change: self.change.clone(),
            pos: self.start,
        }
    }

    /// Convenience function to get the end position of a
    /// [`Vertex<ChangeId>`](struct.Vertex.html) as a
    /// [`Position`](struct.Position.html).
    pub fn end_pos(&self) -> Position<H> {
        Position {
            change: self.change.clone(),
            pos: self.end,
        }
    }

    /// Is this vertex of zero length?
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }

    /// Length of this key, in bytes.
    pub fn len(&self) -> usize {
        self.end - self.start
    }
}
/// The position of a byte within a change.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct ChangePosition(pub super::L64);

impl ChangePosition {
    pub(crate) const ROOT: ChangePosition = ChangePosition(L64(0u64));
    pub(crate) const BOTTOM: ChangePosition = ChangePosition(L64(1u64.to_le()));
}

impl std::ops::Add<usize> for ChangePosition {
    type Output = ChangePosition;
    fn add(self, x: usize) -> Self::Output {
        ChangePosition(self.0 + x)
    }
}

impl std::ops::Sub<ChangePosition> for ChangePosition {
    type Output = usize;
    fn sub(self, x: ChangePosition) -> Self::Output {
        let a: u64 = self.0.into();
        let b: u64 = x.0.into();
        (a - b) as usize
    }
}

impl ChangePosition {
    pub(crate) fn us(&self) -> usize {
        u64::from_le((self.0).0) as usize
    }
}

impl From<ChangePosition> for u64 {
    fn from(f: ChangePosition) -> u64 {
        u64::from_le((f.0).0)
    }
}

/// A byte identifier, i.e. a change together with a position.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
#[doc(hidden)]
pub struct Position<P> {
    pub change: P,
    pub pos: ChangePosition,
}

use super::Base32;

impl<H: super::Base32> Base32 for Position<H> {
    fn to_base32(&self) -> String {
        let mut v = self.change.to_base32();
        let mut bytes = [0; 8];
        self.pos.0.to_slice_le(&mut bytes);
        let mut i = 7;
        while i > 2 && bytes[i] == 0 {
            i -= 1
        }
        i += 1;
        let len = data_encoding::BASE32_NOPAD.encode_len(i);
        let len0 = v.len() + 1;
        v.push_str("..............");
        v.truncate(len0 + len);
        data_encoding::BASE32_NOPAD.encode_mut(&bytes[..i], unsafe {
            v.split_at_mut(len0).1.as_bytes_mut()
        });
        v
    }
    fn from_base32(s: &[u8]) -> Option<Self> {
        let n = s.iter().position(|c| *c == b'.')?;
        let (s, pos) = s.split_at(n);
        let pos = &pos[1..];
        let change = H::from_base32(s)?;
        let mut dec = [0; 8];
        let len = data_encoding::BASE32_NOPAD.decode_len(pos.len()).ok()?;
        let pos = data_encoding::BASE32_NOPAD
            .decode_mut(pos, &mut dec[..len])
            .map(|_| L64::from_slice_le(&dec))
            .ok()?;
        Some(Position {
            change,
            pos: ChangePosition(pos),
        })
    }
}

impl<H> std::ops::Add<usize> for Position<H> {
    type Output = Position<H>;
    fn add(self, x: usize) -> Self::Output {
        Position {
            change: self.change,
            pos: self.pos + x,
        }
    }
}

impl<H> Position<Option<H>> {
    pub fn unwrap(self) -> Position<H> {
        Position {
            change: self.change.unwrap(),
            pos: self.pos,
        }
    }
}

impl<H> Vertex<Option<H>> {
    pub fn unwrap(self) -> Vertex<H> {
        Vertex {
            change: self.change.unwrap(),
            start: self.start,
            end: self.end,
        }
    }
}

impl Position<ChangeId> {
    pub fn inode_vertex(&self) -> Vertex<ChangeId> {
        Vertex {
            change: self.change,
            start: self.pos,
            end: self.pos,
        }
    }

    pub fn is_root(&self) -> bool {
        self.change.is_root()
    }

    pub fn to_option(&self) -> Position<Option<ChangeId>> {
        Position {
            change: Some(self.change),
            pos: self.pos,
        }
    }

    pub const ROOT: Position<ChangeId> = Position {
        change: ChangeId::ROOT,
        pos: ChangePosition(L64(0u64)),
    };

    pub(crate) const OPTION_ROOT: Position<Option<ChangeId>> = Position {
        change: Some(ChangeId::ROOT),
        pos: ChangePosition(L64(0u64)),
    };

    pub(crate) const BOTTOM: Position<ChangeId> = Position {
        change: ChangeId::ROOT,
        pos: ChangePosition::BOTTOM,
    };
}
