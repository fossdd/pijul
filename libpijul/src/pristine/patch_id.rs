use byteorder::{ByteOrder, LittleEndian};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct ChangeId(pub u64);

impl std::fmt::Debug for ChangeId {
fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
write!(fmt, "ChangeId({})", self.to_base32())
}
}

impl ChangeId {
pub(crate) const ROOT: ChangeId = ChangeId(0);
pub fn is_root(&self) -> bool {
*self == ChangeId::ROOT
}

pub fn to_base32(&self) -> String {
let mut b = [0; 8];
LittleEndian::write_u64(&mut b, self.0);
base32::encode(base32::Alphabet::Crockford, &b)
}
}
