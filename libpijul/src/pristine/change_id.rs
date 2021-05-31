use super::{Base32, L64};

#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
#[doc(hidden)]
pub struct ChangeId(pub super::L64);

impl std::fmt::Debug for ChangeId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "ChangeId({})", self.to_base32())
    }
}

impl ChangeId {
    pub const ROOT: ChangeId = ChangeId(L64(0));
    pub fn is_root(&self) -> bool {
        *self == ChangeId::ROOT
    }
}

impl super::Base32 for ChangeId {
    fn to_base32(&self) -> String {
        let mut b = [0; 8];
        self.0.to_slice_le(&mut b);
        data_encoding::BASE32_NOPAD.encode(&b)
    }
    fn from_base32(b: &[u8]) -> Option<Self> {
        let mut dec = [0; 8];
        let len = if let Ok(len) = data_encoding::BASE32_NOPAD.decode_len(b.len()) {
            len
        } else {
            return None;
        };
        if len > 8 {
            return None;
        }
        if data_encoding::BASE32_NOPAD
            .decode_mut(b, &mut dec[..len])
            .is_ok()
        {
            Some(ChangeId(L64::from_slice_le(&dec)))
        } else {
            None
        }
    }
}
