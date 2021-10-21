use super::{Base32, L64};
use byteorder::{ByteOrder, LittleEndian};

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

pub mod changeid_base32_serde {
    use super::*;
    use serde::*;

    pub struct ChangeIdDe {}

    impl<'de> serde::de::Visitor<'de> for ChangeIdDe {
        type Value = ChangeId;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(formatter, "a base32-encoded string")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let mut b = [0; 8];
            if data_encoding::BASE32_NOPAD
                .decode_mut(s.as_bytes(), &mut b)
                .is_ok()
            {
                let b: u64 = LittleEndian::read_u64(&b);
                Ok(ChangeId(b.into()))
            } else {
                Err(de::Error::invalid_value(
                    serde::de::Unexpected::Str(s),
                    &self,
                ))
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<ChangeId, D::Error> {
        d.deserialize_str(ChangeIdDe {})
    }

    pub fn serialize<S: Serializer>(inode: &ChangeId, s: S) -> Result<S::Ok, S::Error> {
        let inode: u64 = inode.0.into();
        let mut b = [0; 8];
        LittleEndian::write_u64(&mut b, inode);
        let mut bb = [0; 13];
        data_encoding::BASE32_NOPAD.encode_mut(&b, &mut bb);
        let b = std::str::from_utf8(&bb).unwrap();
        s.serialize_str(b)
    }
}
