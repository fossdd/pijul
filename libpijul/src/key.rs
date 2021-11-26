use ed25519_dalek::Signer;
use hmac::Hmac;
use sha2::{Digest, Sha256};

pub const VERSION: u64 = 0;

#[derive(Debug, Error)]
pub enum KeyError {
    #[error("Base 58 decoding error")]
    Encoding(#[from] bs58::decode::Error),
    #[error(transparent)]
    Dalek(#[from] ed25519_dalek::ed25519::Error),
    #[error("No password supplied")]
    NoPassword,
    #[error("The key expired")]
    Expired,
}

#[derive(Serialize, Deserialize)]
pub struct SecretKey {
    pub version: u64,
    pub algorithm: Algorithm,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Encryption>,
    pub key: String,
}

pub enum SKey {
    Ed25519 {
        key: ed25519_dalek::Keypair,
        expires: Option<chrono::DateTime<chrono::Utc>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublicKey {
    pub version: u64,
    pub algorithm: Algorithm,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<chrono::DateTime<chrono::Utc>>,
    pub signature: String,
    pub key: String,
}

#[derive(Debug)]
pub enum PKey {
    Ed25519 {
        expires: Option<chrono::DateTime<chrono::Utc>>,
        signature: String,
        key: ed25519_dalek::PublicKey,
    },
}

#[test]
fn sign_public_key() {
    use chrono::Datelike;
    let expires = chrono::Utc::now();
    let expires = expires.with_year(expires.year() + 1).unwrap();
    let sk = SKey::generate(Some(expires));
    let pk = sk.public_key();
    println!("{:?}", pk);
    let pk = pk.load().unwrap();
    println!("{:?}", pk);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    pub version: u64,
    pub key: PublicKey,
    pub signature: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

impl SKey {
    pub fn sign(&self, h: &[u8]) -> Result<Signature, KeyError> {
        Ok(Signature {
            version: VERSION,
            signature: self.sign_raw(h)?,
            key: self.public_key(),
            date: chrono::Utc::now(),
        })
    }

    pub fn sign_raw(&self, h: &[u8]) -> Result<String, KeyError> {
        match self {
            SKey::Ed25519 { key, expires } => {
                if let Some(expires) = expires {
                    if expires <= &chrono::Utc::now() {
                        return Err(KeyError::Expired);
                    }
                }
                let sig = key.sign(&h);
                Ok(bs58::encode(&sig.to_bytes()).into_string())
            }
        }
    }

    pub fn generate(expires: Option<chrono::DateTime<chrono::Utc>>) -> Self {
        use rand::RngCore;
        let mut key = [0; 32];
        rand::thread_rng().fill_bytes(&mut key);
        let secret = ed25519_dalek::SecretKey::from_bytes(&key).unwrap();
        SKey::Ed25519 {
            key: ed25519_dalek::Keypair {
                public: (&secret).into(),
                secret,
            },
            expires,
        }
    }

    pub fn save(&self, password: Option<&str>) -> SecretKey {
        match self {
            SKey::Ed25519 { key, expires } => {
                let mut key = key.to_bytes();
                let encryption = if let Some(password) = password {
                    use rand::Rng;
                    let salt = rand::thread_rng()
                        .sample_iter(&rand::distributions::Alphanumeric)
                        .take(32)
                        .collect();
                    let enc = Encryption::Aes128(Kdf::Pbkdf2 { salt });
                    enc.encrypt(password.as_bytes(), &mut key);
                    Some(enc)
                } else {
                    None
                };
                SecretKey {
                    version: VERSION,
                    algorithm: Algorithm::Ed25519,
                    expires: expires.clone(),
                    encryption,
                    key: bs58::encode(&key).into_string(),
                }
            }
        }
    }

    pub fn public_key(&self) -> PublicKey {
        match self {
            SKey::Ed25519 { key, expires } => {
                let to_sign =
                    bincode::serialize(&(Algorithm::Ed25519, expires.clone(), key.public)).unwrap();
                debug!("to_sign {:?}", to_sign);
                let sig = key.sign(&to_sign);
                PublicKey {
                    version: VERSION,
                    algorithm: Algorithm::Ed25519,
                    expires: expires.clone(),
                    key: bs58::encode(key.public.as_bytes()).into_string(),
                    signature: bs58::encode(&sig.to_bytes()).into_string(),
                }
            }
        }
    }

    pub fn pkey(&self) -> PKey {
        match self {
            SKey::Ed25519 { key, expires } => {
                let to_sign =
                    bincode::serialize(&(Algorithm::Ed25519, expires.clone(), key.public)).unwrap();
                debug!("to_sign {:?}", to_sign);
                let sig = key.sign(&to_sign);
                PKey::Ed25519 {
                    expires: expires.clone(),
                    key: key.public.clone(),
                    signature: bs58::encode(&sig.to_bytes()).into_string(),
                }
            }
        }
    }
}

impl SecretKey {
    pub fn load(&self, pw: Option<&str>) -> Result<SKey, KeyError> {
        if let Some(expires) = self.expires {
            if expires <= chrono::Utc::now() {
                return Err(KeyError::Expired);
            }
        }
        match self.algorithm {
            Algorithm::Ed25519 => {
                let mut key_enc = [0; 64];
                bs58::decode(self.key.as_bytes()).into(&mut key_enc)?;
                if let Some(ref enc) = self.encryption {
                    let password = if let Some(ref pw) = pw {
                        pw
                    } else {
                        return Err(KeyError::NoPassword);
                    };
                    enc.decrypt(password.as_bytes(), &mut key_enc);
                }
                Ok(SKey::Ed25519 {
                    key: ed25519_dalek::Keypair::from_bytes(&key_enc)?,
                    expires: self.expires,
                })
            }
        }
    }
}

impl PublicKey {
    pub fn fingerprint(&self) -> String {
        match self.algorithm {
            Algorithm::Ed25519 => {
                let signed =
                    bincode::serialize(&(Algorithm::Ed25519, self.expires.clone(), &self.key))
                        .unwrap();
                let mut hash = ed25519_dalek::Sha512::new();
                hash.update(&signed);
                bs58::encode(&hash.finalize()).into_string()
            }
        }
    }

    pub fn load(&self) -> Result<PKey, KeyError> {
        match self.algorithm {
            Algorithm::Ed25519 => {
                let mut key = [0; 32];
                bs58::decode(self.key.as_bytes()).into(&mut key)?;
                let key = ed25519_dalek::PublicKey::from_bytes(&key)?;
                let mut signature = [0; 64];
                bs58::decode(self.signature.as_bytes()).into(&mut signature)?;
                let signature = ed25519_dalek::Signature::from_bytes(&signature)?;

                let msg =
                    bincode::serialize(&(Algorithm::Ed25519, self.expires.clone(), &key)).unwrap();
                key.verify_strict(&msg, &signature)?;
                Ok(PKey::Ed25519 {
                    signature: self.signature.clone(),
                    expires: self.expires.clone(),
                    key,
                })
            }
        }
    }
}

impl PKey {
    pub fn save(&self) -> PublicKey {
        match self {
            PKey::Ed25519 {
                key,
                expires,
                signature,
            } => PublicKey {
                version: VERSION,
                algorithm: Algorithm::Ed25519,
                expires: expires.clone(),
                signature: signature.clone(),
                key: bs58::encode(key.as_bytes()).into_string(),
            },
        }
    }

    pub fn verify(
        &self,
        h: &[u8],
        signature: &str,
        date: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), KeyError> {
        match self {
            PKey::Ed25519 { key, expires, .. } => {
                if let Some(expires) = expires {
                    if expires <= date {
                        return Err(KeyError::Expired);
                    }
                }
                let mut sig = [0; 64];
                bs58::decode(signature.as_bytes()).into(&mut sig)?;
                let sig = ed25519_dalek::Signature::from_bytes(&sig)?;
                key.verify_strict(&h, &sig)?;
                Ok(())
            }
        }
    }
}

#[test]
fn verify_test() {
    use chrono::Datelike;
    let expires = chrono::Utc::now();
    let expires = expires.with_year(expires.year() + 1).unwrap();
    let sk = SKey::generate(Some(expires));
    let m = b"blabla";
    let signature = sk.sign(m).unwrap();
    signature.verify(m).unwrap();
}

impl Signature {
    pub fn verify(&self, h: &[u8]) -> Result<(), KeyError> {
        self.key.load()?.verify(h, &self.signature, &self.date)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum Algorithm {
    Ed25519,
}

impl From<u8> for Algorithm {
    fn from(u: u8) -> Self {
        assert_eq!(u, 0);
        Algorithm::Ed25519
    }
}
impl From<Algorithm> for u8 {
    fn from(u: Algorithm) -> Self {
        match u {
            Algorithm::Ed25519 => 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub enum Encryption {
    Aes128(Kdf),
}

#[derive(Serialize, Deserialize)]
pub enum Kdf {
    Pbkdf2 { salt: String },
}

impl Encryption {
    pub fn encrypt<'a>(&self, password: &[u8], bytes: &'a mut [u8]) {
        match self {
            Encryption::Aes128(Kdf::Pbkdf2 { ref salt }) => {
                let mut kdf = [0; 32];
                pbkdf2::pbkdf2::<Hmac<Sha256>>(password, salt.as_ref(), 10_000, &mut kdf);
                use aes::{
                    cipher::FromBlockCipher, cipher::StreamCipher, Aes128, Aes128Ctr,
                    NewBlockCipher,
                };
                let (a, b) = kdf.split_at(16);
                let cipher = Aes128::new(generic_array::GenericArray::from_slice(&a));
                let mut cipher = Aes128Ctr::from_block_cipher(
                    cipher,
                    generic_array::GenericArray::from_slice(b),
                );
                cipher.apply_keystream(bytes);
            }
        }
    }
    pub fn decrypt<'a>(&self, password: &[u8], bytes: &'a mut [u8]) {
        self.encrypt(password, bytes)
    }
}

#[test]
fn encrypt_decrypt() {
    let enc = Encryption::Aes128(Kdf::Pbkdf2 {
        salt: "blabla".to_string(),
    });
    let b0 = b"very confidential secret".to_vec();
    let mut b = b0.clone();
    enc.encrypt(b"password", &mut b[..]);
    println!("{:?}", b);
    enc.decrypt(b"password", &mut b[..]);
    println!("{:?}", b);
    assert_eq!(b, b0);
}

#[derive(Clone, Copy)]
pub struct SerializedKey {
    pub(crate) t: u8,
    k: K,
}

impl From<ed25519_dalek::PublicKey> for SerializedKey {
    fn from(k: ed25519_dalek::PublicKey) -> Self {
        SerializedKey {
            t: 0,
            k: K {
                ed25519: k.as_bytes().clone(),
            },
        }
    }
}

impl From<SerializedKey> for ed25519_dalek::PublicKey {
    fn from(k: SerializedKey) -> Self {
        assert_eq!(k.t, 0);
        unsafe { ed25519_dalek::PublicKey::from_bytes(&k.k.ed25519).unwrap() }
    }
}

#[derive(Clone, Copy)]
pub(crate) union K {
    ed25519: [u8; 32],
}
