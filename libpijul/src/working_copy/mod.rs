use crate::chardetng::EncodingDetector;

use crate::pristine::InodeMetadata;
use crate::text_encoding::Encoding;

#[cfg(feature = "ondisk-repos")]
pub mod filesystem;
#[cfg(feature = "ondisk-repos")]
pub use filesystem::FileSystem;

pub mod memory;
pub use memory::Memory;

pub trait WorkingCopy {
    type Error: std::error::Error + Send;
    fn create_dir_all(&self, path: &str) -> Result<(), Self::Error>;
    fn file_metadata(&self, file: &str) -> Result<InodeMetadata, Self::Error>;
    fn read_file(&self, file: &str, buffer: &mut Vec<u8>) -> Result<(), Self::Error>;
    fn modified_time(&self, file: &str) -> Result<std::time::SystemTime, Self::Error>;
    fn remove_path(&self, name: &str, rec: bool) -> Result<(), Self::Error>;
    fn rename(&self, former: &str, new: &str) -> Result<(), Self::Error>;
    fn set_permissions(&self, name: &str, permissions: u16) -> Result<(), Self::Error>;

    type Writer: std::io::Write;
    fn write_file(&self, file: &str) -> Result<Self::Writer, Self::Error>;
    /// Read the file into the buffer
    ///
    /// Returns the file's text encoding or None if it was a binary file
    fn decode_file(
        &self,
        file: &str,
        buffer: &mut Vec<u8>,
    ) -> Result<Option<Encoding>, Self::Error> {
        let init = buffer.len();
        self.read_file(&file, buffer)?;
        let mut detector = EncodingDetector::new();
        detector.feed(&buffer[init..], true);
        let (encoding, score) = detector.guess_assess(None, true);
        if score {
            Ok(Some(Encoding(encoding)))
        } else {
            Ok(None)
        }
    }
}
