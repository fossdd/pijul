use crate::pristine::InodeMetadata;

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
    fn remove_path(&self, name: &str) -> Result<(), Self::Error>;
    fn rename(&self, former: &str, new: &str) -> Result<(), Self::Error>;
    fn set_permissions(&self, name: &str, permissions: u16) -> Result<(), Self::Error>;

    type Writer: std::io::Write;
    fn write_file(&self, file: &str) -> Result<Self::Writer, Self::Error>;
}
