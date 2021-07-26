use std::path::PathBuf;

use clap::Clap;
use libpijul::MutTxnT;

use crate::repository::*;

#[derive(Clap, Debug)]
pub struct Init {
    /// Set the name of the current channel (defaults to "main").
    #[clap(long = "channel")]
    channel: Option<String>,
    /// Path where the repository should be initalized
    path: Option<PathBuf>,
}

impl Init {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::init(self.path)?;
        let mut txn = repo.pristine.mut_txn_begin()?;
        let channel_name = self
            .channel
            .unwrap_or_else(|| crate::DEFAULT_CHANNEL.to_string());
        txn.open_or_create_channel(&channel_name)?;
        txn.set_current_channel(&channel_name)?;
        txn.commit()?;
        Ok(())
    }
}
