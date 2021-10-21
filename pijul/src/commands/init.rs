use std::path::PathBuf;

use clap::Parser;
use libpijul::MutTxnT;

use crate::repository::*;

#[derive(Parser, Debug)]
pub struct Init {
    /// Set the name of the current channel (defaults to "main").
    #[clap(long = "channel")]
    channel: Option<String>,
    /// Project kind; if Pijul knows about your project kind, the .ignore file will be
    /// populated with a conservative list of commonly ignored entries.
    /// Example: `pijul init --kind=rust`
    #[clap(long = "kind", short = 'k')]
    kind: Option<String>,
    /// Path where the repository should be initalized
    path: Option<PathBuf>,
}

impl Init {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::init(self.path, self.kind.as_ref())?;
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
