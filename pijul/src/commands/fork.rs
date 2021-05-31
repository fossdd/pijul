use std::path::PathBuf;

use clap::Clap;
use libpijul::{MutTxnT, MutTxnTExt, TxnT};
use log::debug;

use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Fork {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Make the new channel from this channel instead of the current channel
    #[clap(long = "channel", conflicts_with = "change")]
    channel: Option<String>,
    /// Apply this change after creating the channel
    #[clap(long = "change", conflicts_with = "channel")]
    change: Option<String>,
    /// The name of the new channel
    to: String,
}

impl Fork {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path).await?;
        debug!("{:?}", repo.config);
        let mut txn = repo.pristine.mut_txn_begin()?;
        if let Some(ref ch) = self.change {
            let (hash, _) = txn.hash_from_prefix(ch)?;
            let channel = txn.open_or_create_channel(&self.to)?;
            let mut channel = channel.write().unwrap();
            txn.apply_change_rec(&repo.changes, &mut channel, &hash)?
        } else {
            let (channel_name, _) = repo.config.get_current_channel(self.channel.as_deref());
            if let Some(channel) = txn.load_channel(channel_name)? {
                txn.fork(&channel, &self.to)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
}
