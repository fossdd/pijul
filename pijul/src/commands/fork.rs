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
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path)?;
        debug!("{:?}", repo.config);
        let mut txn = repo.pristine.mut_txn_begin()?;
        if let Some(ref ch) = self.change {
            let (hash, _) = txn.hash_from_prefix(ch)?;
            let channel = txn.open_or_create_channel(&self.to)?;
            let mut channel = channel.write();
            txn.apply_change_rec(&repo.changes, &mut channel, &hash)?
        } else {
            let cur = txn
                .current_channel()
                .unwrap_or(crate::DEFAULT_CHANNEL)
                .to_string();
            let channel_name = if let Some(ref c) = self.channel {
                c
            } else {
                cur.as_str()
            };
            if let Some(channel) = txn.load_channel(&channel_name)? {
                txn.fork(&channel, &self.to)?;
            }
        }
        txn.commit()?;
        Ok(())
    }
}
