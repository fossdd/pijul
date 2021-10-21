use std::path::PathBuf;

use clap::Parser;
use libpijul::changestore::ChangeStore;
use libpijul::*;

use crate::repository::*;

#[derive(Parser, Debug)]
pub struct Change {
    /// Use the repository at PATH instead of the current directory
    #[clap(long = "repository", value_name = "PATH")]
    repo_path: Option<PathBuf>,
    /// The hash of the change to show, or an unambiguous prefix thereof
    #[clap(value_name = "HASH")]
    hash: Option<String>,
}

impl Change {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path.clone())?;
        let txn = repo.pristine.txn_begin()?;
        let changes = repo.changes;

        let hash = if let Some(hash) = self.hash {
            txn.hash_from_prefix(&hash)?.0
        } else {
            let channel_name = txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL);
            let channel = if let Some(channel) = txn.load_channel(&channel_name)? {
                channel
            } else {
                return Ok(());
            };
            let channel = channel.read();
            if let Some(h) = txn.reverse_log(&*channel, None)?.next() {
                (h?.1).0.into()
            } else {
                return Ok(());
            }
        };
        let change = changes.get_change(&hash).unwrap();
        let colors = super::diff::is_colored();
        change.write(
            &changes,
            Some(hash),
            true,
            super::diff::Colored {
                w: termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto),
                colors,
            },
        )?;
        Ok(())
    }
}
