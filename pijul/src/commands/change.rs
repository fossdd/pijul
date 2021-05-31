use std::path::PathBuf;

use clap::Clap;
use libpijul::change::Local;
use libpijul::changestore::ChangeStore;
use libpijul::*;

use crate::repository::*;

#[derive(Clap, Debug)]
pub struct Change {
    /// Use the repository at PATH instead of the current directory
    #[clap(long = "repository", value_name = "PATH")]
    repo_path: Option<PathBuf>,
    /// The hash of the change to show, or an unambiguous prefix thereof
    #[clap(value_name = "HASH")]
    hash: Option<String>,
}

impl Change {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = unsafe { Repository::find_root(self.repo_path.clone()).await? };
        let txn = repo.pristine.txn_begin()?;
        let changes = repo.changes;

        let hash = if let Some(hash) = self.hash {
            txn.hash_from_prefix(&hash)?.0
        } else {
            let (channel_name, _) = repo.config.get_current_channel(None);
            let channel = if let Some(channel) = txn.load_channel(channel_name)? {
                channel
            } else {
                return Ok(());
            };
            let channel = channel.read()?;
            if let Some(h) = txn.reverse_log(&*channel, None)?.next() {
                (h?.1).0.into()
            } else {
                return Ok(());
            }
        };
        let change = changes.get_change(&hash).unwrap();
        let file_name = |l: &Local, _| format!("{}:{}", l.path, l.line);
        let colors = super::diff::is_colored();
        change.write(
            &changes,
            Some(hash),
            file_name,
            true,
            super::diff::Colored {
                w: termcolor::StandardStream::stdout(termcolor::ColorChoice::Auto),
                colors,
            },
        )?;
        Ok(())
    }
}
