use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;
use clap::Clap;
use libpijul::changestore::*;
use libpijul::{Base32, TxnT, TxnTExt};

use crate::repository::Repository;

#[derive(Clap, Debug)]
pub struct Log {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Show logs for this channel instead of the current channel
    #[clap(long = "channel")]
    channel: Option<String>,
    /// Only show the change hashes
    #[clap(long = "hash-only")]
    hash_only: bool,
    /// Include state identifiers in the output
    #[clap(long = "state")]
    states: bool,
    /// Include full change description in the output
    #[clap(long = "description")]
    descriptions: bool,
}

impl Log {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let repo = unsafe { Repository::find_root(self.repo_path).await? };
        let txn = repo.pristine.txn_begin()?;
        let (channel_name, _) = repo.config.get_current_channel(self.channel.as_deref());
        let channel = if let Some(channel) = txn.load_channel(channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };
        super::pager();
        let changes = repo.changes;
        let mut stdout = std::io::stdout();
        if self.hash_only {
            for h in txn.reverse_log(&*channel.read()?, None)? {
                let h: libpijul::Hash = (h?.1).0.into();
                writeln!(stdout, "{}", h.to_base32())?
            }
        } else {
            let states = self.states;
            for h in txn.reverse_log(&*channel.read()?, None)? {
                let (h, mrk) = h?.1;
                let h: libpijul::Hash = h.into();
                let mrk: libpijul::Merkle = mrk.into();
                let header = changes.get_header(&h.into())?;
                writeln!(stdout, "Change {}", h.to_base32())?;
                writeln!(stdout, "Author: {:?}", header.authors)?;
                writeln!(stdout, "Date: {}", header.timestamp)?;
                if states {
                    writeln!(stdout, "State: {}", mrk.to_base32())?;
                }
                writeln!(stdout, "\n    {}\n", header.message)?;
                if self.descriptions {
                    if let Some(ref descr) = header.description {
                        writeln!(stdout, "\n    {}\n", descr)?;
                    }
                }
            }
        }
        Ok(())
    }
}
