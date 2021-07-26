use std::io::Write;
use std::path::PathBuf;

use crate::repository::Repository;
use anyhow::anyhow;
use anyhow::bail;
use clap::Clap;
use libpijul::{ChannelTxnT, MutTxnT, TxnT};

#[derive(Clap, Debug)]
pub struct Channel {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    #[clap(subcommand)]
    subcmd: Option<SubCommand>,
}

#[derive(Clap, Debug)]
pub enum SubCommand {
    /// Delete a channel.
    /// The channel must not be the current channel.
    #[clap(name = "delete")]
    Delete { delete: String },
    /// Rename a channel.
    #[clap(name = "rename")]
    Rename { from: String, to: Option<String> },
    /// Switch to a channel.
    /// There must not be unrecorded changes in the working copy.
    #[clap(name = "switch")]
    Switch { to: Option<String> },
    /// Create a new, empty channel.
    #[clap(name = "new")]
    New { name: String },
}

impl Channel {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let mut stdout = std::io::stdout();
        match self.subcmd {
            None => {
                let repo = Repository::find_root(self.repo_path)?;
                let txn = repo.pristine.txn_begin()?;
                let current = txn.current_channel().ok();
                for channel in txn.iter_channels("")? {
                    let (_, channel) = channel?;
                    let channel = channel.read();
                    let name = txn.name(&*channel);
                    if current == Some(name) {
                        writeln!(stdout, "* {}", name)?;
                    } else {
                        writeln!(stdout, "  {}", name)?;
                    }
                }
            }
            Some(SubCommand::Delete { ref delete }) => {
                let repo = Repository::find_root(self.repo_path)?;
                let mut txn = repo.pristine.mut_txn_begin()?;
                let current = txn.current_channel().ok();
                if Some(delete.as_str()) == current {
                    bail!("Cannot delete current channel")
                }
                if !txn.drop_channel(delete)? {
                    return Err(anyhow!("Channel {} not found", delete));
                }
                txn.commit()?;
            }
            Some(SubCommand::Switch { to }) => {
                (crate::commands::reset::Reset {
                    repo_path: self.repo_path,
                    channel: to,
                    dry_run: false,
                    files: Vec::new(),
                })
                .switch()?;
            }
            Some(SubCommand::Rename { ref from, ref to }) => {
                let repo = Repository::find_root(self.repo_path)?;
                let mut txn = repo.pristine.mut_txn_begin()?;
                let current = txn.current_channel().ok();
                let (from, to) = if let Some(to) = to {
                    (from.as_str(), to.as_str())
                } else if let Some(current) = current {
                    (current, from.as_str())
                } else {
                    bail!("No current channel")
                };
                let mut channel = if let Some(channel) = txn.load_channel(from)? {
                    channel
                } else {
                    bail!("No such channel: {:?}", from)
                };
                txn.rename_channel(&mut channel, to)?;
                txn.set_current_channel(&to)?;
                txn.commit()?;
            }
            Some(SubCommand::New { name }) => {
                let repo = Repository::find_root(self.repo_path)?;
                let mut txn = repo.pristine.mut_txn_begin()?;
                if txn.load_channel(&name)?.is_some() {
                    bail!("Channel {} already exists", name)
                }
                txn.open_or_create_channel(&name)?;
                txn.commit()?;
            }
        }
        Ok(())
    }
}
