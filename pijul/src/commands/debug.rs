use std::path::PathBuf;

use crate::repository::Repository;
use anyhow::bail;
use clap::Parser;
use libpijul::{TxnT, TxnTExt};

#[derive(Parser, Debug)]
pub struct Debug {
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    #[clap(long = "channel")]
    channel: Option<String>,
    #[clap(long = "sanakirja-only")]
    sanakirja_only: bool,
    root: Option<String>,
}

impl Debug {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let repo = Repository::find_root(self.repo_path)?;
        let txn = repo.pristine.txn_begin()?;
        let channel_name = if let Some(ref c) = self.channel {
            c
        } else {
            txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL)
        }
        .to_string();
        let channel = if let Some(channel) = txn.load_channel(&channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };
        if !self.sanakirja_only {
            libpijul::pristine::debug_inodes(&txn);
            libpijul::pristine::debug_dep(&txn);
            libpijul::pristine::debug_revdep(&txn);
            libpijul::pristine::debug_revinodes(&txn);
            libpijul::pristine::debug_tree_print(&txn);
            libpijul::pristine::debug_revtree_print(&txn);
            libpijul::pristine::debug_remotes(&txn);
            if let Some(root) = self.root {
                let (pos, _) = txn
                    .follow_oldest_path(&repo.changes, &channel, &root)
                    .unwrap();
                libpijul::pristine::debug_root(
                    &txn,
                    &channel.read(),
                    pos.inode_vertex(),
                    std::io::stdout(),
                    true,
                )?;
            } else {
                let channel = channel.read();
                libpijul::pristine::debug(&txn, &channel, std::io::stdout())?;
            }
            libpijul::pristine::check_alive_debug(&repo.changes, &txn, &*channel.read(), 0)?;
        }
        ::sanakirja::debug::debug(&txn.txn, &[&txn.tree], "debug.tree", true);
        eprintln!(
            "{:#?}",
            txn.check_database(&mut std::collections::BTreeMap::new())
        );
        let channel = channel.read();
        ::sanakirja::debug::debug(&txn.txn, &[&channel.graph], "debug.sanakirja", true);
        Ok(())
    }
}
