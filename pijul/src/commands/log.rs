use std::collections::hash_map::Entry;
use std::collections::HashMap;
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
        let repo = Repository::find_root(self.repo_path)?;
        let txn = repo.pristine.txn_begin()?;
        let channel_name = if let Some(ref c) = self.channel {
            c
        } else {
            txn.current_channel().unwrap_or(crate::DEFAULT_CHANNEL)
        };
        let channel = if let Some(channel) = txn.load_channel(channel_name)? {
            channel
        } else {
            bail!("No such channel: {:?}", channel_name)
        };
        super::pager();
        let changes = repo.changes;
        let mut stdout = std::io::stdout();
        if self.hash_only {
            for h in txn.reverse_log(&*channel.read(), None)? {
                let h: libpijul::Hash = (h?.1).0.into();
                writeln!(stdout, "{}", h.to_base32())?
            }
        } else {
            let states = self.states;
            let mut authors = HashMap::new();
            let mut id_path = repo.path.join(libpijul::DOT_DIR);
            id_path.push("identities");

            for h in txn.reverse_log(&*channel.read(), None)? {
                let (h, mrk) = h?.1;
                let h: libpijul::Hash = h.into();
                let mrk: libpijul::Merkle = mrk.into();
                let header = changes.get_header(&h.into())?;
                writeln!(stdout, "Change {}", h.to_base32())?;
                write!(stdout, "Author: ")?;
                let mut is_first = true;
                for mut auth in header.authors.into_iter() {
                    let auth = if let Some(k) = auth.0.remove("key") {
                        match authors.entry(k) {
                            Entry::Occupied(e) => e.into_mut(),
                            Entry::Vacant(e) => {
                                let mut id = None;
                                id_path.push(e.key());
                                if let Ok(f) = std::fs::File::open(&id_path) {
                                    if let Ok(id_) =
                                        serde_json::from_reader::<_, super::Identity>(f)
                                    {
                                        id = Some(id_)
                                    }
                                }
                                id_path.pop();
                                if let Some(id) = id {
                                    e.insert(id.login)
                                } else {
                                    let k = e.key().to_string();
                                    e.insert(k)
                                }
                            }
                        }
                    } else {
                        auth.0.get("name").unwrap()
                    };
                    if is_first {
                        is_first = false;
                        write!(stdout, "{}", auth)?;
                    } else {
                        write!(stdout, ", {}", auth)?;
                    }
                }
                writeln!(stdout)?;
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
