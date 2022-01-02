use std::io::Write;
use std::path::PathBuf;

use crate::commands::record::timestamp_validator;
use crate::repository::Repository;
use anyhow::bail;
use clap::Parser;
use libpijul::change::ChangeHeader;
use libpijul::{ArcTxn, Base32, ChannelMutTxnT, ChannelTxnT, MutTxnT, TxnT, TxnTExt};
use log::*;

#[derive(Parser, Debug)]
pub struct Tag {
    #[clap(subcommand)]
    subcmd: Option<SubCommand>,
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    #[clap(long = "channel")]
    channel: Option<String>,
}

#[derive(Parser, Debug)]
pub enum SubCommand {
    /// Create a tag, which are compressed channels. Note that tags
    /// are not independent from the changes they contain.
    #[clap(name = "create")]
    Create {
        /// Set the repository where this command should run. Defaults to
        /// the first ancestor of the current directory that contains a
        /// `.pijul` directory.
        #[clap(long = "repository")]
        repo_path: Option<PathBuf>,
        #[clap(short = 'm', long = "message")]
        message: Option<String>,
        /// Set the author field
        #[clap(long = "author")]
        author: Option<String>,
        /// Tag the current state of this channel instead of the
        /// current channel.
        #[clap(long = "channel")]
        channel: Option<String>,
        #[clap(long = "timestamp", validator = timestamp_validator)]
        timestamp: Option<i64>,
    },
    /// Restore a tag into a new channel.
    #[clap(name = "checkout")]
    Checkout {
        /// Set the repository where this command should run. Defaults to
        /// the first ancestor of the current directory that contains a
        /// `.pijul` directory.
        #[clap(long = "repository")]
        repo_path: Option<PathBuf>,
        tag: String,
        /// Optional new channel name. If not given, the base32
        /// representation of the tag hash is used.
        #[clap(long = "to-channel")]
        to_channel: Option<String>,
    },
    /// Restore a tag into a new channel.
    #[clap(name = "reset")]
    Reset {
        /// Set the repository where this command should run. Defaults to
        /// the first ancestor of the current directory that contains a
        /// `.pijul` directory.
        #[clap(long = "repository")]
        repo_path: Option<PathBuf>,
        tag: String,
    },
    /// Delete a tag from a channel. If the same state isn't tagged in
    /// other channels, delete the tag file.
    #[clap(name = "delete")]
    Delete {
        /// Set the repository where this command should run. Defaults to
        /// the first ancestor of the current directory that contains a
        /// `.pijul` directory.
        #[clap(long = "repository")]
        repo_path: Option<PathBuf>,
        /// Delete the tag in this channel instead of the current channel
        #[clap(long = "channel")]
        channel: Option<String>,
        tag: String,
    },
}

impl Tag {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let mut stdout = std::io::stdout();
        match self.subcmd {
            Some(SubCommand::Create {
                repo_path,
                message,
                author,
                channel,
                timestamp,
            }) => {
                let mut repo = Repository::find_root(repo_path)?;
                let txn = repo.pristine.arc_txn_begin()?;
                let channel_name = if let Some(c) = channel {
                    c
                } else {
                    txn.read()
                        .current_channel()
                        .unwrap_or(crate::DEFAULT_CHANNEL)
                        .to_string()
                };
                debug!("channel_name = {:?}", channel_name);
                try_record(&mut repo, txn.clone(), &channel_name)?;
                let channel = txn.read().load_channel(&channel_name)?.unwrap();
                let last_t = if let Some(n) = txn.read().reverse_log(&*channel.read(), None)?.next()
                {
                    n?.0.into()
                } else {
                    bail!("Channel {} is empty", channel_name);
                };
                log::debug!("last_t = {:?}", last_t);
                if txn.read().is_tagged(&channel.read().tags, last_t)? {
                    bail!("Current state is already tagged")
                }
                let mut tag_path = repo.changes_dir.clone();
                std::fs::create_dir_all(&tag_path)?;

                let mut temp_path = tag_path.clone();
                temp_path.push("tmp");

                let mut w = std::fs::File::create(&temp_path)?;
                let header = header(author.as_deref(), message, timestamp)?;
                let h: libpijul::Merkle =
                    libpijul::tag::from_channel(&*txn.read(), &channel_name, &header, &mut w)?;
                libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &h);
                std::fs::create_dir_all(tag_path.parent().unwrap())?;
                std::fs::rename(&temp_path, &tag_path)?;

                txn.write()
                    .put_tags(&mut channel.write().tags, last_t.into(), &h)?;
                txn.commit()?;
                writeln!(stdout, "{}", h.to_base32())?;
            }
            Some(SubCommand::Checkout {
                repo_path,
                mut tag,
                to_channel,
            }) => {
                let repo = Repository::find_root(repo_path)?;
                let mut tag_path = repo.changes_dir.clone();
                let h = if let Some(h) = libpijul::Merkle::from_base32(tag.as_bytes()) {
                    libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &h);
                    h
                } else {
                    super::find_hash(&mut tag_path, &tag)?
                };

                let mut txn = repo.pristine.mut_txn_begin()?;
                tag = h.to_base32();
                let channel_name = if let Some(ref channel) = to_channel {
                    channel.as_str()
                } else {
                    tag.as_str()
                };
                if txn.load_channel(channel_name)?.is_some() {
                    bail!("Channel {:?} already exists", channel_name)
                }
                let f = libpijul::tag::OpenTagFile::open(&tag_path, &h)?;
                libpijul::tag::restore_channel(f, &mut txn, &channel_name)?;
                txn.commit()?;
                writeln!(stdout, "Tag {} restored as channel {}", tag, channel_name)?;
            }
            Some(SubCommand::Reset { repo_path, tag }) => {
                let repo = Repository::find_root(repo_path)?;
                let mut tag_path = repo.changes_dir.clone();
                let h = if let Some(h) = libpijul::Merkle::from_base32(tag.as_bytes()) {
                    libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &h);
                    h
                } else {
                    super::find_hash(&mut tag_path, &tag)?
                };

                let tag = libpijul::tag::txn::TagTxn::new(&tag_path, &h)?;
                let txn = libpijul::tag::txn::WithTag {
                    tag,
                    txn: repo.pristine.mut_txn_begin()?,
                };
                let channel = txn.channel();
                let txn = ArcTxn::new(txn);

                libpijul::output::output_repository_no_pending_(
                    &repo.working_copy,
                    &repo.changes,
                    &txn,
                    &channel,
                    "",
                    true,
                    None,
                    num_cpus::get(),
                    0,
                )?;
                if let Ok(txn) = std::sync::Arc::try_unwrap(txn.0) {
                    txn.into_inner().txn.commit()?
                }
                writeln!(stdout, "Reset to tag {}", h.to_base32())?;
            }
            Some(SubCommand::Delete {
                repo_path,
                channel,
                tag,
            }) => {
                let repo = Repository::find_root(repo_path)?;
                let mut tag_path = repo.changes_dir.clone();
                let h = if let Some(h) = libpijul::Merkle::from_base32(tag.as_bytes()) {
                    libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &h);
                    h
                } else {
                    super::find_hash(&mut tag_path, &tag)?
                };

                let mut txn = repo.pristine.mut_txn_begin()?;
                let channel_name = channel.unwrap_or_else(|| {
                    txn.current_channel()
                        .unwrap_or(crate::DEFAULT_CHANNEL)
                        .to_string()
                });
                let channel = if let Some(c) = txn.load_channel(&channel_name)? {
                    c
                } else {
                    bail!("Channel {:?} not found", channel_name)
                };
                {
                    let mut ch = channel.write();
                    if let Some(n) = txn.channel_has_state(txn.states(&*ch), &h.into())? {
                        let tags = txn.tags_mut(&mut *ch);
                        txn.del_tags(tags, n.into())?;
                    }
                }
                txn.commit()?;
                writeln!(stdout, "Deleted tag {}", h.to_base32())?;
            }
            None => {
                let repo = Repository::find_root(self.repo_path)?;
                let txn = repo.pristine.txn_begin()?;
                let channel_name = self.channel.unwrap_or_else(|| {
                    txn.current_channel()
                        .unwrap_or(crate::DEFAULT_CHANNEL)
                        .to_string()
                });
                let channel = if let Some(c) = txn.load_channel(&channel_name)? {
                    c
                } else {
                    bail!("Channel {:?} not found", channel_name)
                };
                let mut tag_path = repo.changes_dir.clone();
                super::pager();
                for t in txn.rev_iter_tags(txn.tags(&*channel.read()), None)? {
                    let (t, _) = t?;
                    let (_, m) = txn.get_changes(&channel, (*t).into())?.unwrap();
                    libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &m);
                    debug!("tag path {:?}", tag_path);
                    let mut f = libpijul::tag::OpenTagFile::open(&tag_path, &m)?;
                    let header = f.header()?;
                    writeln!(stdout, "State {}", m.to_base32())?;
                    writeln!(stdout, "Author: {:?}", header.authors)?;
                    writeln!(stdout, "Date: {}", header.timestamp)?;
                    writeln!(stdout, "\n    {}\n", header.message)?;
                    libpijul::changestore::filesystem::pop_filename(&mut tag_path);
                }
            }
        }
        Ok(())
    }
}

fn header(
    author: Option<&str>,
    message: Option<String>,
    timestamp: Option<i64>,
) -> Result<ChangeHeader, anyhow::Error> {
    let mut authors = Vec::new();
    use libpijul::change::Author;
    let mut b = std::collections::BTreeMap::new();
    if let Some(ref a) = author {
        b.insert("name".to_string(), a.to_string());
    } else if let Some(mut dir) = crate::config::global_config_dir() {
        dir.push("publickey.json");
        if let Ok(key) = std::fs::File::open(&dir) {
            let k: libpijul::key::PublicKey = serde_json::from_reader(key).unwrap();
            b.insert("key".to_string(), k.key);
        } else {
            bail!("No identity configured yet. Please use `pijul key` to create one")
        }
    }
    authors.push(Author(b));
    let header = ChangeHeader {
        message: message.clone().unwrap_or_else(String::new),
        authors,
        description: None,
        timestamp: if let Some(t) = timestamp {
            chrono::DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(t, 0), chrono::Utc)
        } else {
            chrono::Utc::now()
        },
    };
    if header.message.is_empty() {
        let toml = toml::to_string_pretty(&header)?;
        loop {
            let bytes = edit::edit_bytes(toml.as_bytes())?;
            if let Ok(header) = toml::from_slice(&bytes) {
                return Ok(header);
            }
        }
    } else {
        Ok(header)
    }
}

fn try_record<T: ChannelMutTxnT + TxnT + Send + Sync + 'static>(
    repo: &mut Repository,
    txn: ArcTxn<T>,
    channel: &str,
) -> Result<(), anyhow::Error> {
    let channel = if let Some(channel) = txn.read().load_channel(channel)? {
        channel
    } else {
        bail!("Channel not found: {}", channel)
    };
    let mut state = libpijul::RecordBuilder::new();
    state.record(
        txn,
        libpijul::Algorithm::default(),
        false,
        &libpijul::DEFAULT_SEPARATOR,
        channel,
        &repo.working_copy,
        &repo.changes,
        "",
        num_cpus::get(),
    )?;
    let rec = state.finish();
    if !rec.actions.is_empty() {
        bail!("Cannot change channel, as there are unrecorded changes.")
    }
    Ok(())
}
