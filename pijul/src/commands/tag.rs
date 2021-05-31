use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::commands::record::timestamp_validator;
use crate::repository::Repository;
use anyhow::bail;
use clap::Clap;
use libpijul::change::ChangeHeader;
use libpijul::{Base32, ChannelMutTxnT, ChannelTxnT, MutTxnT, TxnT, TxnTExt};

#[derive(Clap, Debug)]
pub struct Tag {
    /// Set the repository where this command should run. Defaults to
    /// the first ancestor of the current directory that contains a
    /// `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    #[clap(subcommand)]
    subcmd: Option<SubCommand>,
}

#[derive(Clap, Debug)]
pub enum SubCommand {
    /// Create a tag.
    #[clap(name = "create")]
    Create {
        #[clap(short = 'm', long = "message")]
        message: Option<String>,
        /// Set the author field
        #[clap(long = "author")]
        author: Option<String>,
        /// Record the change in this channel instead of the current channel
        #[clap(long = "channel")]
        channel: Option<String>,
        #[clap(long = "timestamp", validator = timestamp_validator)]
        timestamp: Option<i64>,
    },
    /// Restore a tag into a new channel.
    #[clap(name = "checkout")]
    Checkout {
        tag: String,
        /// Optional new channel name. If not given, the base32
        /// representation of the tag hash is used.
        #[clap(long = "to-channel")]
        to_channel: Option<String>,
    },
}

impl Tag {
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut stdout = std::io::stdout();
        let mut repo = Repository::find_root(self.repo_path).await?;
        match self.subcmd {
            Some(SubCommand::Create {
                message,
                author,
                channel,
                timestamp,
            }) => {
                let channel_name = repo
                    .config
                    .get_current_channel(channel.as_deref())
                    .0
                    .to_string();
                try_record(&mut repo, &channel_name)?;
                let mut txn = repo.pristine.mut_txn_begin()?;
                let channel = txn.load_channel(&channel_name)?.unwrap();
                let last_t = if let Some(n) = txn.reverse_log(&*channel.read()?, None)?.next() {
                    n?.0.into()
                } else {
                    bail!("Channel {} is empty", channel_name);
                };
                log::debug!("last_t = {:?}", last_t);
                if txn.get_tags(&channel.read()?.tags, &last_t)?.is_some() {
                    bail!("Current state is already tagged")
                }
                let mut tag_path = repo.path.join(libpijul::DOT_DIR);
                tag_path.push("tags");
                std::fs::create_dir_all(&tag_path)?;

                let mut temp_path = tag_path.clone();
                temp_path.push("tag");

                let mut w = std::fs::File::create(&temp_path)?;
                let header = header(author.as_deref(), message, timestamp)?;
                let h = libpijul::tag::from_channel(&txn, &channel_name, &header, &mut w)?;
                libpijul::changestore::filesystem::push_filename(&mut tag_path, &h);
                std::fs::create_dir_all(tag_path.parent().unwrap())?;
                std::fs::rename(&temp_path, &tag_path)?;

                txn.put_tags(&mut *channel.write()?, last_t.into(), &h)?;
                txn.commit()?;
                writeln!(stdout, "{}", h.to_base32())?;
            }
            Some(SubCommand::Checkout { tag, to_channel }) => {
                let h = if let Some(h) = libpijul::Hash::from_base32(tag.as_bytes()) {
                    h
                } else {
                    bail!("Invalid tag {:?}", tag)
                };
                let channel_name = if let Some(ref channel) = to_channel {
                    channel.as_str()
                } else {
                    tag.as_str()
                };
                let mut txn = repo.pristine.mut_txn_begin()?;
                if txn.load_channel(channel_name)?.is_some() {
                    bail!("Channel {:?} already exists", channel_name)
                }
                let mut tag_path = repo.path.join(libpijul::DOT_DIR);
                tag_path.push("tags");
                libpijul::changestore::filesystem::push_filename(&mut tag_path, &h);
                let f = libpijul::tag::OpenTagFile::open(&tag_path)?;
                libpijul::tag::restore_channel(f, &mut txn, &channel_name)?;
                txn.commit()?;
                writeln!(stdout, "Tag {} restored as channel {}", tag, channel_name)?;
            }
            None => {
                let channel_name = repo.config.get_current_channel(None).0.to_string();
                let txn = repo.pristine.txn_begin()?;
                let channel = if let Some(c) = txn.load_channel(&channel_name)? {
                    c
                } else {
                    bail!("Channel {:?} not found", channel_name)
                };
                let mut tag_path = repo.path.join(libpijul::DOT_DIR);
                tag_path.push("tags");
                super::pager();
                for t in txn.rev_iter_tags(txn.tags(&*channel.read()?), None)? {
                    let (_, h) = t?;
                    let h: libpijul::Hash = h.into();
                    libpijul::changestore::filesystem::push_filename(&mut tag_path, &h);
                    let mut f = libpijul::tag::OpenTagFile::open(&tag_path)?;
                    let header = f.header()?;
                    writeln!(stdout, "Tag {}", h.to_base32())?;
                    writeln!(stdout, "Author: {:?}", header.authors)?;
                    writeln!(stdout, "Date: {}", header.timestamp)?;
                    writeln!(stdout, "State: {}", f.state().to_base32())?;
                    writeln!(stdout, "\n    {}\n", header.message)?;
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
    let authors = if let Some(a) = author {
        vec![libpijul::change::Author {
            name: a.to_string(),
            full_name: None,
            email: None,
        }]
    } else if let Ok(global) = crate::config::Global::load() {
        vec![global.author]
    } else {
        Vec::new()
    };
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
    let toml = toml::to_string_pretty(&header)?;
    loop {
        let bytes = edit::edit_bytes(toml.as_bytes())?;
        if let Ok(header) = toml::from_slice(&bytes) {
            return Ok(header);
        }
    }
}

fn try_record(repo: &mut Repository, channel: &str) -> Result<(), anyhow::Error> {
    let txn = repo.pristine.mut_txn_begin()?;
    if let Some(channel) = txn.load_channel(channel)? {
        let mut state = libpijul::RecordBuilder::new();
        state.record(
            Arc::new(RwLock::new(txn)),
            libpijul::Algorithm::default(),
            channel,
            repo.working_copy.clone(),
            &repo.changes,
            "",
            num_cpus::get(),
        )?;
        let rec = state.finish();
        if !rec.actions.is_empty() {
            bail!("Cannot change channel, as there are unrecorded changes.")
        }
    } else {
        bail!("Channel not found: {}", channel)
    }
    Ok(())
}
