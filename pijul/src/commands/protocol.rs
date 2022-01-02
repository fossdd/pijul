use std::collections::{HashMap, HashSet};
use std::io::BufWriter;
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;

use crate::repository::Repository;
use anyhow::bail;
use byteorder::{BigEndian, WriteBytesExt};
use clap::Parser;
use lazy_static::lazy_static;
use libpijul::*;
use log::{debug, error};
use regex::Regex;

/// This command is not meant to be run by the user,
/// instead it is called over SSH
#[derive(Parser, Debug)]
pub struct Protocol {
    /// Set the repository where this command should run. Defaults to the first ancestor of the current directory that contains a `.pijul` directory.
    #[clap(long = "repository")]
    repo_path: Option<PathBuf>,
    /// Use this protocol version
    #[clap(long = "version")]
    version: usize,
}

lazy_static! {
    static ref STATE: Regex = Regex::new(r#"state\s+(\S+)(\s+([0-9]+)?)\s+"#).unwrap();
    static ref ID: Regex = Regex::new(r#"id\s+(\S+)\s+"#).unwrap();
    static ref IDENTITIES: Regex = Regex::new(r#"identities(\s+([0-9]+))?\s+"#).unwrap();
    static ref CHANGELIST: Regex = Regex::new(r#"changelist\s+(\S+)\s+([0-9]+)(.*)\s+"#).unwrap();
    static ref CHANGELIST_PATHS: Regex = Regex::new(r#""(((\\")|[^"])+)""#).unwrap();
    static ref CHANGE: Regex = Regex::new(r#"((change)|(partial))\s+([^ ]*)\s+"#).unwrap();
    static ref TAG: Regex = Regex::new(r#"^tag\s+(\S+)\s+"#).unwrap();
    static ref TAGUP: Regex = Regex::new(r#"^tagup\s+(\S+)\s+(\S+)\s+([0-9]+)\s+"#).unwrap();
    static ref APPLY: Regex = Regex::new(r#"apply\s+(\S+)\s+([^ ]*) ([0-9]+)\s+"#).unwrap();
    static ref CHANNEL: Regex = Regex::new(r#"channel\s+(\S+)\s+"#).unwrap();
    static ref ARCHIVE: Regex =
        Regex::new(r#"archive\s+(\S+)\s*(( ([^:]+))*)( :(.*))?\n"#).unwrap();
}

fn load_channel<T: MutTxnTExt>(txn: &T, name: &str) -> Result<ChannelRef<T>, anyhow::Error> {
    if let Some(c) = txn.load_channel(name)? {
        Ok(c)
    } else {
        bail!("No such channel: {:?}", name)
    }
}

const PARTIAL_CHANGE_SIZE: u64 = 1 << 20;

impl Protocol {
    pub fn run(self) -> Result<(), anyhow::Error> {
        let mut repo = Repository::find_root(self.repo_path)?;
        let txn = repo.pristine.arc_txn_begin()?;
        let mut ws = libpijul::ApplyWorkspace::new();
        let mut buf = String::new();
        let mut buf2 = vec![0; 4096 * 10];
        let s = std::io::stdin();
        let mut s = s.lock();
        let o = std::io::stdout();
        let mut o = BufWriter::new(o.lock());
        let mut applied = HashMap::new();

        debug!("reading");
        while s.read_line(&mut buf)? > 0 {
            debug!("{:?}", buf);
            if let Some(cap) = ID.captures(&buf) {
                let channel = load_channel(&*txn.read(), &cap[1])?;
                let c = channel.read();
                writeln!(o, "{}", c.id)?;
                o.flush()?;
            } else if let Some(cap) = STATE.captures(&buf) {
                let channel = load_channel(&*txn.read(), &cap[1])?;
                let init = if let Some(u) = cap.get(3) {
                    u.as_str().parse().ok()
                } else {
                    None
                };
                if let Some(pos) = init {
                    let txn = txn.read();
                    for x in txn.log(&*channel.read(), pos)? {
                        let (n, (_, m)) = x?;
                        match n.cmp(&pos) {
                            std::cmp::Ordering::Less => continue,
                            std::cmp::Ordering::Greater => {
                                writeln!(o, "-")?;
                                break;
                            }
                            std::cmp::Ordering::Equal => {
                                let m: libpijul::Merkle = m.into();
                                let m2 = if let Some(x) = txn
                                    .rev_iter_tags(txn.tags(&*channel.read()), Some(n))?
                                    .next()
                                {
                                    x?.1.b.into()
                                } else {
                                    Merkle::zero()
                                };
                                writeln!(o, "{} {} {}", n, m.to_base32(), m2.to_base32())?;
                                break;
                            }
                        }
                    }
                } else {
                    let txn = txn.read();
                    if let Some(x) = txn.reverse_log(&*channel.read(), None)?.next() {
                        let (n, (_, m)) = x?;
                        let m: Merkle = m.into();
                        let m2 = if let Some(x) = txn
                            .rev_iter_tags(txn.tags(&*channel.read()), Some(n))?
                            .next()
                        {
                            x?.1.b.into()
                        } else {
                            Merkle::zero()
                        };
                        writeln!(o, "{} {} {}", n, m.to_base32(), m2.to_base32())?
                    } else {
                        writeln!(o, "-")?;
                    }
                }
                o.flush()?;
            } else if let Some(cap) = CHANGELIST.captures(&buf) {
                let channel = load_channel(&*txn.read(), &cap[1])?;
                let from: u64 = cap[2].parse().unwrap();
                let mut paths = HashSet::new();
                debug!("cap[3] = {:?}", &cap[3]);
                let txn = txn.read();
                for r in CHANGELIST_PATHS.captures_iter(&cap[3]) {
                    let s: String = r[1].replace("\\\"", "\"");
                    if let Ok((p, ambiguous)) = txn.follow_oldest_path(&repo.changes, &channel, &s)
                    {
                        if ambiguous {
                            bail!("Ambiguous path")
                        }
                        let h: libpijul::Hash = txn.get_external(&p.change)?.unwrap().into();
                        writeln!(o, "{}.{}", h.to_base32(), p.pos.0)?;
                        paths.insert(p);
                        paths.extend(
                            libpijul::fs::iter_graph_descendants(&*txn, &channel.read(), p)?
                                .map(|x| x.unwrap()),
                        );
                    } else {
                        debug!("protocol line: {:?}", buf);
                        bail!("Protocol error")
                    }
                }
                debug!("paths = {:?}", paths);
                let tags: Vec<u64> = txn
                    .iter_tags(txn.tags(&*channel.read()), from)?
                    .map(|k| (*k.unwrap().0).into())
                    .collect();
                let mut tagsi = 0;
                for x in txn.log(&*channel.read(), from)? {
                    let (n, (h, m)) = x?;
                    let h_int = txn.get_internal(h)?.unwrap();
                    if paths.is_empty()
                        || paths.iter().any(|x| {
                            x.change == *h_int
                                || txn.get_touched_files(x, Some(h_int)).unwrap().is_some()
                        })
                    {
                        let h: Hash = h.into();
                        let m: Merkle = m.into();
                        if paths.is_empty() && tags.get(tagsi) == Some(&n) {
                            writeln!(o, "{}.{}.{}.", n, h.to_base32(), m.to_base32())?;
                            tagsi += 1;
                        } else {
                            writeln!(o, "{}.{}.{}", n, h.to_base32(), m.to_base32())?;
                        }
                    }
                }
                writeln!(o)?;
                o.flush()?;
            } else if let Some(cap) = TAG.captures(&buf) {
                if let Some(state) = Merkle::from_base32(cap[1].as_bytes()) {
                    let mut tag_path = repo.changes_dir.clone();
                    libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &state);
                    let mut tag = libpijul::tag::OpenTagFile::open(&tag_path, &state)?;
                    let mut buf = Vec::new();
                    tag.short(&mut buf)?;
                    o.write_u64::<BigEndian>(buf.len() as u64)?;
                    o.write_all(&buf)?;
                    o.flush()?;
                }
            } else if let Some(cap) = TAGUP.captures(&buf) {
                if let Some(state) = Merkle::from_base32(cap[1].as_bytes()) {
                    let channel = load_channel(&*txn.read(), &cap[2])?;
                    let m = libpijul::pristine::current_state(&*txn.read(), &*channel.read())?;
                    if m == state {
                        let mut tag_path = repo.changes_dir.clone();
                        libpijul::changestore::filesystem::push_tag_filename(&mut tag_path, &m);
                        if std::fs::metadata(&tag_path).is_ok() {
                            bail!("Tag for state {} already exists", m.to_base32());
                        }

                        let last_t = if let Some(n) =
                            txn.read().reverse_log(&*channel.read(), None)?.next()
                        {
                            n?.0.into()
                        } else {
                            bail!("Channel {} is empty", &cap[2]);
                        };
                        if txn.read().is_tagged(&channel.read().tags, last_t)? {
                            bail!("Current state is already tagged")
                        }

                        let size: usize = cap[3].parse().unwrap();
                        let mut buf = vec![0; size];
                        s.read_exact(&mut buf)?;

                        let header = libpijul::tag::read_short(std::io::Cursor::new(&buf[..]), &m)?;

                        let temp_path = tag_path.with_extension("tmp");

                        std::fs::create_dir_all(temp_path.parent().unwrap())?;
                        let mut w = std::fs::File::create(&temp_path)?;
                        libpijul::tag::from_channel(&*txn.read(), &cap[2], &header, &mut w)?;

                        std::fs::rename(&temp_path, &tag_path)?;
                        txn.write()
                            .put_tags(&mut channel.write().tags, last_t.into(), &m)?;
                    } else {
                        bail!("Wrong state, cannot tag")
                    }
                }
            } else if let Some(cap) = CHANGE.captures(&buf) {
                let h_ = &cap[4];
                let h = if let Some(h) = Hash::from_base32(h_.as_bytes()) {
                    h
                } else {
                    debug!("protocol error: {:?}", buf);
                    bail!("Protocol error")
                };
                libpijul::changestore::filesystem::push_filename(&mut repo.changes_dir, &h);
                debug!("repo = {:?}", repo.changes_dir);
                let mut f = std::fs::File::open(&repo.changes_dir)?;
                let size = std::fs::metadata(&repo.changes_dir)?.len();
                let size = if &cap[1] == "change" || size <= PARTIAL_CHANGE_SIZE {
                    size
                } else {
                    libpijul::change::Change::size_no_contents(&mut f)?
                };
                o.write_u64::<BigEndian>(size)?;
                let mut size = size as usize;
                while size > 0 {
                    if size < buf2.len() {
                        buf2.truncate(size as usize);
                    }
                    let n = f.read(&mut buf2[..])?;
                    if n == 0 {
                        break;
                    }
                    size -= n;
                    o.write_all(&buf2[..n])?;
                }
                o.flush()?;
                libpijul::changestore::filesystem::pop_filename(&mut repo.changes_dir);
            } else if let Some(cap) = APPLY.captures(&buf) {
                let h = if let Some(h) = Hash::from_base32(cap[2].as_bytes()) {
                    h
                } else {
                    debug!("protocol error {:?}", buf);
                    bail!("Protocol error");
                };
                let mut path = repo.changes_dir.clone();
                libpijul::changestore::filesystem::push_filename(&mut path, &h);
                std::fs::create_dir_all(path.parent().unwrap())?;
                let size: usize = cap[3].parse().unwrap();
                buf2.resize(size, 0);
                s.read_exact(&mut buf2)?;
                std::fs::write(&path, &buf2)?;
                libpijul::change::Change::deserialize(&path.to_string_lossy(), Some(&h))?;
                let channel = load_channel(&*txn.read(), &cap[1])?;
                {
                    let mut channel_ = channel.write();
                    txn.write()
                        .apply_change_ws(&repo.changes, &mut channel_, &h, &mut ws)?;
                }
                applied.insert(cap[1].to_string(), channel);
            } else if let Some(cap) = ARCHIVE.captures(&buf) {
                let mut w = Vec::new();
                let mut tarball = libpijul::output::Tarball::new(
                    &mut w,
                    cap.get(6).map(|x| x.as_str().to_string()),
                    0,
                );
                let channel = load_channel(&*txn.read(), &cap[1])?;
                let conflicts = if let Some(caps) = cap.get(2) {
                    debug!("caps = {:?}", caps.as_str());
                    let mut hashes = caps.as_str().split(' ').filter(|x| !x.is_empty());
                    let state: libpijul::Merkle = hashes.next().unwrap().parse().unwrap();
                    let extra: Vec<libpijul::Hash> = hashes.map(|x| x.parse().unwrap()).collect();
                    debug!("state = {:?}, extra = {:?}", state, extra);
                    if txn.read().current_state(&*channel.read())? == state && extra.is_empty() {
                        txn.read().archive(&repo.changes, &channel, &mut tarball)?
                    } else {
                        use rand::Rng;
                        let fork_name: String = rand::thread_rng()
                            .sample_iter(&rand::distributions::Alphanumeric)
                            .take(30)
                            .map(|x| x as char)
                            .collect();
                        let mut txn = txn.write();
                        let mut fork = txn.fork(&channel, &fork_name)?;
                        let conflicts = txn.archive_with_state(
                            &repo.changes,
                            &mut fork,
                            &state,
                            &extra,
                            &mut tarball,
                            0,
                        )?;
                        txn.drop_channel(&fork_name)?;
                        conflicts
                    }
                } else {
                    txn.write().archive(&repo.changes, &channel, &mut tarball)?
                };
                std::mem::drop(tarball);
                let mut o = std::io::stdout();
                o.write_u64::<BigEndian>(w.len() as u64)?;
                o.write_u64::<BigEndian>(conflicts.len() as u64)?;
                o.write_all(&w)?;
                o.flush()?;
            } else if let Some(cap) = IDENTITIES.captures(&buf) {
                let last_touched: u64 = if let Some(last) = cap.get(2) {
                    last.as_str().parse().unwrap()
                } else {
                    0
                };
                let mut id_dir = repo.path.clone();
                id_dir.push(DOT_DIR);
                id_dir.push("identities");
                let r = if let Ok(r) = std::fs::read_dir(&id_dir) {
                    r
                } else {
                    continue;
                };
                for id in r {
                    output_id(id, last_touched, &mut o).unwrap_or(());
                }
                writeln!(o)?;
                o.flush()?;
            } else {
                error!("unmatched")
            }
            buf.clear();
        }
        let applied_nonempty = !applied.is_empty();
        for (_, channel) in applied {
            libpijul::output::output_repository_no_pending(
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
        }
        if applied_nonempty {
            txn.commit()?;
        }
        Ok(())
    }
}

fn get_public_key() -> Result<libpijul::key::PublicKey, anyhow::Error> {
    if let Some(mut dir) = crate::config::global_config_dir() {
        dir.push("publickey.json");
        if let Ok(mut pkf) = std::fs::File::open(&dir) {
            if let Ok(pkf) = serde_json::from_reader(&mut pkf) {
                return Ok(pkf);
            }
        }
    }
    bail!("No public key found")
}

fn output_id<W: Write>(
    id: Result<std::fs::DirEntry, std::io::Error>,
    last_touched: u64,
    mut o: W,
) -> Result<(), anyhow::Error> {
    let id = id?;
    let m = id.metadata()?;
    let p = id.path();
    debug!("{:?}", p);
    let mod_ts = m
        .modified()?
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if mod_ts >= last_touched {
        let mut done = HashSet::new();
        if p.file_name() == Some("publickey.json".as_ref()) {
            let public_key: libpijul::key::PublicKey = if let Ok(pk) = get_public_key() {
                pk
            } else {
                return Ok(());
            };
            if !done.insert(public_key.key.clone()) {
                return Ok(());
            }
            if let Ok((config, last_modified)) = crate::config::Global::load() {
                serde_json::to_writer(
                    &mut o,
                    &crate::Identity {
                        public_key,
                        email: config.author.email,
                        name: config.author.full_name,
                        login: config.author.name,
                        origin: String::new(),
                        last_modified,
                    },
                )
                .unwrap();
                writeln!(o)?;
            } else {
                debug!("no global config");
            }
        } else {
            let mut idf = if let Ok(f) = std::fs::File::open(&p) {
                f
            } else {
                return Ok(());
            };
            let id: Result<crate::Identity, _> = serde_json::from_reader(&mut idf);
            if let Ok(id) = id {
                if !done.insert(id.public_key.key.clone()) {
                    return Ok(());
                }
                serde_json::to_writer(&mut o, &id).unwrap();
                writeln!(o)?;
            }
        }
    }
    Ok(())
}
