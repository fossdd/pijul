use std::collections::{HashMap, HashSet};
use std::io::BufWriter;
use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::repository::Repository;
use anyhow::bail;
use byteorder::{BigEndian, WriteBytesExt};
use clap::Clap;
use lazy_static::lazy_static;
use libpijul::*;
use log::{debug, error};
use regex::Regex;

/// This command is not meant to be run by the user,
/// instead it is called over SSH
#[derive(Clap, Debug)]
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
    static ref CHANGELIST: Regex = Regex::new(r#"changelist\s+(\S+)\s+([0-9]+)(.*)\s+"#).unwrap();
    static ref CHANGELIST_PATHS: Regex = Regex::new(r#""(((\\")|[^"])+)""#).unwrap();
    static ref CHANGE: Regex = Regex::new(r#"((change)|(partial))\s+([^ ]*)\s+"#).unwrap();
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
    pub async fn run(self) -> Result<(), anyhow::Error> {
        let mut repo = Repository::find_root(self.repo_path).await?;
        let mut txn = repo.pristine.mut_txn_begin()?;
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
            if let Some(cap) = STATE.captures(&buf) {
                let channel = load_channel(&txn, &cap[1])?;
                let init = if let Some(u) = cap.get(3) {
                    u.as_str().parse().ok()
                } else {
                    None
                };
                if let Some(pos) = init {
                    for x in txn.log(&*channel.read()?, pos)? {
                        let (n, (_, m)) = x?;
                        match n.cmp(&pos) {
                            std::cmp::Ordering::Less => continue,
                            std::cmp::Ordering::Greater => {
                                writeln!(o, "-")?;
                                break;
                            }
                            std::cmp::Ordering::Equal => {
                                let m: libpijul::Merkle = m.into();
                                writeln!(o, "{} {}", n, m.to_base32())?;
                                break;
                            }
                        }
                    }
                } else if let Some(x) = txn.reverse_log(&*channel.read()?, None)?.next() {
                    let (n, (_, m)) = x?;
                    let m: Merkle = m.into();
                    writeln!(o, "{} {}", n, m.to_base32())?
                } else {
                    writeln!(o, "-")?;
                }
                o.flush()?;
            } else if let Some(cap) = CHANGELIST.captures(&buf) {
                let channel = load_channel(&txn, &cap[1])?;
                let from: u64 = cap[2].parse().unwrap();
                let mut paths = HashSet::new();
                debug!("cap[3] = {:?}", &cap[3]);
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
                            libpijul::fs::iter_graph_descendants(&txn, &channel.read()?.graph, p)?
                                .map(|x| x.unwrap()),
                        );
                    } else {
                        debug!("protocol line: {:?}", buf);
                        bail!("Protocol error")
                    }
                }
                debug!("paths = {:?}", paths);
                for x in txn.log(&*channel.read()?, from)? {
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
                        writeln!(o, "{}.{}.{}", n, h.to_base32(), m.to_base32())?
                    }
                }
                writeln!(o)?;
                o.flush()?;
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
                let channel = load_channel(&txn, &cap[1])?;
                {
                    let mut channel_ = channel.write().unwrap();
                    txn.apply_change_ws(&repo.changes, &mut channel_, &h, &mut ws)?;
                }
                applied.insert(cap[1].to_string(), channel);
            } else if let Some(cap) = ARCHIVE.captures(&buf) {
                let mut w = Vec::new();
                let mut tarball = libpijul::output::Tarball::new(
                    &mut w,
                    cap.get(6).map(|x| x.as_str().to_string()),
                    0,
                );
                let channel = load_channel(&txn, &cap[1])?;
                let conflicts = if let Some(caps) = cap.get(2) {
                    debug!("caps = {:?}", caps.as_str());
                    let mut hashes = caps.as_str().split(' ').filter(|x| !x.is_empty());
                    let state: libpijul::Merkle = hashes.next().unwrap().parse().unwrap();
                    let extra: Vec<libpijul::Hash> = hashes.map(|x| x.parse().unwrap()).collect();
                    debug!("state = {:?}, extra = {:?}", state, extra);
                    if txn.current_state(&*channel.read()?)? == state && extra.is_empty() {
                        txn.archive(&repo.changes, &channel, &mut tarball)?
                    } else {
                        use rand::Rng;
                        let fork_name: String = rand::thread_rng()
                            .sample_iter(&rand::distributions::Alphanumeric)
                            .take(30)
                            .map(|x| x as char)
                            .collect();
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
                    txn.archive(&repo.changes, &channel, &mut tarball)?
                };
                std::mem::drop(tarball);
                let mut o = std::io::stdout();
                o.write_u64::<BigEndian>(w.len() as u64)?;
                o.write_u64::<BigEndian>(conflicts.len() as u64)?;
                o.write_all(&w)?;
                o.flush()?;
            } else {
                error!("unmatched")
            }
            buf.clear();
        }
        let applied_nonempty = !applied.is_empty();
        let txn = Arc::new(RwLock::new(txn));
        for (_, channel) in applied {
            libpijul::output::output_repository_no_pending(
                repo.working_copy.clone(),
                &repo.changes,
                txn.clone(),
                channel.clone(),
                "",
                true,
                None,
                num_cpus::get(),
                0,
            )?;
        }
        if applied_nonempty {
            let txn = if let Ok(txn) = Arc::try_unwrap(txn) {
                txn.into_inner().unwrap()
            } else {
                unreachable!()
            };
            txn.commit()?;
        }
        Ok(())
    }
}
